package orderbook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yourorg/binance-orderbook/config"
	"github.com/yourorg/binance-orderbook/models"
	"github.com/yourorg/binance-orderbook/ws"
	"go.uber.org/zap"
)

var errReinitialize = errors.New("reinitialize required")
var errServerShutdown = errors.New("server shutdown received")

type wsClient interface {
	ReadMessage() ([]byte, error)
	Close() error
}

type wsFactory func(url string) (wsClient, error)

type snapshotFetcher interface {
	FetchSnapshot(symbol string, limit int) (*models.RESTDepthResponse, error)
}

type managerEvent struct {
	depth          *models.DepthEvent
	serverShutdown bool
	eventTime      int64
}

type envelopeEvent struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
}

// Manager owns the local order book synchronization lifecycle.
type Manager struct {
	cfg             *config.Config
	logger          *zap.Logger
	book            *Book
	snapshotFetcher snapshotFetcher
	persister       *Persister
	wsFactory       wsFactory
}

// NewManager constructs a manager with real dependencies.
func NewManager(cfg *config.Config, logger *zap.Logger, book *Book, fetcher snapshotFetcher, persister *Persister) *Manager {
	return &Manager{
		cfg:             cfg,
		logger:          logger,
		book:            book,
		snapshotFetcher: fetcher,
		persister:       persister,
		wsFactory: func(url string) (wsClient, error) {
			return ws.NewClient(url)
		},
	}
}

// Run keeps the synchronization loop alive until the context is canceled.
func (m *Manager) Run(ctx context.Context) error {
	m.persister.Start(ctx)

	baseDelay := time.Duration(m.cfg.ReconnectDelayMS) * time.Millisecond
	if baseDelay <= 0 {
		baseDelay = 2 * time.Second
	}

	attempt := 0
	for {
		initialized, err := m.runSession(ctx)
		if err == nil || errors.Is(err, context.Canceled) {
			return nil
		}

		if initialized {
			attempt = 0
		}

		if errors.Is(err, errServerShutdown) {
			m.logger.Warn("Reconnecting", zap.String("reason", err.Error()), zap.Int("attempt", 1), zap.Int64("delayMs", 0))
			continue
		}

		attempt++
		delay := exponentialDelay(baseDelay, attempt)
		m.logger.Warn("Reconnecting", zap.String("reason", err.Error()), zap.Int("attempt", attempt), zap.Int64("delayMs", delay.Milliseconds()))

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func (m *Manager) runSession(ctx context.Context) (bool, error) {
	initialized := false

	// Step 1: open WebSocket connection.
	wsURL := fmt.Sprintf("%s/ws/%s@depth@100ms", strings.TrimRight(m.cfg.WSBaseURL, "/"), strings.ToLower(m.cfg.Symbol))
	client, err := m.wsFactory(wsURL)
	if err != nil {
		return initialized, err
	}
	defer client.Close()
	m.logger.Info("WebSocket connected", zap.String("url", wsURL))

	eventCh := make(chan managerEvent, 2048)
	errCh := make(chan error, 1)
	go m.readLoop(ctx, client, eventCh, errCh)

	// Step 2: buffer all incoming depthUpdate events before applying.
	buffer := make([]models.DepthEvent, 0, 2048)
	var firstBufferedU int64

	// Step 3: record U of the first buffered event.
	for firstBufferedU == 0 {
		event, err := m.waitForDepthEvent(ctx, eventCh, errCh, &buffer)
		if err != nil {
			return initialized, err
		}
		firstBufferedU = event.FirstUpdateID
	}

	// Step 4 and Step 5: fetch REST snapshot and retry if too old while keeping buffer.
	snapshot, err := m.fetchSnapshotUntilFresh(ctx, firstBufferedU, &buffer, eventCh, errCh)
	if err != nil {
		return initialized, err
	}

	// Step 6: drain buffer by discarding stale events where event.u <= snapshot.lastUpdateId.
	buffer = discardStaleEvents(buffer, snapshot.LastUpdateID)

	for len(buffer) == 0 {
		event, err := m.waitForDepthEvent(ctx, eventCh, errCh, nil)
		if err != nil {
			return initialized, err
		}
		buffer = append(buffer, event)
		buffer = discardStaleEvents(buffer, snapshot.LastUpdateID)
	}

	// Step 7: validate first surviving event range.
	if !validateFirstEvent(buffer[0], snapshot.LastUpdateID) {
		return initialized, errReinitialize
	}

	// Step 8: load snapshot and apply buffered events in order.
	m.book.LoadSnapshot(*snapshot)
	currentLastUpdateID := m.book.LastUpdateID()
	bufferedEventsApplied := 0

	for _, event := range buffer {
		if event.FinalUpdateID <= currentLastUpdateID {
			continue
		}
		m.book.ApplyEvent(event)
		currentLastUpdateID = event.FinalUpdateID
		m.persister.MarkDirty()
		m.logger.Debug("Event applied", zap.Int64("eventU", event.FirstUpdateID), zap.Int64("newLastUpdateId", currentLastUpdateID))
		bufferedEventsApplied++
	}

	initialized = true
	m.logger.Info("Initialization complete", zap.Int64("lastUpdateId", currentLastUpdateID), zap.Int("bufferedEventsApplied", bufferedEventsApplied))

	// Step 9: steady-state processing with gap detection.
	for {
		event, err := m.waitForDepthEvent(ctx, eventCh, errCh, nil)
		if err != nil {
			return initialized, err
		}

		if event.FinalUpdateID <= currentLastUpdateID {
			continue
		}

		if hasSteadyStateGap(event, currentLastUpdateID) {
			expected := currentLastUpdateID + 1
			m.logger.Warn("Gap detected", zap.Int64("expected", expected), zap.Int64("got", event.FirstUpdateID))
			return initialized, errReinitialize
		}

		m.book.ApplyEvent(event)
		currentLastUpdateID = event.FinalUpdateID

		// Step 10: flush to file after each applied event (debounced by persister loop).
		m.persister.MarkDirty()
		m.logger.Debug("Event applied", zap.Int64("eventU", event.FirstUpdateID), zap.Int64("newLastUpdateId", currentLastUpdateID))
	}
}

func (m *Manager) readLoop(ctx context.Context, client wsClient, eventCh chan<- managerEvent, errCh chan<- error) {
	defer close(eventCh)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		payload, err := client.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			select {
			case errCh <- err:
			default:
			}
			return
		}

		var env envelopeEvent
		if err := json.Unmarshal(payload, &env); err != nil {
			continue
		}

		switch env.EventType {
		case "depthUpdate":
			var event models.DepthEvent
			if err := json.Unmarshal(payload, &event); err != nil {
				continue
			}
			event.EventType = env.EventType
			select {
			case eventCh <- managerEvent{depth: &event}:
			case <-ctx.Done():
				return
			}
		case "serverShutdown":
			select {
			case eventCh <- managerEvent{serverShutdown: true, eventTime: env.EventTime}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (m *Manager) fetchSnapshotUntilFresh(ctx context.Context, firstBufferedU int64, buffer *[]models.DepthEvent, eventCh <-chan managerEvent, errCh <-chan error) (*models.RESTDepthResponse, error) {
	for {
		if err := m.drainPendingEvents(buffer, eventCh, errCh); err != nil {
			return nil, err
		}

		snapshot, err := m.snapshotFetcher.FetchSnapshot(m.cfg.Symbol, m.cfg.DepthLimit)
		if err != nil {
			return nil, err
		}

		m.logger.Info("Snapshot fetched", zap.Int64("lastUpdateId", snapshot.LastUpdateID), zap.Int("bidLevels", len(snapshot.Bids)), zap.Int("askLevels", len(snapshot.Asks)))

		if err := m.drainPendingEvents(buffer, eventCh, errCh); err != nil {
			return nil, err
		}

		if snapshotTooOld(snapshot.LastUpdateID, firstBufferedU) {
			m.logger.Warn("Snapshot too old — retrying", zap.Int64("snapshotLastUpdateId", snapshot.LastUpdateID), zap.Int64("requiredMin", firstBufferedU))
			continue
		}

		return snapshot, nil
	}
}

func (m *Manager) waitForDepthEvent(ctx context.Context, eventCh <-chan managerEvent, errCh <-chan error, buffer *[]models.DepthEvent) (models.DepthEvent, error) {
	for {
		select {
		case <-ctx.Done():
			return models.DepthEvent{}, ctx.Err()
		case err := <-errCh:
			if err != nil {
				return models.DepthEvent{}, err
			}
		case msg, ok := <-eventCh:
			if !ok {
				return models.DepthEvent{}, errors.New("event stream closed")
			}

			if msg.serverShutdown {
				m.logger.Warn("serverShutdown received", zap.Int64("eventTime", msg.eventTime))
				return models.DepthEvent{}, errServerShutdown
			}

			if msg.depth == nil {
				continue
			}

			if buffer != nil {
				*buffer = append(*buffer, *msg.depth)
			}

			return *msg.depth, nil
		}
	}
}

func (m *Manager) drainPendingEvents(buffer *[]models.DepthEvent, eventCh <-chan managerEvent, errCh <-chan error) error {
	for {
		select {
		case err := <-errCh:
			if err != nil {
				return err
			}
		case msg, ok := <-eventCh:
			if !ok {
				return errors.New("event stream closed")
			}
			if msg.serverShutdown {
				m.logger.Warn("serverShutdown received", zap.Int64("eventTime", msg.eventTime))
				return errServerShutdown
			}
			if msg.depth != nil && buffer != nil {
				*buffer = append(*buffer, *msg.depth)
			}
		default:
			return nil
		}
	}
}

func exponentialDelay(base time.Duration, attempt int) time.Duration {
	delay := base
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= 30*time.Second {
			return 30 * time.Second
		}
	}
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

func discardStaleEvents(buffer []models.DepthEvent, lastUpdateID int64) []models.DepthEvent {
	filtered := make([]models.DepthEvent, 0, len(buffer))
	for _, event := range buffer {
		if event.FinalUpdateID <= lastUpdateID {
			continue
		}
		filtered = append(filtered, event)
	}
	return filtered
}

func validateFirstEvent(event models.DepthEvent, lastUpdateID int64) bool {
	required := lastUpdateID + 1
	return event.FirstUpdateID <= required && event.FinalUpdateID >= required
}

func hasSteadyStateGap(event models.DepthEvent, prevFinalUpdateID int64) bool {
	return event.FirstUpdateID != prevFinalUpdateID+1
}

func snapshotTooOld(snapshotLastUpdateID, firstBufferedU int64) bool {
	return snapshotLastUpdateID < firstBufferedU
}
