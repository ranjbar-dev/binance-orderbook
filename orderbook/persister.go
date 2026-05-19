package orderbook

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yourorg/binance-orderbook/models"
	"go.uber.org/zap"
)

// Persister flushes snapshots to disk using atomic writes and debounce.
type Persister struct {
	path       string
	interval   time.Duration
	snapshotFn func() models.DepthSnapshot
	logger     *zap.Logger

	dirty atomic.Bool
	once  sync.Once
}

// NewPersister creates a new snapshot persister.
func NewPersister(path string, interval time.Duration, snapshotFn func() models.DepthSnapshot, logger *zap.Logger) *Persister {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	return &Persister{
		path:       path,
		interval:   interval,
		snapshotFn: snapshotFn,
		logger:     logger,
	}
}

// Start begins the flush loop and exits when the context is canceled.
func (p *Persister) Start(ctx context.Context) {
	p.once.Do(func() {
		go p.flushLoop(ctx)
	})
}

// MarkDirty signals there is data waiting to be flushed.
func (p *Persister) MarkDirty() {
	p.dirty.Store(true)
}

// FlushNow writes the current snapshot immediately.
func (p *Persister) FlushNow() error {
	snap := p.snapshotFn()
	if err := writeSnapshotAtomic(p.path, snap); err != nil {
		return err
	}
	p.dirty.Store(false)
	if p.logger != nil {
		p.logger.Debug("File flushed", zap.String("path", p.path), zap.Int64("lastUpdateId", snap.LastUpdateID))
	}
	return nil
}

func (p *Persister) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if err := p.FlushNow(); err != nil && p.logger != nil {
				p.logger.Error("Fatal error", zap.NamedError("err", err))
			}
			return
		case <-ticker.C:
			if !p.dirty.Swap(false) {
				continue
			}

			snap := p.snapshotFn()
			if err := writeSnapshotAtomic(p.path, snap); err != nil {
				p.dirty.Store(true)
				if p.logger != nil {
					p.logger.Error("Fatal error", zap.NamedError("err", err))
				}
				continue
			}

			if p.logger != nil {
				p.logger.Debug("File flushed", zap.String("path", p.path), zap.Int64("lastUpdateId", snap.LastUpdateID))
			}
		}
	}
}

func writeSnapshotAtomic(path string, snap models.DepthSnapshot) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	payload, err := json.Marshal(snap)
	if err != nil {
		return err
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, payload, 0o644); err != nil {
		return err
	}

	return os.Rename(tmpPath, path)
}
