package orderbook

import (
	"context"
	"testing"

	"github.com/yourorg/binance-orderbook/config"
	"github.com/yourorg/binance-orderbook/models"
	"go.uber.org/zap"
)

type fakeSnapshotFetcher struct {
	responses []*models.RESTDepthResponse
	calls     int
}

func (f *fakeSnapshotFetcher) FetchSnapshot(symbol string, limit int) (*models.RESTDepthResponse, error) {
	if f.calls >= len(f.responses) {
		return f.responses[len(f.responses)-1], nil
	}
	resp := f.responses[f.calls]
	f.calls++
	return resp, nil
}

func TestBufferDrain_DiscardsStaleEvents(t *testing.T) {
	buffer := []models.DepthEvent{
		{FirstUpdateID: 90, FinalUpdateID: 100},
		{FirstUpdateID: 101, FinalUpdateID: 102},
		{FirstUpdateID: 103, FinalUpdateID: 104},
	}

	got := discardStaleEvents(buffer, 100)
	if len(got) != 2 {
		t.Fatalf("expected 2 events after drain, got %d", len(got))
	}
	if got[0].FinalUpdateID != 102 || got[1].FinalUpdateID != 104 {
		t.Fatalf("unexpected events survived drain: %+v", got)
	}
}

func TestFirstEventValidation_RejectsGap(t *testing.T) {
	lastUpdateID := int64(200)
	event := models.DepthEvent{FirstUpdateID: 202, FinalUpdateID: 203}
	if validateFirstEvent(event, lastUpdateID) {
		t.Fatalf("expected first event validation to fail for gap")
	}
}

func TestSteadyStateGap_TriggersReinit(t *testing.T) {
	prevFinal := int64(300)
	event := models.DepthEvent{FirstUpdateID: 305, FinalUpdateID: 306}
	if !hasSteadyStateGap(event, prevFinal) {
		t.Fatalf("expected steady-state gap detection to trigger reinit")
	}
}

func TestSnapshotTooOld_RetriggersFetch(t *testing.T) {
	fetcher := &fakeSnapshotFetcher{
		responses: []*models.RESTDepthResponse{
			{LastUpdateID: 99},
			{LastUpdateID: 150},
		},
	}

	m := &Manager{
		cfg: &config.Config{
			Symbol:     "BTCUSDT",
			DepthLimit: 5000,
		},
		logger:          zap.NewNop(),
		snapshotFetcher: fetcher,
	}

	buffer := []models.DepthEvent{{FirstUpdateID: 100, FinalUpdateID: 100}}
	snapshot, err := m.fetchSnapshotUntilFresh(context.Background(), 100, &buffer, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fetcher.calls != 2 {
		t.Fatalf("expected 2 snapshot fetch attempts, got %d", fetcher.calls)
	}
	if snapshot.LastUpdateID != 150 {
		t.Fatalf("expected fresh snapshot with LastUpdateID 150, got %d", snapshot.LastUpdateID)
	}
}
