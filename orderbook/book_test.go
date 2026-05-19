package orderbook

import (
	"testing"

	"github.com/yourorg/binance-orderbook/models"
)

func TestApplyEvent_NewPrice(t *testing.T) {
	b := NewBook("BTCUSDT")
	b.ApplyEvent(models.DepthEvent{
		FinalUpdateID: 1,
		Bids:          [][]string{{"100.00", "1.25"}},
	})

	snap := b.Snapshot()
	if got := snap.Bids["100.00"]; got != "1.25" {
		t.Fatalf("expected bid quantity 1.25, got %q", got)
	}
}

func TestApplyEvent_ZeroQuantity(t *testing.T) {
	b := NewBook("BTCUSDT")
	b.ApplyEvent(models.DepthEvent{
		FinalUpdateID: 1,
		Bids:          [][]string{{"100.00", "1.25"}},
	})
	b.ApplyEvent(models.DepthEvent{
		FinalUpdateID: 2,
		Bids:          [][]string{{"100.00", "0"}},
	})

	snap := b.Snapshot()
	if _, ok := snap.Bids["100.00"]; ok {
		t.Fatalf("expected bid to be deleted when qty is zero")
	}
}

func TestApplyEvent_UpdatePrice(t *testing.T) {
	b := NewBook("BTCUSDT")
	b.ApplyEvent(models.DepthEvent{
		FinalUpdateID: 1,
		Asks:          [][]string{{"101.00", "0.5"}},
	})
	b.ApplyEvent(models.DepthEvent{
		FinalUpdateID: 2,
		Asks:          [][]string{{"101.00", "0.8"}},
	})

	snap := b.Snapshot()
	if got := snap.Asks["101.00"]; got != "0.8" {
		t.Fatalf("expected ask quantity 0.8, got %q", got)
	}
}

func TestLoadSnapshot_ClearsExistingState(t *testing.T) {
	b := NewBook("BTCUSDT")
	b.ApplyEvent(models.DepthEvent{
		FinalUpdateID: 1,
		Bids:          [][]string{{"100.00", "1.25"}},
		Asks:          [][]string{{"101.00", "0.5"}},
	})

	b.LoadSnapshot(models.RESTDepthResponse{
		LastUpdateID: 9,
		Bids:         [][]string{{"99.00", "2.0"}},
		Asks:         [][]string{{"102.00", "3.0"}},
	})

	snap := b.Snapshot()
	if _, ok := snap.Bids["100.00"]; ok {
		t.Fatalf("old bid level should be cleared")
	}
	if _, ok := snap.Asks["101.00"]; ok {
		t.Fatalf("old ask level should be cleared")
	}
	if snap.Bids["99.00"] != "2.0" {
		t.Fatalf("snapshot bid not loaded")
	}
	if snap.Asks["102.00"] != "3.0" {
		t.Fatalf("snapshot ask not loaded")
	}
}
