package orderbook

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/yourorg/binance-orderbook/models"
	"go.uber.org/zap"
)

func TestAtomicWrite(t *testing.T) {
	baseDir := t.TempDir()
	path := filepath.Join(baseDir, "data", "depth.json")

	p := NewPersister(path, 10, func() models.DepthSnapshot {
		return models.DepthSnapshot{
			Symbol:       "BTCUSDT",
			LastUpdateID: 123,
			Bids:         map[string]string{"100": "1"},
			Asks:         map[string]string{"101": "2"},
			UpdatedAt:    1716000000000,
		}
	}, zap.NewNop())

	if err := p.FlushNow(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected output file to exist: %v", err)
	}

	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("expected temp file to not linger, got err=%v", err)
	}
}

func TestDirectoryCreation(t *testing.T) {
	baseDir := t.TempDir()
	path := filepath.Join(baseDir, "nested", "dir", "depth.json")

	p := NewPersister(path, 10, func() models.DepthSnapshot {
		return models.DepthSnapshot{Symbol: "BTCUSDT", Bids: map[string]string{}, Asks: map[string]string{}}
	}, zap.NewNop())

	if err := p.FlushNow(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	if stat, err := os.Stat(filepath.Dir(path)); err != nil || !stat.IsDir() {
		t.Fatalf("expected directory to be created")
	}
}

func TestOutputSchema(t *testing.T) {
	baseDir := t.TempDir()
	path := filepath.Join(baseDir, "data", "depth.json")

	expected := models.DepthSnapshot{
		Symbol:       "BTCUSDT",
		LastUpdateID: 42,
		Bids:         map[string]string{"100.00": "1.23"},
		Asks:         map[string]string{"101.00": "0.45"},
		UpdatedAt:    1716000000000,
	}

	p := NewPersister(path, 10, func() models.DepthSnapshot { return expected }, zap.NewNop())
	if err := p.FlushNow(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	var parsed models.DepthSnapshot
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("json schema mismatch: %v", err)
	}

	if parsed.Symbol != expected.Symbol || parsed.LastUpdateID != expected.LastUpdateID || parsed.UpdatedAt != expected.UpdatedAt {
		t.Fatalf("unexpected scalar fields: got %+v", parsed)
	}
	if parsed.Bids["100.00"] != "1.23" {
		t.Fatalf("unexpected bids payload")
	}
	if parsed.Asks["101.00"] != "0.45" {
		t.Fatalf("unexpected asks payload")
	}
}
