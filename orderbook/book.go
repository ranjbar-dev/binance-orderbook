package orderbook

import (
	"sync"
	"time"

	"github.com/yourorg/binance-orderbook/models"
)

// Book is the in-memory local order book.
type Book struct {
	mu           sync.RWMutex
	symbol       string
	lastUpdateID int64
	bids         map[string]string
	asks         map[string]string
}

// NewBook creates an empty order book for a symbol.
func NewBook(symbol string) *Book {
	return &Book{
		symbol: symbol,
		bids:   make(map[string]string),
		asks:   make(map[string]string),
	}
}

// ApplyEvent applies a single depth update event to bids and asks.
func (b *Book) ApplyEvent(event models.DepthEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	applySide(b.bids, event.Bids)
	applySide(b.asks, event.Asks)
	b.lastUpdateID = event.FinalUpdateID
}

// LoadSnapshot replaces the entire local state with a fresh snapshot.
func (b *Book) LoadSnapshot(snap models.RESTDepthResponse) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.bids = make(map[string]string, len(snap.Bids))
	b.asks = make(map[string]string, len(snap.Asks))

	for _, level := range snap.Bids {
		if len(level) < 2 {
			continue
		}
		b.bids[level[0]] = level[1]
	}

	for _, level := range snap.Asks {
		if len(level) < 2 {
			continue
		}
		b.asks[level[0]] = level[1]
	}

	b.lastUpdateID = snap.LastUpdateID
}

// Snapshot returns a point-in-time deep copy of the local book state.
func (b *Book) Snapshot() models.DepthSnapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()

	bids := make(map[string]string, len(b.bids))
	for k, v := range b.bids {
		bids[k] = v
	}

	asks := make(map[string]string, len(b.asks))
	for k, v := range b.asks {
		asks[k] = v
	}

	return models.DepthSnapshot{
		Symbol:       b.symbol,
		LastUpdateID: b.lastUpdateID,
		Bids:         bids,
		Asks:         asks,
		UpdatedAt:    time.Now().UnixMilli(),
	}
}

// LastUpdateID returns the local book's final update ID.
func (b *Book) LastUpdateID() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.lastUpdateID
}

func applySide(side map[string]string, levels [][]string) {
	for _, level := range levels {
		if len(level) < 2 {
			continue
		}
		price, qty := level[0], level[1]
		if qty == "0" {
			delete(side, price)
			continue
		}
		side[price] = qty
	}
}
