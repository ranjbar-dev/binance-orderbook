# PRD: Binance Local Order Book Keeper

## Overview

A production-grade Go application that maintains a perfectly synchronized local snapshot of a Binance Spot order book. It follows the official Binance algorithm for managing a local order book via the Diff Depth WebSocket stream and REST snapshot bootstrapping. The local state is persisted to `./data/depth.json` and kept continuously up-to-date.

---

## Goals

- Maintain a 100% correct, continuously updated local order book that mirrors Binance's server state.
- Persist the current order book snapshot to `./data/depth.json` after every processed update.
- Survive network drops, server-side disconnections, and stream gaps by automatically reinitializing.
- Be observable via structured logging.
- Be configurable via environment variables or a config file — no hardcoded symbols.

---

## Non-Goals

- No HTTP server or API exposure.
- No database persistence (file only).
- No order placement or authenticated endpoints.
- No multi-symbol support in v1 (single symbol only).

---

## Technology Stack

| Concern | Choice |
|---|---|
| Language | Go 1.22+ |
| WebSocket client | `github.com/gorilla/websocket` |
| HTTP client | Standard `net/http` |
| JSON | Standard `encoding/json` |
| Configuration | `github.com/spf13/viper` (env + optional config file) |
| Logging | `go.uber.org/zap` (structured, leveled) |
| File I/O | Standard `os` + `sync/atomic` for safe writes |

---

## Configuration

All settings configurable via environment variables (or `.env` / `config.yaml`):

| Variable | Default | Description |
|---|---|---|
| `SYMBOL` | `BTCUSDT` | Trading pair symbol (uppercase) |
| `DEPTH_LIMIT` | `5000` | Snapshot depth level (max 5000) |
| `DATA_PATH` | `./data/depth.json` | Output file path |
| `WS_BASE_URL` | `wss://stream.binance.com:9443` | WebSocket base URL |
| `REST_BASE_URL` | `https://api.binance.com` | REST API base URL |
| `FLUSH_INTERVAL_MS` | `500` | Min milliseconds between file writes (debounce) |
| `LOG_LEVEL` | `info` | Logging level (`debug`, `info`, `warn`, `error`) |
| `RECONNECT_DELAY_MS` | `2000` | Delay before reconnect attempt after failure |

---

## Core Data Structures

```go
// DepthSnapshot is the persisted file format
type DepthSnapshot struct {
    Symbol       string            `json:"symbol"`
    LastUpdateID int64             `json:"lastUpdateId"`
    Bids         map[string]string `json:"bids"` // price -> quantity
    Asks         map[string]string `json:"asks"` // price -> quantity
    UpdatedAt    int64             `json:"updatedAt"` // Unix milliseconds
}

// DepthEvent is a single WebSocket diff depth message
type DepthEvent struct {
    EventType    string     `json:"e"`
    EventTime    int64      `json:"E"`
    Symbol       string     `json:"s"`
    FirstUpdateID int64     `json:"U"` // First update ID in event
    FinalUpdateID int64     `json:"u"` // Final update ID in event
    PrevUpdateID  int64     `json:"pu"` // (Not present on spot, only futures; reserved)
    Bids          [][]string `json:"b"` // [price, quantity] pairs
    Asks          [][]string `json:"a"` // [price, quantity] pairs
}

// RESTDepthResponse is the response from GET /api/v3/depth
type RESTDepthResponse struct {
    LastUpdateID int64      `json:"lastUpdateId"`
    Bids         [][]string `json:"bids"`
    Asks         [][]string `json:"asks"`
}
```

---

## Binance Order Book Synchronization Algorithm

This is the **exact official algorithm** that must be implemented. No deviation is permitted.

### Step-by-Step

1. **Open WebSocket** connection to `wss://stream.binance.com:9443/ws/<symbol_lowercase>@depth@100ms`.
2. **Buffer all incoming `depthUpdate` events** from the stream in memory. Do not apply them yet.
3. **Record `U`** (FirstUpdateID) of the very first event received in this session.
4. **Fetch REST snapshot**: `GET https://api.binance.com/api/v3/depth?symbol=<SYMBOL>&limit=5000`.
5. **Validate snapshot freshness**: If `snapshot.lastUpdateId < U` (the first buffered event's `U`), the snapshot is too old. Discard the snapshot, **keep buffering**, and go back to step 4.
6. **Drain the buffer**: Discard every buffered event where `event.u <= snapshot.lastUpdateId`.
7. **Validate the first surviving event**: The first event that passes the filter MUST satisfy `event.U <= snapshot.lastUpdateId + 1` AND `event.u >= snapshot.lastUpdateId + 1`. If it does not, the stream has a gap — reinitialize from step 1.
8. **Apply buffered events** (the ones that passed the filter) to the snapshot in order.
9. **Enter steady-state loop**: For each new incoming event:
   - If `event.u <= currentLastUpdateID`, discard (already applied).
   - Apply the event's bids and asks to the local book.
   - Update `currentLastUpdateID = event.u`.
10. **Flush to file** after applying each event (with debounce).

### Applying an Event (Bid/Ask Update Logic)

For each `[price, quantity]` pair in a `depthUpdate` event:

- If `quantity == "0"`, **delete** the price level from the local map.
- Otherwise, **set/overwrite** the price level: `book[price] = quantity`.

This applies identically to both bids and asks.

### Gap Detection During Steady State

After initialization, each incoming event's `U` (FirstUpdateID) must equal `currentLastUpdateID + 1`. If this invariant is violated (a gap), the entire process must **reinitialize** from step 1. Log a warning with the expected and actual IDs.

---

## Persistence: `./data/depth.json`

### File Format

```json
{
  "symbol": "BTCUSDT",
  "lastUpdateId": 48291083721,
  "bids": {
    "67500.00": "0.12300000",
    "67499.50": "0.45000000"
  },
  "asks": {
    "67500.50": "0.08100000",
    "67501.00": "1.20000000"
  },
  "updatedAt": 1716000000000
}
```

### Write Strategy

- Use **atomic file writes**: write to a temp file (`depth.json.tmp`), then `os.Rename` to `depth.json`. This prevents partial reads from consuming processes.
- **Debounce** writes: don't write more than once per `FLUSH_INTERVAL_MS` (default 500ms). Track a `dirty` flag; flush on a ticker.
- Create `./data/` directory if it does not exist (`os.MkdirAll`).

---

## WebSocket Connection Management

### Ping/Pong

- Binance sends a **WebSocket ping frame every 20 seconds**.
- The application **must reply with a pong frame** containing the same payload immediately.
- Set `conn.SetPingHandler` to respond with pong automatically.
- If no pong is sent within 60 seconds, Binance will disconnect.

### 24-Hour Connection Limit

- Binance WebSocket connections are valid for **24 hours maximum**.
- A `serverShutdown` event will be sent **10 minutes before** the forced disconnect.
- On receiving `serverShutdown`, **immediately begin reconnection** (do not wait for the disconnect).

### Reconnection Logic

- On any connection error, read error, or forced close: wait `RECONNECT_DELAY_MS`, then **reinitialize the full algorithm** from step 1.
- Use exponential backoff with a cap of 30 seconds for repeated failures.
- Log each reconnection attempt and reason.

---

## Application Structure

```
binance-orderbook/
├── main.go                  # Entry point, signal handling, graceful shutdown
├── config/
│   └── config.go            # Viper-based config loading
├── orderbook/
│   ├── book.go              # Local order book state (map-based, mutex-protected)
│   ├── manager.go           # Main sync loop (the algorithm implementation)
│   ├── snapshot.go          # REST snapshot fetching
│   └── persister.go         # Atomic file write with debounce
├── ws/
│   └── client.go            # WebSocket connection, reconnect, ping/pong
├── models/
│   └── depth.go             # DepthEvent, RESTDepthResponse, DepthSnapshot structs
├── go.mod
├── go.sum
└── data/                    # Created at runtime
    └── depth.json           # Output file (gitignored)
```

---

## Component Responsibilities

### `main.go`
- Load config via `config.Load()`.
- Initialize logger.
- Create `./data/` directory.
- Instantiate `orderbook.Manager` and call `manager.Run(ctx)`.
- Handle `SIGINT`/`SIGTERM` for graceful shutdown (flush final state, close WebSocket cleanly).

### `ws/client.go`
- Establish WebSocket connection with `gorilla/websocket`.
- Set `PingHandler` to respond with pong.
- Set read deadline to 90 seconds (refreshed on each message).
- Expose `ReadMessage() ([]byte, error)` and `Close()`.
- Does NOT contain business logic — only transport.

### `orderbook/manager.go`
- Owns the main state machine (steps 1–10 of the algorithm).
- Manages the event buffer (slice of `DepthEvent`).
- Orchestrates `snapshot.go`, `ws/client.go`, and `book.go`.
- On any error, resets state and retries full initialization.

### `orderbook/book.go`
- Holds `bids map[string]string` and `asks map[string]string` protected by `sync.RWMutex`.
- Methods: `ApplyEvent(event DepthEvent)`, `LoadSnapshot(snap RESTDepthResponse)`, `Snapshot() DepthSnapshot`.
- `ApplyEvent` applies the zero-quantity delete logic.

### `orderbook/snapshot.go`
- `FetchSnapshot(symbol string, limit int) (*RESTDepthResponse, error)`
- Uses `net/http` with a 10-second timeout.
- Returns error on non-200 or parse failure.

### `orderbook/persister.go`
- `Persister` struct with a dirty flag and flush ticker.
- `MarkDirty()` — called after every applied event.
- Background goroutine flushes to file when dirty and interval has elapsed.
- Uses atomic rename strategy.

---

## Error Handling & Logging

### Structured Log Events (zap fields)

| Event | Level | Key Fields |
|---|---|---|
| Application start | INFO | symbol, dataPath |
| WebSocket connected | INFO | url |
| Snapshot fetched | INFO | lastUpdateId, bidLevels, askLevels |
| Initialization complete | INFO | lastUpdateId, bufferedEventsApplied |
| Event applied | DEBUG | eventU, newLastUpdateId |
| File flushed | DEBUG | path, lastUpdateId |
| Gap detected | WARN | expected, got |
| Snapshot too old — retrying | WARN | snapshotLastUpdateId, requiredMin |
| Reconnecting | WARN | reason, attempt, delayMs |
| serverShutdown received | WARN | eventTime |
| Fatal error | ERROR | err |

---

## Graceful Shutdown

On `SIGINT` or `SIGTERM`:
1. Cancel the root context.
2. Close the WebSocket connection.
3. Perform one final file flush regardless of debounce timer.
4. Log "shutdown complete" and exit 0.

---

## Testing Requirements

### Unit Tests

- `orderbook/book_test.go`:
  - Apply event with new price: added to map.
  - Apply event with quantity `"0"`: price removed from map.
  - Apply event to existing price: quantity updated.
  - `LoadSnapshot` clears previous state and loads correctly.

- `orderbook/manager_test.go`:
  - Buffer drain: events with `u <= lastUpdateId` are discarded.
  - First event validation: rejects events violating `U <= lastUpdateId+1 AND u >= lastUpdateId+1`.
  - Gap detection: triggers reinit when `event.U != prevU + 1`.
  - Snapshot too old: retries when `snapshot.lastUpdateId < firstBufferedU`.

- `orderbook/persister_test.go`:
  - File is written atomically (via rename).
  - `data/` directory is created if absent.
  - Output JSON matches expected schema.

### Integration Test (optional, tagged `//go:build integration`)

- Full end-to-end against Binance testnet (`wss://testnet.binance.vision`).
- Confirm file is created and updated within 5 seconds.
- Confirm `lastUpdateId` increases monotonically over 30 seconds.

---

## Acceptance Criteria

- [ ] Application starts, bootstraps, and writes `./data/depth.json` within 10 seconds.
- [ ] `depth.json` is updated continuously as long as the process runs.
- [ ] `lastUpdateId` in the file is strictly increasing over time.
- [ ] Killing and restarting the process results in a fresh, valid snapshot within 10 seconds.
- [ ] Simulating a network drop results in automatic reconnection and re-sync without operator intervention.
- [ ] `depth.json` is never written in a partial/corrupt state (atomic rename verified).
- [ ] All unit tests pass: `go test ./...`.
- [ ] No data races: `go test -race ./...`.
- [ ] `go vet ./...` reports zero issues.

---

## Sequence Diagram

```
main.go          Manager          ws.Client       REST API         Persister
  |                 |                 |               |                 |
  |--Run(ctx)------>|                 |               |                 |
  |                 |--Connect()----->|               |                 |
  |                 |<--connected-----|               |                 |
  |                 |--start buffering events-------->|                 |
  |                 |--FetchSnapshot()--------------->|                 |
  |                 |<--RESTDepthResponse-------------|                 |
  |                 |--validate snapshot freshness    |                 |
  |                 |--drain buffer (discard stale)   |                 |
  |                 |--validate first event           |                 |
  |                 |--LoadSnapshot(book)             |                 |
  |                 |--apply buffered events          |                 |
  |                 |--MarkDirty()-------------------------------------->|
  |                 |                                 |              (flush)
  |                 |====STEADY STATE LOOP============|                 |
  |                 |<--DepthEvent----|               |                 |
  |                 |--ApplyEvent(book)               |                 |
  |                 |--MarkDirty()-------------------------------------->|
  |                 |  [gap detected] -- reinitialize |                 |
  |                 |  [serverShutdown] -- reconnect  |                 |
```

---

## Appendix: Key Binance API Reference

| Resource | Value |
|---|---|
| WebSocket endpoint | `wss://stream.binance.com:9443/ws/<symbol>@depth@100ms` |
| REST snapshot endpoint | `GET https://api.binance.com/api/v3/depth?symbol=<SYMBOL>&limit=5000` |
| Max depth levels | 5000 per side |
| WS ping interval | Every 20 seconds |
| WS pong deadline | Within 60 seconds |
| Max connection lifetime | 24 hours |
| `serverShutdown` notice | 10 minutes before forced disconnect |
| Rate limit weight for `/api/v3/depth` with limit=5000 | 250 weight units |
