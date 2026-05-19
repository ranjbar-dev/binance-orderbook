package orderbook

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/yourorg/binance-orderbook/models"
)

// SnapshotFetcher fetches REST order book snapshots.
type SnapshotFetcher struct {
	baseURL string
	client  *http.Client
}

// NewSnapshotFetcher builds a fetcher with a 10-second HTTP timeout.
func NewSnapshotFetcher(baseURL string) *SnapshotFetcher {
	return &SnapshotFetcher{
		baseURL: strings.TrimRight(baseURL, "/"),
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// FetchSnapshot retrieves GET /api/v3/depth for a symbol and limit.
func (s *SnapshotFetcher) FetchSnapshot(symbol string, limit int) (*models.RESTDepthResponse, error) {
	u, err := url.Parse(s.baseURL + "/api/v3/depth")
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("symbol", symbol)
	q.Set("limit", strconv.Itoa(limit))
	u.RawQuery = q.Encode()

	resp, err := s.client.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("snapshot status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var out models.RESTDepthResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}

	return &out, nil
}
