package ws

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Client wraps a Binance depth WebSocket connection.
type Client struct {
	conn      *websocket.Conn
	writeMu   sync.Mutex
	closeOnce sync.Once
}

// NewClient dials a WebSocket endpoint and configures ping/pong handlers.
func NewClient(url string) (*Client, error) {
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}

	client := &Client{conn: conn}
	_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))

	conn.SetPingHandler(func(appData string) error {
		client.writeMu.Lock()
		defer client.writeMu.Unlock()
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
	})

	return client, nil
}

// ReadMessage reads the next message and refreshes the read deadline.
func (c *Client) ReadMessage() ([]byte, error) {
	_, payload, err := c.conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	return payload, nil
}

// Close closes the underlying WebSocket connection.
func (c *Client) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		closeErr = c.conn.Close()
	})
	return closeErr
}
