package models

// DepthSnapshot is the persisted file format.
type DepthSnapshot struct {
	Symbol       string            `json:"symbol"`
	LastUpdateID int64             `json:"lastUpdateId"`
	Bids         map[string]string `json:"bids"`
	Asks         map[string]string `json:"asks"`
	UpdatedAt    int64             `json:"updatedAt"`
}

// DepthEvent is a single WebSocket diff depth message.
type DepthEvent struct {
	EventType     string     `json:"e"`
	EventTime     int64      `json:"E"`
	Symbol        string     `json:"s"`
	FirstUpdateID int64      `json:"U"`
	FinalUpdateID int64      `json:"u"`
	PrevUpdateID  int64      `json:"pu"`
	Bids          [][]string `json:"b"`
	Asks          [][]string `json:"a"`
}

// RESTDepthResponse is the response from GET /api/v3/depth.
type RESTDepthResponse struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}
