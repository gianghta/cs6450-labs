package kvs

import (
	"encoding/json"
	"log"
	"os"
	"sync"
)

type HistoryEntry struct {
	ClientID    int    `json:"client_id"`
	OpType      string `json:"op_type"` // "put" or "get"
	Key         string `json:"key"`
	InputValue  any    `json:"input_value,omitempty"`  // Value for Put operations
	OutputValue any    `json:"output_value,omitempty"` // Value for Get operations
	StartNanos  int64  `json:"start_nanos"`
	EndNanos    int64  `json:"end_nanos"`
}

type HistoryLogger struct {
	file    *os.File
	mu      sync.Mutex
	encoder *json.Encoder
}

func NewHistoryLogger(filename string) (*HistoryLogger, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	return &HistoryLogger{
		file:    f,
		encoder: json.NewEncoder(f),
	}, nil
}

func (h *HistoryLogger) Log(entry HistoryEntry) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.encoder.Encode(entry); err != nil {
		log.Printf("failed to write history entry: %v", err)
	}
}

func (h *HistoryLogger) Close() {
	h.file.Close()
}
