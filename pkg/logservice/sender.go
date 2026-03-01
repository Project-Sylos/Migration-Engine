// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package logservice

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

const defaultBatchSize = 20_000

// LS is the global log service sender instance.
// It must be initialized via InitGlobalLogger before use.
var LS *Sender

// InitGlobalLogger initializes the global LS instance.
// Logs are persisted to the main DB's logs table when mainDB is non-nil (single DuckDB).
func InitGlobalLogger(mainDB *db.DB, addr, level string) error {
	sender, err := NewSender(mainDB, addr, level)
	if err != nil {
		return fmt.Errorf("failed to initialize global logger: %w", err)
	}
	LS = sender

	if err := LS.Log("info", "Test log", "test", "test"); err != nil {
		err = LS.Close()
		if err != nil {
			return fmt.Errorf("failed to close logger: %w", err)
		}
		return fmt.Errorf("failed to send test log: %w", err)
	}
	return nil
}

// Sender transmits logs over UDP and optionally writes them to the main DB's logs table.
type Sender struct {
	logDB     *db.DB        // main DB for log persistence (not owned; do not close)
	logBuffer *db.LogBuffer // buffered log writer (nil if logDB is nil)
	Addr      string        // e.g. "127.0.0.1:1997"
	Level      string        // threshold for UDP output
	conn       net.Conn
	minLevelIx int
	mu         sync.Mutex // guards buffer/encoder
	buf        *bytes.Buffer
	enc        *json.Encoder
	tmp        LogPacket // reusable scratch struct
}

// getLevelIndex assigns numeric priority to levels.
func getLevelIndex(level string) int {
	switch level {
	case "trace":
		return 0
	case "debug":
		return 1
	case "info":
		return 2
	case "warning":
		return 3
	case "error":
		return 4
	case "critical":
		return 5
	default:
		return -1
	}
}

// NewSender initializes a new dual-channel sender.
// If logDB is non-nil, logs are persisted to it (dedicated log file). If nil, only UDP is used.
func NewSender(logDB *db.DB, addr, level string) (*Sender, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	minIx := getLevelIndex(level)
	if minIx == -1 {
		return nil, fmt.Errorf("invalid threshold level: %s", level)
	}
	buf := new(bytes.Buffer)

	var logBuffer *db.LogBuffer
	if logDB != nil {
		logBuffer = db.NewLogBuffer(logDB, defaultBatchSize, 10*time.Second)
	}

	return &Sender{
		logDB:     logDB,
		logBuffer: logBuffer,
		Addr:      addr,
		Level:     level,
		conn:      conn,
		minLevelIx: minIx,
		buf:       buf,
		enc:       json.NewEncoder(buf),
	}, nil
}

// Log sends the message via UDP (if level >= threshold) and optionally to the log DB via the buffer.
// Safe for concurrent use.
func (s *Sender) Log(level, message, entity, entityID string, queues ...string) error {
	timestamp := time.Now()

	queue := ""
	if len(queues) > 0 {
		queue = queues[0]
	}

	// --- DB write (when log DB is set, buffered) ---
	if s.logBuffer != nil {
		id := db.GenerateLogID()
		s.logBuffer.Add(db.LogEntry{
			ID:        id,
			Timestamp: timestamp.Format(time.RFC3339Nano),
			Level:     level,
			Entity:    entity,
			EntityID:  entityID,
			Message:   message,
			Queue:     queue,
		})
	}

	// --- UDP send (conditional) ---
	levelIx := getLevelIndex(level)
	if levelIx == -1 {
		return fmt.Errorf("invalid level: %s", level)
	}
	if levelIx < s.minLevelIx {
		return nil // below UDP threshold
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Conn may be nil if Close() was already called (e.g. migration finished, workers still exiting).
	if s.conn == nil {
		return nil
	}

	s.tmp.Timestamp = timestamp
	s.tmp.Level = level
	s.tmp.Message = message
	s.tmp.Entity = entity
	s.tmp.EntityID = entityID
	s.tmp.Queue = queue

	s.buf.Reset()
	if err := s.enc.Encode(&s.tmp); err != nil {
		return err
	}

	_, err := s.conn.Write(s.buf.Bytes())
	return err
}

// ClearConsole sends a log with the message "<<CLEAR_SCREEN>>" to instruct the listener to clear its console.
// Does NOT write to the DB log table (no persistence); only sends to UDP listener(s).
func (s *Sender) ClearConsole() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn == nil {
		return nil
	}

	// Build special log packet (with only what the listener expects).
	s.tmp.Timestamp = time.Now()
	s.tmp.Level = "info"
	s.tmp.Message = "<<CLEAR_SCREEN>>"
	s.tmp.Entity = ""
	s.tmp.EntityID = ""
	s.tmp.Queue = ""

	s.buf.Reset()
	if err := s.enc.Encode(&s.tmp); err != nil {
		return err
	}

	_, err := s.conn.Write(s.buf.Bytes())
	return err
}

// Close terminates the UDP connection and stops the log buffer. Does not close the main DB.
// Sets conn to nil under mu so any Log() after Close() no-ops instead of writing to a closed connection.
func (s *Sender) Close() error {
	if s.logBuffer != nil {
		s.logBuffer.Stop()
		s.logBuffer = nil
	}
	s.mu.Lock()
	conn := s.conn
	s.conn = nil
	s.mu.Unlock()
	if conn != nil {
		if err := conn.Close(); err != nil {
			return fmt.Errorf("failed to close UDP connection: %w", err)
		}
	}
	s.logDB = nil
	return nil
}
