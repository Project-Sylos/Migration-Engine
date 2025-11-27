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

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
)

// LS is the global log service sender instance.
// It must be initialized via InitGlobalLogger before use.
var LS *Sender

// InitGlobalLogger initializes the global LS instance.
// This should be called once during application startup.
func InitGlobalLogger(dbInstance *db.DB, addr, level string) error {
	sender, err := NewSender(dbInstance, addr, level)
	if err != nil {
		return fmt.Errorf("failed to initialize global logger: %w", err)
	}
	LS = sender

	// send a test log on the global logger
	if err := LS.Log("info", "Test log", "test", "test"); err != nil {
		return fmt.Errorf("failed to send test log: %w", err)
	}

	if err := LS.ClearConsole(); err != nil {
		return fmt.Errorf("failed to send clear console log: %w", err)
	}

	return nil
}

// Sender transmits logs over UDP and writes them to the database.
type Sender struct {
	DB         *db.DB        // BoltDB handle for persistence
	logBuffer  *db.LogBuffer // buffered log writer
	Addr       string        // e.g. "127.0.0.1:1997"
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
// dbInstance should be a BoltDB instance.
func NewSender(dbInstance *db.DB, addr, level string) (*Sender, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	minIx := getLevelIndex(level)
	if minIx == -1 {
		return nil, fmt.Errorf("invalid threshold level: %s", level)
	}
	buf := new(bytes.Buffer)

	// Create a log buffer that flushes every 500 entries or every 2 seconds
	logBuffer := db.NewLogBuffer(dbInstance, 500, 2*time.Second)

	return &Sender{
		DB:         dbInstance,
		logBuffer:  logBuffer,
		Addr:       addr,
		Level:      level,
		conn:       conn,
		minLevelIx: minIx,
		buf:        buf,
		enc:        json.NewEncoder(buf),
	}, nil
}

// Log sends the message via UDP (if level >= threshold)
// and writes it unconditionally to the logs table in the DB via the buffer.
// Safe for concurrent use.
func (s *Sender) Log(level, message, entity, entityID string, queues ...string) error {
	timestamp := time.Now()

	queue := ""
	if len(queues) > 0 {
		queue = queues[0]
	}

	// --- DB write (always, buffered) ---
	id := db.GenerateLogID() // Generate UUID for log entry
	s.logBuffer.Add(db.LogEntry{
		ID:        id,
		Timestamp: timestamp.Format(time.RFC3339Nano), // ISO8601 format
		Level:     level,
		Entity:    entity,
		EntityID:  entityID,
		Message:   message,
		Queue:     queue,
	})

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

// Close terminates the UDP connection and stops the log buffer.
func (s *Sender) Close() error {
	if s.logBuffer != nil {
		s.logBuffer.Stop()
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}
	return nil
}
