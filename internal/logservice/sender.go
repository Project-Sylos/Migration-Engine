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

	"github.com/Project-Sylos/Migration-Engine/internal/db"
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
	return nil
}

// Sender transmits logs over UDP and writes them to the database.
type Sender struct {
	DB         *db.DB // database handle for persistence
	Addr       string // e.g. "127.0.0.1:1997"
	Level      string // threshold for UDP output
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
	return &Sender{
		DB:         dbInstance,
		Addr:       addr,
		Level:      level,
		conn:       conn,
		minLevelIx: minIx,
		buf:        buf,
		enc:        json.NewEncoder(buf),
	}, nil
}

// Log sends the message via UDP (if level >= threshold)
// and writes it unconditionally to the logs table in the DB.
// Safe for concurrent use.
func (s *Sender) Log(level, message, entity, entityID string) error {
	timestamp := time.Now()

	// --- DB write (always) ---
	id := fmt.Sprintf("%d", timestamp.UnixNano()) // basic unique ID for now
	_ = s.DB.Write("logs", id, timestamp, level, entity, entityID, nil, message, nil)

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

	s.buf.Reset()
	if err := s.enc.Encode(&s.tmp); err != nil {
		return err
	}

	_, err := s.conn.Write(s.buf.Bytes())
	return err
}

// Close terminates the UDP connection.
func (s *Sender) Close() error {
	if s.conn != nil {
		_ = s.conn.Close()
	}
	return nil
}
