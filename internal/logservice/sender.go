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
)

// Sender sends log packets to a UDP listener.
type Sender struct {
	Addr       string // e.g. "127.0.0.1:1997"
	Level      string // threshold, e.g. "info"
	conn       net.Conn
	minLevelIx int

	mu   sync.Mutex      // guards buffer/encoder
	buf  *bytes.Buffer   // reusable JSON buffer
	enc  *json.Encoder   // bound to buf
	tmp  LogPacket       // small scratch value reused per call
}

// getLevelIndex maps log levels to integer weights.
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

// NewSender initializes a UDP log sender.
func NewSender(addr, level string) (*Sender, error) {
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
		Addr:       addr,
		Level:      level,
		conn:       conn,
		minLevelIx: minIx,
		buf:        buf,
		enc:        json.NewEncoder(buf),
	}, nil
}

// Log sends a log message if it meets the threshold.
// Safe for concurrent use.
func (s *Sender) Log(level, message, entity, entityID string) error {
	levelIx := getLevelIndex(level)
	if levelIx == -1 {
		return fmt.Errorf("invalid level: %s", level)
	}
	if levelIx < s.minLevelIx {
		return nil // below threshold
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.tmp.Timestamp = time.Now()
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

// Close closes the UDP connection.
func (s *Sender) Close() error {
	if s.conn != nil {
		_ = s.conn.Close()
	}
	return nil
}
