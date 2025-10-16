// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package logservice

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// Sender sends log packets to a UDP listener.
type Sender struct {
	Addr       string // e.g. "127.0.0.1:1997"
	Level      string // current threshold
	conn       net.Conn
	minLevelIx int
}

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
func NewSender(addr string, level string) (*Sender, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return &Sender{Addr: addr, Level: level, conn: conn}, nil
}

// Log sends a log message if it meets the current level threshold.
func (s *Sender) Log(level, message, entity, entityID string) error {
	pkt := LogPacket{
		Timestamp: time.Now(),
		Level:     level,
		Message:   message,
		Entity:    entity,
		EntityID:  entityID,
	}

	data, err := json.Marshal(pkt)
	if err != nil {
		return err
	}

	minLevelIx := getLevelIndex(s.Level)
	if minLevelIx == -1 {
		return fmt.Errorf("invalid level: %s", s.Level)
	}

	levelIx := getLevelIndex(level)
	if levelIx == -1 {
		return fmt.Errorf("invalid level: %s", level)
	}

	if levelIx < minLevelIx {
		return nil
	}

	_, err = s.conn.Write(data)
	return err
}

// Close closes the UDP connection.
func (s *Sender) Close() error {
	return s.conn.Close()
}
