// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package logservice

import (
	"encoding/json"
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

	_, err = s.conn.Write(data)
	return err
}

// Close closes the UDP connection.
func (s *Sender) Close() error {
	return s.conn.Close()
}
