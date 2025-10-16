// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package logservice

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// LogPacket represents the basic structure of a received log message.
type LogPacket struct {
	Timestamp time.Time `json:"timestamp"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Entity    string    `json:"entity,omitempty"`
	EntityID  string    `json:"entity_id,omitempty"`
}

// StartListener starts a UDP listener on the given address (e.g. "127.0.0.1:1997").
func StartListener(addr string) error {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address: %w", err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("failed to start UDP listener: %w", err)
	}
	defer conn.Close()

	fmt.Printf("[LogService] Listening for logs on %s...\n", addr)

	buf := make([]byte, 4096)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("[LogService] Read error: %v\n", err)
			continue
		}

		var pkt LogPacket
		if err := json.Unmarshal(buf[:n], &pkt); err != nil {
			fmt.Printf("[LogService] Invalid packet: %v\n", err)
			continue
		}

		// Simple display format
		fmt.Printf("%s [%s] %s\n",
			pkt.Timestamp.Format("2006-01-02 15:04:05"),
			pkt.Level,
			pkt.Message)
	}
}
