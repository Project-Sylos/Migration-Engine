// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package logservice

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"time"
)

// LogPacket represents the basic structure of a received log message.
type LogPacket struct {
	Timestamp time.Time `json:"timestamp"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Entity    string    `json:"entity,omitempty"`
	EntityID  string    `json:"entity_id,omitempty"`
	Queue     string    `json:"queue,omitempty"`
}

// StartListener spawns a new terminal window with a UDP listener on the given address.
// This is the default behavior when called from main application.
func StartListener(addr string) error {
	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "windows":
		// Spawn a new cmd window that runs the listener
		cmd = exec.Command("C:\\Windows\\System32\\cmd.exe", "/C", "start", "cmd", "/K",
			fmt.Sprintf("go run pkg/logservice/main/spawn.go %s", addr))
	case "darwin":
		// macOS: use Terminal.app
		cmd = exec.Command("osascript", "-e",
			fmt.Sprintf(`tell application "Terminal" to do script "cd %s && go run pkg/logservice/main/spawn.go %s"`,
				getCurrentDir(), addr))
	case "linux":
		// Linux: try common terminal emulators
		for _, term := range []string{"x-terminal-emulator", "gnome-terminal", "xterm"} {
			if _, err := exec.LookPath(term); err == nil {
				cmd = exec.Command(term, "-e",
					fmt.Sprintf("bash -c 'cd %s && go run pkg/logservice/main/spawn.go %s'",
						getCurrentDir(), addr))
				break
			}
		}
		if cmd == nil {
			return fmt.Errorf("no suitable terminal emulator found")
		}
	default:
		return fmt.Errorf("unsupported operating system: %s", runtime.GOOS)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to spawn listener terminal: %w", err)
	}

	return nil
}

// getCurrentDir returns the current working directory.
func getCurrentDir() string {
	dir, err := os.Getwd()
	if err != nil {
		return "."
	}
	return dir
}

// RunListener is the actual listener loop (called when --listen flag is passed).
func RunListener(addr string) error {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address: %w", err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("failed to start UDP listener: %w", err)
	}
	defer conn.Close()

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
		fmt.Printf("%s [%s] %s [%s]\n",
			pkt.Timestamp.Format("2006-01-02 15:04:05"),
			pkt.Level,
			pkt.Message,
			pkt.Queue)
	}
}
