// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package logservice

import (
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
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
		// Find the absolute path to spawn.go
		spawnPath, err := getSpawnAbsPath()
		if err != nil {
			return fmt.Errorf("unable to locate spawn.go: %v", err)
		}
		// Spawn a new cmd window that runs the listener with abs path
		// NOTE: We quote the path only for the shell command (NOT for Go import!), no malformed import path here.
		cmd = exec.Command("C:\\Windows\\System32\\cmd.exe", "/C", "start", "cmd", "/K",
			fmt.Sprintf("go run %s %s", quoteForWindowsCmd(spawnPath), addr))
	case "darwin":
		// macOS: use Terminal.app
		spawnPath, err := getSpawnAbsPath()
		if err != nil {
			return fmt.Errorf("unable to locate spawn.go: %v", err)
		}
		cmd = exec.Command("osascript", "-e",
			fmt.Sprintf(`tell application "Terminal" to do script "go run '%s' %s"`, spawnPath, addr))
	case "linux":
		// Linux: try common terminal emulators
		spawnPath, err := getSpawnAbsPath()
		if err != nil {
			return fmt.Errorf("unable to locate spawn.go: %v", err)
		}
		for _, term := range []string{"x-terminal-emulator", "gnome-terminal", "xterm"} {
			if _, err := exec.LookPath(term); err == nil {
				cmd = exec.Command(term, "-e",
					fmt.Sprintf("bash -c 'go run \"%s\" %s'", spawnPath, addr))
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

// quoteForWindowsCmd surrounds the path in quotes if it contains a space.
// This ensures we don't get malformed import/argument; used only for the shell command.
func quoteForWindowsCmd(path string) string {
	if len(path) == 0 {
		return path
	}
	if path[0] == '"' && path[len(path)-1] == '"' {
		return path
	}
	// Only quote if space or special chars
	for _, c := range path {
		if c == ' ' || c == '\t' {
			return fmt.Sprintf("\"%s\"", path)
		}
	}
	return path
}

// getSpawnAbsPath returns the absolute path to spawn.go based on the current file's directory.
func getSpawnAbsPath() (string, error) {
	// Use runtime.Caller to get the path to this source file, regardless of the working directory
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("unable to determine the caller source file path")
	}
	// The structure is listener.go → main/spawn.go
	baseDir := filepath.Dir(currentFile)                  // .../pkg/logservice
	spawnPath := filepath.Join(baseDir, "main", "spawn.go")
	return spawnPath, nil
}

// clearConsole runs a clear screen command appropriate to the OS.
// Returns any error encountered.
func clearConsole() error {
	switch runtime.GOOS {
	case "windows":
		// Use the absolute path to cmd.exe in System32 for reliability
		cmd := exec.Command("C:\\Windows\\System32\\cmd.exe", "/C", "cls")
		return cmd.Run()
	case "darwin", "linux":
		cmd := exec.Command("clear")
		return cmd.Run()
	default:
		return nil
	}
}

// RunListener is the actual listener loop (called when --listen flag is passed).
// If a log with Message == "<<CLEAR_SCREEN>>" is received, clears the console.
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

		// If specific log message received, clear console.
		if pkt.Message == "<<CLEAR_SCREEN>>" {
			if err := clearConsole(); err != nil {
				fmt.Printf("[LogService] Failed to clear console: %v\n", err)
			}
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
