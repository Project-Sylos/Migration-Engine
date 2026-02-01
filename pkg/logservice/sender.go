// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package logservice

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
)

// LS is the global log service sender instance.
// It must be initialized via InitGlobalLogger before use.
var LS *Sender

// InitGlobalLogger initializes the global LS instance.
// Logs are persisted to a dedicated Bolt file derived from mainDB.Path() (e.g. migration.db -> migration_logs.db).
// If mainDB.Path() is empty, log persistence is skipped (UDP only).
func InitGlobalLogger(mainDB *db.DB, addr, level string) error {
	var logDB *db.DB
	if mainDB != nil {
		logDBPath := deriveLogDBPath(mainDB.Path())
		if logDBPath != "" {
			var err error
			logDB, err = db.OpenLogDB(db.Options{Path: logDBPath})
			if err != nil {
				return fmt.Errorf("failed to open log DB: %w", err)
			}
		}
	}
	sender, err := NewSender(logDB, addr, level)
	if err != nil {
		if logDB != nil {
			_ = logDB.Close()
		}
		return fmt.Errorf("failed to initialize global logger: %w", err)
	}
	LS = sender

	if err := LS.Log("info", "Test log", "test", "test"); err != nil {
		_ = LS.Close()
		return fmt.Errorf("failed to send test log: %w", err)
	}
	return nil
}

// deriveLogDBPath returns the path for the dedicated log DB file (e.g. migration.db -> migration_logs.db).
// Returns empty string if mainPath is empty (no log persistence).
func deriveLogDBPath(mainPath string) string {
	mainPath = strings.TrimSpace(mainPath)
	if mainPath == "" {
		return ""
	}
	dir := filepath.Dir(mainPath)
	base := filepath.Base(mainPath)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	if name == "" {
		name = "migration"
	}
	return filepath.Join(dir, name+"_logs.db")
}

// Sender transmits logs over UDP and optionally writes them to a dedicated log DB.
type Sender struct {
	logDB     *db.DB        // dedicated log Bolt DB (owned by Sender when set; closed in Close())
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
		logBuffer = db.NewLogBuffer(logDB, 500, 2*time.Second, db.DefaultLogShardCap)
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

// Close terminates the UDP connection, stops the log buffer, and closes the log DB if owned.
func (s *Sender) Close() error {
	if s.logBuffer != nil {
		s.logBuffer.Stop()
		s.logBuffer = nil
	}
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
	if s.logDB != nil {
		_ = s.logDB.Close()
		s.logDB = nil
	}
	return nil
}
