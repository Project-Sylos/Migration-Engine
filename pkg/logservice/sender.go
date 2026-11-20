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
	"codeberg.org/Sylos/Migration-Engine/pkg/db/logbuf"
	"github.com/google/uuid"
)

const defaultBatchSize = 50_000

// LS is the global log service sender instance.
// It must be initialized via InitGlobalLogger before use.
var LS *Sender

// InitGlobalLogger initializes the global LS instance.
// Logs are persisted to logDB (typically the migration logs sidecar) when non-nil.
// UDP threshold defaults to level; persist threshold defaults to "trace".
func InitGlobalLogger(logDB *db.DB, addr, level string) error {
	return InitGlobalLoggerPersist(logDB, addr, level, "trace")
}

// InitGlobalLoggerPersist is InitGlobalLogger with an explicit persist_log_level.
func InitGlobalLoggerPersist(logDB *db.DB, addr, udpLevel, persistLevel string) error {
	sender, err := NewSenderPersist(logDB, addr, udpLevel, persistLevel)
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

// Sender transmits logs over UDP and optionally writes them to the logs sidecar.
type Sender struct {
	logDB         *db.DB
	logBuffer     *logbuf.LogBuffer
	Addr          string
	Level         string // UDP threshold
	PersistLevel  string // disk threshold (live ring always receives all emits)
	conn          net.Conn
	minLevelIx    int
	minPersistIx  int
	mu            sync.Mutex
	buf           *bytes.Buffer
	enc           *json.Encoder
	tmp           LogPacket
	recent        *recentRing
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

// NewSender initializes a sender with persist threshold "trace".
func NewSender(logDB *db.DB, addr, level string) (*Sender, error) {
	return NewSenderPersist(logDB, addr, level, "trace")
}

// NewSenderPersist initializes a dual-channel sender with separate UDP and persist thresholds.
func NewSenderPersist(logDB *db.DB, addr, udpLevel, persistLevel string) (*Sender, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	minIx := getLevelIndex(udpLevel)
	if minIx == -1 {
		return nil, fmt.Errorf("invalid threshold level: %s", udpLevel)
	}
	if persistLevel == "" {
		persistLevel = "trace"
	}
	persistIx := getLevelIndex(persistLevel)
	if persistIx == -1 {
		return nil, fmt.Errorf("invalid persist level: %s", persistLevel)
	}
	buf := new(bytes.Buffer)

	var logBuffer *logbuf.LogBuffer
	if logDB != nil {
		logBuffer = logbuf.NewLogBuffer(logDB, defaultBatchSize, 3*time.Second)
	}

	return &Sender{
		logDB:        logDB,
		logBuffer:    logBuffer,
		Addr:         addr,
		Level:        udpLevel,
		PersistLevel: persistLevel,
		conn:         conn,
		minLevelIx:   minIx,
		minPersistIx: persistIx,
		buf:          buf,
		enc:          json.NewEncoder(buf),
		recent:       newRecentRing(recentLogCap),
	}, nil
}

// Recent returns the newest in-memory log lines (live UI). Does not read the DB.
func (s *Sender) Recent(limit int) []RecentLog {
	if s == nil {
		return nil
	}
	return s.recent.recent(limit)
}

// LogFields carries optional structured fields for one emit.
type LogFields struct {
	Action string
	NodeID string
	Queue  string
}

// Log sends the message via UDP (if level >= UDP threshold) and optionally to the log DB.
// The in-memory recent ring always receives the line. Safe for concurrent use.
func (s *Sender) Log(level, message, entity, entityID string, queues ...string) error {
	queue := ""
	if len(queues) > 0 {
		queue = queues[0]
	}
	return s.LogStructured(level, message, entity, entityID, LogFields{Queue: queue})
}

// LogStructured emits a log with action/node/queue metadata.
func (s *Sender) LogStructured(level, message, entity, entityID string, fields LogFields) error {
	if s == nil {
		return nil
	}
	timestamp := time.Now()
	id := uuid.New().String()
	s.recent.add(RecentLog{
		ID: id, At: timestamp, Level: level, Message: message,
		Entity: entity, EntityID: entityID, Queue: fields.Queue,
		Action: fields.Action, NodeID: fields.NodeID,
	})
	levelIx := getLevelIndex(level)
	if levelIx == -1 {
		return fmt.Errorf("invalid level: %s", level)
	}
	if s.logBuffer != nil && levelIx >= s.minPersistIx {
		s.logBuffer.Add(logbuf.LogEntry{
			ID:        id,
			Timestamp: timestamp.Format(time.RFC3339Nano),
			Level:     level,
			Entity:    entity,
			EntityID:  entityID,
			Message:   message,
			Queue:     fields.Queue,
			Action:    fields.Action,
			NodeID:    fields.NodeID,
		})
	}

	if levelIx < s.minLevelIx {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn == nil {
		return nil
	}

	s.tmp.Timestamp = timestamp
	s.tmp.Level = level
	s.tmp.Message = message
	s.tmp.Entity = entity
	s.tmp.EntityID = entityID
	s.tmp.Queue = fields.Queue

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
