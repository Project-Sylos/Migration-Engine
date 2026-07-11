// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package db

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestEncryptedOpenRoundTrip(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enc.db")
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatal(err)
	}

	database, err := Open(Options{Path: dbPath, EncryptionKey: key})
	if err != nil {
		t.Fatalf("open create: %v", err)
	}
	if _, err := database.conn.Exec("CREATE TABLE enc_test (id INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := database.conn.Exec("INSERT INTO enc_test VALUES (7)"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	database2, err := Open(Options{Path: dbPath, EncryptionKey: key})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer database2.Close()
	var v int
	if err := database2.conn.QueryRow("SELECT id FROM enc_test").Scan(&v); err != nil {
		t.Fatalf("select: %v", err)
	}
	if v != 7 {
		t.Fatalf("got %d", v)
	}
}

func TestPlaintextOpenUnchanged(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "plain.db")
	database, err := Open(Options{Path: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.conn.Exec("CREATE TABLE plain_test (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatal(err)
	}
}

func TestEncryptedConcurrentQueriesUseAttachedCatalog(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enc.db")
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatal(err)
	}

	database, err := Open(Options{Path: dbPath, EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	if _, err := database.conn.Exec("CREATE TABLE pool_test (id INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := database.conn.Exec("INSERT INTO pool_test VALUES (1)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var v int
			if err := database.conn.QueryRow("SELECT id FROM pool_test").Scan(&v); err != nil {
				errCh <- err
				return
			}
			if v != 1 {
				errCh <- fmt.Errorf("got %d", v)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestEncryptedRunWriteWhileConnHeld(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enc.db")
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		t.Fatal(err)
	}

	database, err := Open(Options{Path: dbPath, EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	ctx := context.Background()
	held, err := database.conn.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()

	done := make(chan error, 1)
	go func() {
		done <- database.RunWrite(ctx, func(s *WriteSession) error {
			return s.WithTx(func(w *Writer) error {
				return w.AppendQueueStats("held-write-test", QueueStatsPhaseTraversal, "{}")
			})
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunWrite blocked while another pooled conn is held")
	}
}
