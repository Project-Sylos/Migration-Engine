// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

const (
	flatCopyFolderA = "alpha"
	flatCopyFolderB = "bravo"
	flatCopyFile    = "sample.txt"
)

// memCopyAdapter is a minimal in-memory FS for flat copy integration tests.
type memCopyAdapter struct {
	rootID   string
	rootName string
	mu       sync.Mutex
	folders  map[string]map[string]types.Folder // parent service id -> name -> folder
	files    map[string]map[string]types.File
	fileData map[string][]byte
	nextID   int
	ops      []string
}

func newMemCopyAdapter(rootID, rootName string) *memCopyAdapter {
	return &memCopyAdapter{
		rootID:   rootID,
		rootName: rootName,
		folders:  make(map[string]map[string]types.Folder),
		files:    make(map[string]map[string]types.File),
		fileData: make(map[string][]byte),
	}
}

func (m *memCopyAdapter) nextServiceID() string {
	m.nextID++
	return fmt.Sprintf("svc-%d", m.nextID)
}

func (m *memCopyAdapter) ListChildren(ctx context.Context, identifier string, depth *int, parentPath string) (types.ListResult, error) {
	return types.ListResult{}, nil
}

func (m *memCopyAdapter) OpenRead(ctx context.Context, fileID string) (io.ReadCloser, error) {
	m.mu.Lock()
	data := m.fileData[fileID]
	m.mu.Unlock()
	if data == nil {
		return nil, fmt.Errorf("file %s not found", fileID)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memCopyAdapter) CreateFolder(ctx context.Context, parentId, name string) (types.Folder, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.folders[parentId] == nil {
		m.folders[parentId] = make(map[string]types.Folder)
	}
	id := m.nextServiceID()
	parentPath := "/"
	if parentId != m.rootID {
		parentPath = types.NormalizeLocationPath("/" + parentId)
	}
	f := types.Folder{
		ServiceID:    id,
		ParentId:     parentId,
		ParentPath:   parentPath,
		DisplayName:  name,
		LocationPath: types.NormalizeLocationPath(parentPath + "/" + name),
		LastUpdated:  time.Now().UTC().Format(time.RFC3339),
		Type:         types.NodeTypeFolder,
	}
	m.folders[parentId][name] = f
	m.ops = append(m.ops, "folder:"+name)
	return f, nil
}

func (m *memCopyAdapter) CreateFile(ctx context.Context, parentID, name string, size int64, metadata map[string]string) (types.File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.files[parentID] == nil {
		m.files[parentID] = make(map[string]types.File)
	}
	id := m.nextServiceID()
	f := types.File{
		ServiceID:    id,
		ParentId:     parentID,
		ParentPath:   "/",
		DisplayName:  name,
		LocationPath: "/" + name,
		LastUpdated:  time.Now().UTC().Format(time.RFC3339),
		Size:         size,
		Type:         types.NodeTypeFile,
	}
	m.files[parentID][name] = f
	m.fileData[id] = bytes.Repeat([]byte("x"), int(size))
	m.ops = append(m.ops, "file:"+name)
	return f, nil
}

func (m *memCopyAdapter) OpenWrite(ctx context.Context, fileID string) (io.WriteCloser, error) {
	return &memWriteCloser{m: m, id: fileID}, nil
}

func (m *memCopyAdapter) NormalizePath(path string) string {
	return types.NormalizeLocationPath(path)
}

func (m *memCopyAdapter) Initialize(masterKey []byte, connectionID string) error { return nil }
func (m *memCopyAdapter) RegisterCredentials(credsData []byte, masterKey []byte, connectionID string) error {
	return nil
}
func (m *memCopyAdapter) HasValidCredentials() bool { return true }

func (m *memCopyAdapter) DeleteNode(ctx context.Context, nodeID string, nodeType string) error {
	return nil
}

type memWriteCloser struct {
	m    *memCopyAdapter
	id   string
	buf  bytes.Buffer
	done bool
}

func (w *memWriteCloser) Write(p []byte) (int, error) {
	if w.done {
		return 0, fmt.Errorf("closed")
	}
	return w.buf.Write(p)
}

func (w *memWriteCloser) Close() error {
	w.m.mu.Lock()
	defer w.m.mu.Unlock()
	w.done = true
	w.m.fileData[w.id] = append([]byte(nil), w.buf.Bytes()...)
	return nil
}

// seedFlatCopyLayout inserts a flat SRC tree: two folders and one file at depth 1.
// When wrongParentPathHash is true, rows are inserted with an incorrect parent_path_hash
// to exercise copy pull join logic against malformed path metadata.
func seedFlatCopyLayout(t *testing.T, database *db.DB, wrongParentPathHash bool) {
	t.Helper()
	const srcRootService = "src-root"
	const dstRootService = "dst-root"

	srcRootID := db.DeterministicNodeID("SRC", db.NodeTypeFolder, "/")
	dstRootID := db.DeterministicNodeID("DST", db.NodeTypeFolder, "/")

	if err := db.InsertRootNode(database, "SRC", &db.NodeState{
		ID: srcRootID, ServiceID: srcRootService, Path: "/", ParentPath: "", Type: db.NodeTypeFolder,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusSuccessful, Depth: 0,
	}); err != nil {
		t.Fatalf("insert src root: %v", err)
	}
	if err := db.InsertRootNode(database, "DST", &db.NodeState{
		ID: dstRootID, ServiceID: dstRootService, Path: "/", ParentPath: "", Type: db.NodeTypeFolder,
		TraversalStatus: db.StatusSuccessful, Depth: 0,
	}); err != nil {
		t.Fatalf("insert dst root: %v", err)
	}

	type child struct {
		name string
		typ  string
		size int64
	}
	children := []child{
		{name: flatCopyFolderA, typ: db.NodeTypeFolder},
		{name: flatCopyFolderB, typ: db.NodeTypeFolder},
		{name: flatCopyFile, typ: db.NodeTypeFile, size: 12},
	}

	err := database.RunWrite(context.Background(), func(s *db.WriteSession) error {
		return s.WithTx(func(w *db.Writer) error {
			now := time.Now().UTC().Format(time.RFC3339)
			for _, ch := range children {
				path := "/" + ch.name
				nodeID := db.DeterministicNodeID("SRC", ch.typ, path)
				node := &db.NodeState{
					ID:              nodeID,
					ServiceID:       "src-" + ch.name,
					ParentID:        srcRootID,
					ParentServiceID: srcRootService,
					Path:            path,
					ParentPath:      "",
					Type:            ch.typ,
					Size:            ch.size,
					MTime:           now,
					Depth:           1,
					TraversalStatus: db.StatusSuccessful,
					CopyStatus:      db.CopyStatusPending,
				}
				if wrongParentPathHash {
					conn, err := database.GetDB()
					if err != nil {
						return err
					}
					_, err = conn.ExecContext(context.Background(),
						`INSERT INTO src_nodes (id, service_id, parent_id, parent_service_id, path, parent_path, path_hash, parent_path_hash, type, size, mtime, depth)
						 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
						node.ID, node.ServiceID, node.ParentID, node.ParentServiceID, node.Path, node.ParentPath,
						db.PathHash(node.Path), db.PathHash(""),
						node.Type, node.Size, node.MTime, node.Depth,
					)
					if err != nil {
						return err
					}
				} else if err := w.AppenderInsert("src_nodes", []*db.NodeState{node}); err != nil {
					return err
				}
				if err := w.InsertStatusEvent("SRC", &db.StatusEvent{
					ID:              nodeID,
					TraversalStatus: db.StatusSuccessful,
					CopyStatus:      db.CopyStatusPending,
					EventTime:       time.Now().UnixNano(),
					Depth:           1,
				}); err != nil {
					return err
				}
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("seed children: %v", err)
	}
}

func TestRunCopyPhase_flatLayoutPass1FoldersBeforePass2(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/flat_copy.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seedFlatCopyLayout(t, database, false)

	src := newMemCopyAdapter("src-root", "src")
	dst := newMemCopyAdapter("dst-root", "dst")
	src.fileData["src-"+flatCopyFile] = []byte("hello sample")

	_, err = RunCopyPhase(CopyPhaseConfig{
		DuckDB:       database,
		SrcAdapter:   src,
		DstAdapter:   dst,
		WorkerCount:  2,
		MaxRetries:   1,
		SkipListener: true,
	})
	if err != nil {
		t.Fatalf("RunCopyPhase: %v", err)
	}

	for _, folder := range []string{flatCopyFolderA, flatCopyFolderB} {
		c, err := database.GetCopyCountAtDepth(1, db.NodeTypeFolder, db.CopyStatusPending, true)
		if err != nil {
			t.Fatalf("pending folder count: %v", err)
		}
		if c > 0 {
			t.Fatalf("folder %q still pending (pending folder count=%d)", folder, c)
		}
		state, err := db.GetNodeByPath(database, "DST", "/"+folder)
		if err != nil || state == nil {
			t.Fatalf("dst folder /%s missing: %v", folder, err)
		}
	}

	filePending, err := database.GetCopyCountAtDepth(1, db.NodeTypeFile, db.CopyStatusPending, true)
	if err != nil {
		t.Fatal(err)
	}
	if filePending > 0 {
		t.Fatalf("%s still pending", flatCopyFile)
	}

	// Pass 1 must create folders before pass 2 copies files.
	firstFile := -1
	for i, op := range dst.ops {
		if op == "file:"+flatCopyFile {
			firstFile = i
			break
		}
	}
	if firstFile < 0 {
		t.Fatal("file never copied")
	}
	for i := 0; i < firstFile; i++ {
		if dst.ops[i] != "folder:"+flatCopyFolderA && dst.ops[i] != "folder:"+flatCopyFolderB {
			t.Fatalf("unexpected op before file copy: %s (ops=%v)", dst.ops[i], dst.ops)
		}
	}
	if firstFile < 2 {
		t.Fatalf("expected both folders before file, ops=%v", dst.ops)
	}
}

func TestRunCopyPhase_flatLayoutWrongParentPathHash(t *testing.T) {
	database, err := db.Open(db.Options{Path: t.TempDir() + "/flat_copy_wrong_hash.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	seedFlatCopyLayout(t, database, true)

	src := newMemCopyAdapter("src-root", "src")
	dst := newMemCopyAdapter("dst-root", "dst")
	src.fileData["src-"+flatCopyFile] = []byte("hello sample")

	_, err = RunCopyPhase(CopyPhaseConfig{
		DuckDB:       database,
		SrcAdapter:   src,
		DstAdapter:   dst,
		WorkerCount:  2,
		MaxRetries:   1,
		SkipListener: true,
	})
	if err != nil {
		t.Fatalf("RunCopyPhase: %v", err)
	}

	c, err := database.GetCopyCountAtDepth(1, db.NodeTypeFolder, db.CopyStatusPending, false)
	if err != nil {
		t.Fatal(err)
	}
	if c > 0 {
		t.Fatalf("wrong parent_path_hash rows: %d folders still pending", c)
	}
}
