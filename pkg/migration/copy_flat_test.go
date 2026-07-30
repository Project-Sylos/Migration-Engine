// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/stats"
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

func (m *memCopyAdapter) CreateFolder(ctx context.Context, parentId, name string, metadata map[string]string) (types.Folder, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.folders[parentId] == nil {
		m.folders[parentId] = make(map[string]types.Folder)
	}
	id := m.nextServiceID()
	parentPath := types.LogicalParentFromCreateMetadata(metadata, "/")
	if metadata == nil || (strings.TrimSpace(metadata["location_path"]) == "" && strings.TrimSpace(metadata["parent_path"]) == "") {
		parentPath = "/"
		if parentId != m.rootID {
			parentPath = types.NormalizeLocationPath("/" + parentId)
		}
	}
	loc := types.ChildLocationFromCreateMetadata(metadata, parentPath, name)
	f := types.Folder{
		ServiceID:    id,
		ParentId:     parentId,
		ParentPath:   parentPath,
		DisplayName:  name,
		LocationPath: loc,
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

// seedFlatCopyLayout inserts a flat SRC tree: two folders and one file at depth 1 under the SRC root,
// plus DST root and root id_map so copy parent resolution works.
func seedFlatCopyLayout(t *testing.T, database *db.DB) {
	t.Helper()
	const srcRootService = "src-root"
	const dstRootService = "dst-root"

	srcRootID := db.MintNodeID("SRC", "", db.NodeTypeFolder, "/")
	dstRootID := db.MintNodeID("DST", "", db.NodeTypeFolder, "/")

	if err := pull.InsertRootNode(database, "SRC", &db.NodeState{
		ID: srcRootID, ServiceID: srcRootService, Path: "/", ParentPath: "", Type: db.NodeTypeFolder,
		TraversalStatus: db.StatusSuccessful, CopyStatus: db.CopyStatusAlreadyExisted, Depth: 0,
	}); err != nil {
		t.Fatalf("insert src root: %v", err)
	}
	if err := pull.InsertRootNode(database, "DST", &db.NodeState{
		ID: dstRootID, ServiceID: dstRootService, Path: "/", ParentPath: "", Type: db.NodeTypeFolder,
		TraversalStatus: db.StatusSuccessful, Depth: 0,
	}); err != nil {
		t.Fatalf("insert dst root: %v", err)
	}
	database.AppendIDMapEvent(db.IDMapEvent{
		SrcInternalID: srcRootID,
		DstInternalID: dstRootID,
		Source:        db.IDMapSourceRootSeed,
		Status:        db.IDMapStatusActive,
	})
	if err := database.Flush(); err != nil {
		t.Fatalf("flush id_map: %v", err)
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
				nodeID := db.MintNodeID("SRC", srcRootID, ch.typ, ch.name)
				node := &db.NodeState{
					ID:              nodeID,
					ServiceID:       "src-" + ch.name,
					ParentID:        srcRootID,
					ParentServiceID: srcRootService,
					Path:            path,
					ParentPath:      "/",
					Name:            ch.name,
					Type:            ch.typ,
					Size:            ch.size,
					MTime:           now,
					Depth:           1,
					TraversalStatus: db.StatusSuccessful,
					CopyStatus:      db.CopyStatusPending,
				}
				if err := w.AppenderInsert("src_nodes", []*db.NodeState{node}); err != nil {
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

	seedFlatCopyLayout(t, database)

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
		c, err := stats.GetCopyCountAtDepth(database, 1, db.NodeTypeFolder, db.CopyStatusPending, true)
		if err != nil {
			t.Fatalf("pending folder count: %v", err)
		}
		if c > 0 {
			t.Fatalf("folder %q still pending (pending folder count=%d)", folder, c)
		}
		state, err := pull.GetNodeByPath(database, "DST", "/"+folder)
		if err != nil || state == nil {
			t.Fatalf("dst folder /%s missing: %v", folder, err)
		}
	}

	filePending, err := stats.GetCopyCountAtDepth(database, 1, db.NodeTypeFile, db.CopyStatusPending, true)
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
