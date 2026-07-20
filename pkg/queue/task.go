// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"time"

	"codeberg.org/Sylos/Sylos-FS/pkg/types"
)

// Task types
const (
	TaskTypeSrcTraversal = "src-traversal"
	TaskTypeDstTraversal = "dst-traversal"
	TaskTypeUpload       = "upload"
	TaskTypeCopy         = "copy"
	TaskTypeCopyFolder   = "copy-folder" // Copy phase: create folder
	TaskTypeCopyFile     = "copy-file"   // Copy phase: copy file with streaming
	TaskTypeDeleteFolder = "delete-folder"
	TaskTypeDeleteFile   = "delete-file"
)

// SrcNodeMeta holds Depth and CopyStatus for an SRC node; used by DST tasks at completion to avoid per-child DB lookups.
type SrcNodeMeta struct {
	Depth           int
	TraversalStatus string
	CopyStatus      string
	DeleteStatus    string
	GPLState        string
}

// RetryDstChild holds DST child node meta for retry DST cleanup; populated at pull to avoid per-child DB lookups.
type RetryDstChild struct {
	ID              string
	Depth           int
	TraversalStatus string
}

// RetryDstCleanup holds DST counterpart and its children meta for SRC folder tasks in retry mode; populated at pull.
type RetryDstCleanup struct {
	DstID       string
	DstDepth    int
	DstOldStatus string
	Children    []RetryDstChild
}

// TaskBase represents the foundational structure for all task types.
// Workers lease tasks, mark them Locked, and attempt execution.
// Tasks are identified by ULID (ID) for internal tracking.
type TaskBase struct {
	ID                  string             // ULID for internal tracking (database keys)
	Type                string             // Task type: "src-traversal", "dst-traversal", "upload", etc.
	Folder              types.Folder       // Folder to process (if applicable)
	File                types.File         // File to process (if applicable)
	Locked              bool               // Whether this task is currently leased by a worker
	Attempts            int                // Number of execution attempts
	Status              string             // Execution result: "successful", "failed" (set by queue)
	WorkerResult        string             // Worker execution result: "success", "error" (set by worker before ReportTaskResult)
	LastError           string             // Error message from last execution attempt (set by worker, written by queue)
	ExpectedFolders     []types.Folder     // Expected folders (dst tasks only)
	ExpectedFiles       []types.File       // Expected files (dst tasks only)
	ExpectedSrcIDMap    map[string]string  // Map of Type+Name -> SRC node ID for matching (dst tasks only)
	ExpectedSrcNodeMeta map[string]SrcNodeMeta // SRC node Depth/CopyStatus keyed by SRC ID (dst tasks only, populated at pull)
	RetryDstCleanup     *RetryDstCleanup   // DST counterpart + children meta for SRC folder in retry mode (populated at pull)
	DiscoveredChildren  []ChildResult      // Children discovered during execution
	Round               int                // The round this task belongs to (for buffer coordination)
	LeaseTime           time.Time          // Time when task was leased (for execution time tracking)
	CopyStatus          string             // Current SRC copy status from DB (used to preserve copy_status on traversal completion events)
	DeleteStatus        string             // Current SRC delete status from DB (delete phase)
	// Copy phase specific fields
	CopyPass             int    // Copy pass number (1 for folders, 2 for files)
	SrcLogicalPath       string // SRC root-relative path at pull time (immune to dst adapter path metadata)
	SrcLogicalParentPath string // SRC parent path at pull time
	SrcTraversalStatus   string // SRC node traversal_status at pull time (preserved when writing copy_status events)
	BytesTransferred   int64  // Bytes transferred for file copy tasks
	DstParentID        string // Destination parent folder ServiceID for FS create
	DstParentNodeID    string // Destination parent folder internal UUID (id_map)
	ResolvedDstName    string // Basename/segment from accepted path_events (empty = use original name)
	ParentGPLState     string // Parent gpl_state JSON (GPL cascade)
	GPLState           string // This node's gpl_state JSON (GPL cascade)
	// Transfer checkpoint (file copy); copy_status stays pending while Offset > 0.
	XferOffset   int64
	XferSrcSize  int64
	XferSrcMTime string
	XferDstRef   string
}

// ChildResult represents a discovered child node with its traversal status.
type ChildResult struct {
	Folder        types.Folder // Folder info (if folder)
	File          types.File   // File info (if file)
	Status        string       // "pending", "successful", "missing", "not_on_src"
	IsFile        bool         // true if this is a file, false if folder
	SrcID         string       // ULID of corresponding SRC node (for DST nodes only, set during matching)
		SrcCopyStatus string       // Copy status to update on SRC node (if SrcID is set and match found): "pending", "already_existed", empty if no update needed
}

// Identifier returns the unique identifier for this task (absolute path).
func (t *TaskBase) Identifier() string {
	if t.Folder.ServiceID != "" {
		return t.Folder.ServiceID
	}
	return t.File.ServiceID
}

// LocationPath returns the logical, root-relative path for this task.
func (t *TaskBase) LocationPath() string {
	if t.Folder.LocationPath != "" {
		return t.Folder.LocationPath
	}
	return t.File.LocationPath
}

// IsFolder returns whether this task represents a folder traversal.
// Copy phase: use TaskType (copy-folder) so folder tasks are recognized even when ServiceID is empty;
// traversal/retry still use Folder.ServiceID when Type is not copy-folder/copy-file.
func (t *TaskBase) IsFolder() bool {
	switch t.Type {
	case TaskTypeCopyFolder, TaskTypeDeleteFolder:
		return true
	case TaskTypeCopyFile, TaskTypeDeleteFile:
		return false
	}
	return t.Folder.ServiceID != ""
}

// IsFile returns whether this task represents a file operation.
func (t *TaskBase) IsFile() bool {
	switch t.Type {
	case TaskTypeCopyFile, TaskTypeDeleteFile:
		return true
	case TaskTypeCopyFolder, TaskTypeDeleteFolder:
		return false
	}
	return t.File.ServiceID != ""
}

// UploadTask represents a task to upload a file from source to destination.
type UploadTask struct {
	TaskBase
	SrcId  string // Source file identifier
	DstId  string // Destination parent folder identifier
	DstCtx types.ServiceContext
}

// CopyTask represents a generic copy operation.
type CopyTask struct {
	TaskBase
	SrcId  string
	DstId  string
	DstCtx types.ServiceContext
}
