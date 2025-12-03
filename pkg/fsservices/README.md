# Filesystem Services Package

The **Filesystem Services** package provides a unified abstraction layer for interacting with different filesystem backends. It enables the migration engine to work with multiple storage systems (local filesystem, Spectra simulator, and future cloud storage backends) through a common interface.

---

## Overview

### Importing

```go
import "github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
```

The package now lives at the module root so it can be consumed directly by external
projects (for example, the API service) without going through the Go `internal`
visibility barrier.

The package defines an `FSAdapter` interface that abstracts filesystem operations, allowing the migration engine to traverse and manipulate files and folders without being tied to a specific storage implementation.

### Key Concepts

- **FSAdapter Interface**: Common interface for all filesystem implementations
- **ServiceContext**: Wrapper that associates an adapter with a service name
- **Unified Types**: Shared `Folder` and `File` structures across all adapters
- **Path Normalization**: Consistent path handling regardless of backend

---

## Architecture

### Adapter Pattern

All filesystem backends implement the `FSAdapter` interface:

```go
type FSAdapter interface {
    ListChildren(identifier string) (ListResult, error)
    DownloadFile(identifier string) (io.ReadCloser, error)
    CreateFolder(parentId, name string) (Folder, error)
    UploadFile(destId string, content io.Reader) (File, error)
    NormalizePath(path string) string
}
```

This allows the queue system and workers to operate on any filesystem type without modification.

### Core Types

#### Folder and File

Both `Folder` and `File` structures contain:

- **`Id`**: Backend-specific identifier (absolute path for local, node ID for Spectra)
- **`ParentId`**: Parent's identifier
- **`ParentPath`**: Parent's relative path (root-relative, e.g., `/folder/subfolder`)
- **`DisplayName`**: Human-readable name
- **`LocationPath`**: Relative path from root (e.g., `/folder/subfolder/file.txt`)
- **`LastUpdated`**: Timestamp in RFC3339 format
- **`DepthLevel`**: BFS depth level (0 = root)
- **`Type`**: Node type (`"folder"` or `"file"`)

#### Node Interface

Both `Folder` and `File` implement the `Node` interface:

```go
type Node interface {
    ID() string
    Name() string
    Path() string
    ParentID() string
    NodeType() string
}
```

This allows polymorphic handling of files and folders.

#### ListResult

Returned by `ListChildren()`:

```go
type ListResult struct {
    Folders []Folder
    Files   []File
}
```

#### ListPager and Pagination

For processing large directories in fixed-size pages, use `ListPager`:

```go
// Create a pager from a ListResult
result, err := adapter.ListChildren(nodeIdentifier)
pager := fsservices.NewListPager(result, 100) // 100 items per page

// Iterate through pages
for {
    page, hasMore := pager.Next()
    if !hasMore {
        break
    }
    
    // Process this page
    for _, folder := range page.Folders {
        // Process folder
    }
    for _, file := range page.Files {
        // Process file
    }
    
    fmt.Printf("Page %d: %d items (Total: %d, HasMore: %v)\n",
        page.Page, len(page.Folders)+len(page.Files), page.Total, page.HasMore)
}
```

**ListPage Structure:**
```go
type ListPage struct {
    Folders  []Folder
    Files    []File
    Total    int  // Total number of children across all pages
    Page     int  // 0-based page index
    PageSize int  // Requested maximum number of children per page
    HasMore  bool // Whether more pages exist
}
```

**Note:** `ListPager` provides in-memory pagination over a full `ListResult`. For cloud services with native pagination, adapters can implement their own pagers (e.g., `SpectraFS.NewChildrenPager()`).

#### Path Normalization Helpers

Utility functions for consistent path handling:

```go
// NormalizeLocationPath - Ensures paths use forward slashes, are rooted, and cleaned
normalized := fsservices.NormalizeLocationPath("folder\\subfolder") // Returns "/folder/subfolder"
normalized := fsservices.NormalizeLocationPath("") // Returns "/"

// NormalizeParentPath - Normalizes parent paths but preserves empty strings (for root nodes)
parentPath := fsservices.NormalizeParentPath("/parent") // Returns "/parent"
parentPath := fsservices.NormalizeParentPath("") // Returns "" (for root)
```

---

## Implementations

### LocalFS

**File**: `local.go`

The `LocalFS` adapter interacts with the local filesystem using absolute paths.

**Features:**
- Rooted at a specific directory path
- Uses absolute paths as identifiers
- Converts between absolute and relative paths automatically
- Handles cross-platform path separators (Windows `\` vs Unix `/`)

**Example Usage:**

```go
// Create adapter rooted at /home/user/migration
localFS, err := fsservices.NewLocalFS("/home/user/migration")
if err != nil {
    log.Fatal(err)
}

// List children of root
result, err := localFS.ListChildren("/home/user/migration")
for _, folder := range result.Folders {
    fmt.Printf("Folder: %s\n", folder.LocationPath)
}
```

**Path Handling:**
- `identifier` parameters are **absolute paths** (e.g., `/home/user/migration/folder`)
- `LocationPath` in returned structures is **root-relative** (e.g., `/folder`)
- Paths are normalized to forward slashes internally

---

### SpectraFS

**File**: `spectra.go`

The `SpectraFS` adapter interacts with the Spectra filesystem simulator via the Spectra SDK.

**Features:**
- Works with Spectra node IDs (not paths)
- Supports world-based filtering (multi-tenant isolation)
- Integrates with Spectra SDK for all operations
- Handles Spectra-specific data structures and conversions

**Example Usage:**

```go
// Create adapter for "primary" world with root node ID "root"
spectraFS, err := fsservices.NewSpectraFS(spectraInstance, "root", "primary")
if err != nil {
    log.Fatal(err)
}

// List children of a node (by ID)
result, err := spectraFS.ListChildren("some-node-id")
for _, file := range result.Files {
    fmt.Printf("File: %s (size: %d)\n", file.LocationPath, file.Size)
}
```

**World Filtering:**
- Each adapter instance is bound to a specific "world" (e.g., `"primary"`, `"s1"`, `"s2"`)
- All operations filter results by the configured world
- Enables multi-tenant scenarios where one Spectra instance contains multiple isolated filesystems

**Path Handling:**
- `identifier` parameters are **Spectra node IDs** (e.g., `"abc-123-def"`)
- `LocationPath` in returned structures is the **relative path** from the Spectra root
- Node IDs are opaque identifiers managed by Spectra

**Additional Methods:**

```go
// Create a pager for children (convenience wrapper)
pager, err := spectraFS.NewChildrenPager("node-id", 100)
if err != nil {
    return err
}

// Get the underlying SDK instance (for checking shared instances)
sdkInstance := spectraFS.GetSDKInstance()

// Close the adapter (closes underlying SDK instance)
// Note: Only close once per SDK instance if multiple adapters share it
err := spectraFS.Close()
```

**SDK Instance Management:**
- Multiple `SpectraFS` adapters can share the same SDK instance
- `GetSDKInstance()` allows checking if adapters share an instance
- `Close()` should only be called once per SDK instance to avoid panics

---

## Service Context

The `ServiceContext` wrapper associates a filesystem adapter with a service name:

```go
type ServiceContext struct {
    Name      string    // Service name (e.g., "Spectra-Src", "Windows", "Google Drive")
    Connector FSAdapter // The adapter instance
}
```

**Usage:**

```go
// Create context for source adapter
srcContext := fsservices.NewServiceContext("Spectra-Src", srcAdapter)

// Create context for destination adapter
dstContext := fsservices.NewServiceContext("Local-Dst", dstAdapter)
```

The service context is primarily used for logging and debugging purposes, allowing the system to identify which backend is being used.

---

## Operations

### ListChildren

Lists immediate children (folders and files) of a node.

- **Input**: Node identifier (absolute path for LocalFS, node ID for SpectraFS)
- **Output**: `ListResult` containing `[]Folder` and `[]File`
- **Behavior**: Returns only direct children (no recursion)

**Example:**

```go
result, err := adapter.ListChildren(nodeIdentifier)
if err != nil {
    return err
}

fmt.Printf("Found %d folders and %d files\n", 
    len(result.Folders), len(result.Files))
```

### DownloadFile

Retrieves file content as a stream.

- **Input**: File identifier
- **Output**: `io.ReadCloser` for streaming file data
- **Behavior**: Opens a read stream; caller must close it

**Example:**

```go
reader, err := adapter.DownloadFile(fileIdentifier)
if err != nil {
    return err
}
defer reader.Close()

// Read file content
data, err := io.ReadAll(reader)
```

### CreateFolder

Creates a new folder under a parent node.

- **Input**: Parent identifier, folder name
- **Output**: `Folder` structure for the newly created folder
- **Behavior**: Creates the folder and returns its metadata

**Example:**

```go
newFolder, err := adapter.CreateFolder(parentId, "my-new-folder")
if err != nil {
    return err
}
fmt.Printf("Created folder: %s\n", newFolder.LocationPath)
```

### UploadFile

Writes file content to a destination.

- **Input**: Destination identifier, content `io.Reader`
- **Output**: `File` structure for the newly created file
- **Behavior**: Creates the file and writes content

**Example:**

```go
content := strings.NewReader("Hello, World!")
newFile, err := adapter.UploadFile(destId, content)
if err != nil {
    return err
}
fmt.Printf("Uploaded file: %s (%d bytes)\n", newFile.LocationPath, newFile.Size)
```

### NormalizePath

Normalizes a path string according to the adapter's conventions.

- **Input**: Path string
- **Output**: Normalized path string
- **Behavior**: Cleans and normalizes paths for consistent handling

---

## Usage in Migration Engine

The filesystem adapters are used throughout the migration engine:

1. **Queue Workers**: Use adapters to list children during traversal tasks
2. **Task Execution**: Workers call `ListChildren()` to discover filesystem structure
3. **Comparison**: DST workers use adapters to compare expected vs actual children
4. **Future Extensions**: Upload/copy tasks will use `DownloadFile()` and `UploadFile()`

**Example from Queue Worker:**

```go
// Worker executes a traversal task
result, err := w.fsAdapter.ListChildren(folder.Id)
if err != nil {
    return fmt.Errorf("failed to list children: %w", err)
}

// Process discovered children
for _, childFolder := range result.Folders {
    // Create task for next round
}
```

---

## Extending with New Backends

To add support for a new filesystem backend (e.g., S3, Azure Blob Storage):

1. **Implement `FSAdapter` interface**:
   ```go
   type MyCloudFS struct {
       // ... fields
   }
   
   func (m *MyCloudFS) ListChildren(identifier string) (ListResult, error) {
       // Implementation
   }
   
   func (m *MyCloudFS) DownloadFile(identifier string) (io.ReadCloser, error) {
       // Implementation
   }
   
   // ... implement remaining methods
   ```

2. **Map backend concepts to unified types**:
   - Map backend identifiers to `Folder.Id` / `File.Id`
   - Convert backend paths to relative `LocationPath`
   - Extract metadata (size, timestamps) into unified structures

3. **Handle path normalization**:
   - Normalize paths according to backend conventions
   - Ensure consistent separator usage

4. **Test with existing queue system**:
   - Create adapter instance
   - Pass to queue workers
   - Verify traversal and comparison work correctly

---

## Thread Safety

- **Read Operations**: All `FSAdapter` methods are safe for concurrent reads
- **Write Operations**: Adapters should handle concurrent writes internally if the backend supports it
- **LocalFS**: Uses OS filesystem calls (generally safe for concurrent reads, requires coordination for writes)
- **SpectraFS**: Uses Spectra SDK (thread-safe per SDK documentation)

---

## Error Handling

All adapter methods return errors for:
- Invalid identifiers (node doesn't exist)
- Permission issues
- Network errors (for remote backends)
- Invalid operations (e.g., listing children of a file)

Errors are logged via the global log service if available, and propagated to callers for handling.

---

## Constants

```go
const (
    NodeTypeFile   = "file"
    NodeTypeFolder = "folder"
)
```

These constants are used in the `Type` field of `Folder` and `File` structures.

---

## Summary

The filesystem services package provides:
- ✅ **Unified Interface**: Single API for multiple storage backends
- ✅ **Type Safety**: Consistent `Folder` and `File` structures with `Node` interface
- ✅ **Path Abstraction**: Handles path differences between backends with normalization helpers
- ✅ **Pagination Support**: `ListPager` for processing large directories in pages
- ✅ **Extensibility**: Easy to add new storage backends
- ✅ **Integration**: Seamlessly integrates with queue system and workers
- ✅ **World Isolation**: SpectraFS supports multi-tenant scenarios via world filtering

This abstraction enables the migration engine to work with any filesystem type while keeping the core traversal logic backend-agnostic.
