// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package fsservices

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
)

// LocalFS implements FSAdapter for the local filesystem.
type LocalFS struct {
	root string // absolute, normalized root path for this migration
}

// NewLocalFS constructs a new LocalFS adapter rooted at the given path.
func NewLocalFS(rootPath string) (*LocalFS, error) {
	abs, err := filepath.Abs(rootPath)
	if err != nil {
		return nil, err
	}
	// Normalize to forward slashes
	abs = strings.ReplaceAll(filepath.Clean(abs), "\\", "/")
	return &LocalFS{root: abs}, nil
}

// relativize builds a relative path from the parent's path and the node name.
func (l *LocalFS) relativize(nodeName string, parentRelPath string) string {
	// If parent is root, child path is /{childName}
	if parentRelPath == "/" {
		return "/" + nodeName
	}

	// Otherwise, build path as {parentRelPath}/{name}
	return parentRelPath + "/" + nodeName
}

// ListChildren lists immediate children of the given node identifier (absolute path).
func (l *LocalFS) ListChildren(identifier string) (ListResult, error) {
	var result ListResult

	// Get parent's relative path by stripping root
	normalizedParentId := strings.ReplaceAll(identifier, "\\", "/")
	root := strings.TrimSuffix(l.root, "/")
	p := strings.ReplaceAll(filepath.Clean(normalizedParentId), "\\", "/")
	var parentRelPath string
	if p == root || p == root+"/" {
		parentRelPath = "/"
	} else if strings.HasPrefix(p, root) {
		rel := strings.TrimPrefix(p[len(root):], "/")
		if rel == "" {
			parentRelPath = "/"
		} else {
			parentRelPath = "/" + rel
		}
	} else {
		parentRelPath = "/"
	}

	entries, err := os.ReadDir(identifier)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log(
				"error",
				fmt.Sprintf("Failed to read directory %s: %v", identifier, err),
				"fsservices",
				"local",
			)
		}
		return result, err
	}

	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			if logservice.LS != nil {
				_ = logservice.LS.Log(
					"warning",
					fmt.Sprintf("Failed to get info for %s: %v", entry.Name(), err),
					"fsservices",
					"local",
				)
			}
			continue
		}

		fullPath := filepath.Join(identifier, entry.Name())
		fullPath = strings.ReplaceAll(fullPath, "\\", "/")

		// Use parent's relative path to build child's relative path
		rel := l.relativize(entry.Name(), parentRelPath)

		if entry.IsDir() {
			result.Folders = append(result.Folders, Folder{
				Id:           fullPath,      // physical identifier
				ParentId:     identifier,    // parent physical path
				ParentPath:   parentRelPath, // parent's relative path
				DisplayName:  entry.Name(),
				LocationPath: rel, // logical, root-relative path
				LastUpdated:  info.ModTime().Format(time.RFC3339),
				Type:         NodeTypeFolder,
			})
		} else {
			result.Files = append(result.Files, File{
				Id:           fullPath,
				ParentId:     identifier,
				ParentPath:   parentRelPath, // parent's relative path
				DisplayName:  entry.Name(),
				LocationPath: rel,
				LastUpdated:  info.ModTime().Format(time.RFC3339),
				Size:         info.Size(),
				Type:         NodeTypeFile,
			})
		}
	}

	return result, nil
}

// DownloadFile opens the absolute file path (identifier) for streaming.
func (l *LocalFS) DownloadFile(identifier string) (io.ReadCloser, error) {
	return os.Open(identifier)
}

// CreateFolder creates a new folder under a parent absolute path.
func (l *LocalFS) CreateFolder(parentId, name string) (Folder, error) {
	fullPath := filepath.Join(parentId, name)
	fullPath = strings.ReplaceAll(fullPath, "\\", "/")

	// Get parent's relative path by stripping root
	normalizedParentId := strings.ReplaceAll(parentId, "\\", "/")
	root := strings.TrimSuffix(l.root, "/")
	p := strings.ReplaceAll(filepath.Clean(normalizedParentId), "\\", "/")
	var parentRelPath string
	if p == root || p == root+"/" {
		parentRelPath = "/"
	} else if strings.HasPrefix(p, root) {
		rel := strings.TrimPrefix(p[len(root):], "/")
		if rel == "" {
			parentRelPath = "/"
		} else {
			parentRelPath = "/" + rel
		}
	} else {
		parentRelPath = "/"
	}

	if err := os.MkdirAll(fullPath, os.ModePerm); err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log(
				"error",
				fmt.Sprintf("Failed to create folder %s: %v", l.relativize(name, parentRelPath), err),
				"fsservices",
				"local",
			)
		}
		return Folder{}, err
	}
	info, err := os.Stat(fullPath)
	if err != nil {
		return Folder{}, err
	}

	// Use parent's relative path to build child's relative path
	relPath := l.relativize(name, parentRelPath)

	if logservice.LS != nil {
		_ = logservice.LS.Log(
			"info",
			fmt.Sprintf("Created folder %s", relPath),
			"fsservices",
			"local",
		)
	}

	return Folder{
		Id:           fullPath,
		ParentId:     parentId,
		ParentPath:   parentRelPath,
		DisplayName:  name,
		LocationPath: relPath,
		LastUpdated:  info.ModTime().Format(time.RFC3339),
		Type:         NodeTypeFolder,
	}, nil
}

// UploadFile writes a new file at dest identifier.
func (l *LocalFS) UploadFile(destId string, content io.Reader) (File, error) {
	// Normalize destination path
	normalizedDestId := strings.ReplaceAll(destId, "\\", "/")

	// Get parent directory and normalize it
	parentDir := filepath.Dir(normalizedDestId)
	parentDir = strings.ReplaceAll(parentDir, "\\", "/")

	// Get parent's relative path by stripping root
	root := strings.TrimSuffix(l.root, "/")
	p := strings.ReplaceAll(filepath.Clean(parentDir), "\\", "/")
	var parentRelPath string
	if p == root || p == root+"/" {
		parentRelPath = "/"
	} else if strings.HasPrefix(p, root) {
		rel := strings.TrimPrefix(p[len(root):], "/")
		if rel == "" {
			parentRelPath = "/"
		} else {
			parentRelPath = "/" + rel
		}
	} else {
		parentRelPath = "/"
	}

	nodeName := filepath.Base(normalizedDestId)

	f, err := os.Create(destId)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log(
				"error",
				fmt.Sprintf("Failed to create file %s: %v", l.relativize(nodeName, parentRelPath), err),
				"fsservices",
				"local",
			)
		}
		return File{}, err
	}
	defer f.Close()
	n, err := io.Copy(f, content)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log(
				"error",
				fmt.Sprintf("Failed to write file %s: %v", l.relativize(nodeName, parentRelPath), err),
				"fsservices",
				"local",
			)
		}
		return File{}, err
	}
	info, _ := os.Stat(destId)

	// Use parent's relative path to build file's relative path
	relPath := l.relativize(nodeName, parentRelPath)

	if logservice.LS != nil {
		_ = logservice.LS.Log(
			"info",
			fmt.Sprintf("Uploaded file %s (%d bytes)", relPath, n),
			"fsservices",
			"local",
		)
	}

	return File{
		Id:           destId,
		ParentId:     parentDir, // Note: this is the absolute parent path, not destId's parent
		ParentPath:   parentRelPath,
		DisplayName:  nodeName,
		LocationPath: relPath,
		LastUpdated:  info.ModTime().Format(time.RFC3339),
		Size:         n,
		Type:         NodeTypeFile,
	}, nil
}

// NormalizePath cleans and normalizes any incoming path string.
func (l *LocalFS) NormalizePath(path string) string {
	p := filepath.Clean(path)
	p = strings.ReplaceAll(p, "\\", "/")
	return strings.TrimSuffix(p, "/")
}
