// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package fsservices

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
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

// relativize turns an absolute path into a logical /relative path.
func (l *LocalFS) relativize(absPath string) string {
	// Normalize separators and ensure root ends without slash
	root := strings.TrimSuffix(l.root, "/")
	p := strings.ReplaceAll(filepath.Clean(absPath), "\\", "/")

	if strings.HasPrefix(p, root) {
		rel := strings.TrimPrefix(p[len(root):], "/")
		if rel == "" {
			return "/"
		}
		return "/" + rel
	}
	// Fallback: if somehow outside root, just return the cleaned path
	return "/" + filepath.Base(p)
}

// ListChildren lists immediate children of the given node identifier (absolute path).
func (l *LocalFS) ListChildren(identifier string) (ListResult, error) {
	var result ListResult

	entries, err := os.ReadDir(identifier)
	if err != nil {
		return result, err
	}

	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}

		fullPath := filepath.Join(identifier, entry.Name())
		fullPath = strings.ReplaceAll(fullPath, "\\", "/")

		rel := l.relativize(fullPath)

		if entry.IsDir() {
			result.Folders = append(result.Folders, Folder{
				Id:           fullPath,   // physical identifier
				ParentId:     identifier, // parent physical path
				DisplayName:  entry.Name(),
				LocationPath: rel, // logical, root-relative path
				LastUpdated:  info.ModTime().Format(time.RFC3339),
				Type:         NodeTypeFolder,
			})
		} else {
			result.Files = append(result.Files, File{
				Id:           fullPath,
				ParentId:     identifier,
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
	if err := os.MkdirAll(fullPath, os.ModePerm); err != nil {
		return Folder{}, err
	}
	info, err := os.Stat(fullPath)
	if err != nil {
		return Folder{}, err
	}
	return Folder{
		Id:           fullPath,
		ParentId:     parentId,
		DisplayName:  name,
		LocationPath: l.relativize(fullPath),
		LastUpdated:  info.ModTime().Format(time.RFC3339),
		Type:         NodeTypeFolder,
	}, nil
}

// UploadFile writes a new file at dest identifier.
func (l *LocalFS) UploadFile(destId string, content io.Reader) (File, error) {
	f, err := os.Create(destId)
	if err != nil {
		return File{}, err
	}
	defer f.Close()
	n, err := io.Copy(f, content)
	if err != nil {
		return File{}, err
	}
	info, _ := os.Stat(destId)
	return File{
		Id:           destId,
		DisplayName:  filepath.Base(destId),
		LocationPath: l.relativize(destId),
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
