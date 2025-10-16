// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package fsservices

import (
	"io"
)

const (
	NodeTypeFile   = "file"
	NodeTypeFolder = "folder"
)

// Folder represents a basic folder with attributes like name, path, identifier, and parentID.
type Folder struct {
	Id           string
	ParentId     string
	DisplayName  string
	LocationPath string
	LastUpdated  string
	DepthLevel   int
	Type         string // "folder"
}

func (f Folder) ID() string       { return f.Id }
func (f Folder) Name() string     { return f.DisplayName }
func (f Folder) Path() string     { return f.LocationPath }
func (f Folder) ParentID() string { return f.ParentId }
func (f Folder) NodeType() string { return f.Type }

// File represents a basic file with attributes like name, path, identifier, and parentID.
type File struct {
	Id           string
	ParentId     string
	DisplayName  string
	LocationPath string
	LastUpdated  string
	DepthLevel   int
	Size         int64
	Type         string // "file"
}

func (f File) ID() string       { return f.Id }
func (f File) Name() string     { return f.DisplayName }
func (f File) Path() string     { return f.LocationPath }
func (f File) ParentID() string { return f.ParentId }
func (f File) NodeType() string { return f.Type }

type Node interface {
	ID() string
	Name() string
	Path() string
	ParentID() string
	NodeType() string
}

type ListResult struct {
	Folders []Folder
	Files   []File
}

type FSAdapter interface {
	ListChildren(identifier string) (ListResult, error)
	DownloadFile(identifier string) (io.ReadCloser, error)
	CreateFolder(parentId, name string) (Folder, error)
	UploadFile(destId string, content io.Reader) (File, error)
	NormalizePath(path string) string
}

type ServiceContext struct {
	Name      string    // 'Windows', 'Dropbox', 'Google Drive', etc.
	Connector FSAdapter // The pointer to the actual instance of the service adapter
}
