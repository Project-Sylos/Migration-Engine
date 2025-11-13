// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package fsservices

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Project-Sylos/Migration-Engine/pkg/logservice"
	"github.com/Project-Sylos/Spectra/sdk"
)

// SpectraFS implements FSAdapter for Spectra filesystem simulator.
type SpectraFS struct {
	fs     *sdk.SpectraFS
	rootID string // The root node ID (now always "root" in single-table design)
	world  string // The world name ("primary", "s1", "s2", etc.) for filtering queries
}

// NewSpectraFS constructs a new SpectraFS adapter.
// rootID is the node ID (typically "root"), and world is the world name ("primary", "s1", etc.).
func NewSpectraFS(spectraFS *sdk.SpectraFS, rootID string, world string) (*SpectraFS, error) {
	if spectraFS == nil {
		return nil, fmt.Errorf("spectra filesystem instance cannot be nil")
	}

	if world == "" {
		world = "primary" // Default to primary world if not specified
	}

	// Validate that the root node exists using request struct
	rootNode, err := spectraFS.GetNode(&sdk.GetNodeRequest{
		ID: rootID,
	})
	if err != nil {
		return nil, fmt.Errorf("invalid root node ID '%s': %w", rootID, err)
	}

	if rootNode.Type != NodeTypeFolder {
		return nil, fmt.Errorf("root node must be a folder, got type: %s", rootNode.Type)
	}

	return &SpectraFS{
		fs:     spectraFS,
		rootID: rootID,
		world:  world,
	}, nil
}

// ListChildren lists immediate children of the given node identifier (Spectra node ID).
func (s *SpectraFS) ListChildren(identifier string) (ListResult, error) {
	var result ListResult

	// Get the node to verify it exists and is a folder using request struct
	parentNode, err := s.fs.GetNode(&sdk.GetNodeRequest{
		ID: identifier,
	})
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log(
				"error",
				fmt.Sprintf("Failed to get node %s: %v", identifier, err),
				"fsservices",
				"spectra",
			)
		}
		return result, err
	}

	if parentNode.Type != NodeTypeFolder {
		return result, fmt.Errorf("node %s is not a folder", identifier)
	}


	// List children from Spectra using request struct with world filter
	listResult, err := s.fs.ListChildren(&sdk.ListChildrenRequest{
		ParentID:  identifier,
		TableName: s.world, // Filter by world (e.g., "primary", "s1")
	})
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log(
				"error",
				fmt.Sprintf("Failed to list children of %s: %v", identifier, err),
				"fsservices",
				"spectra",
			)
		}
		return result, err
	}

	// Convert Spectra nodes to our internal format
	for _, node := range listResult.Folders {
		result.Folders = append(result.Folders, Folder{
			Id:           node.ID,
			ParentId:     identifier,
			ParentPath:   node.ParentPath,
			DisplayName:  node.Name,
			LocationPath: node.Path,
			LastUpdated:  node.LastUpdated.Format(time.RFC3339),
			DepthLevel:   node.DepthLevel,
			Type:         NodeTypeFolder,
		})
	}
	for _, node := range listResult.Files {
		result.Files = append(result.Files, File{
			Id:           node.ID,
			ParentId:     identifier,
			ParentPath:   node.ParentPath, // parent's relative path
			DisplayName:  node.Name,
			LocationPath: node.Path,
			LastUpdated:  node.LastUpdated.Format(time.RFC3339),
			Size:         node.Size,
			DepthLevel:   node.DepthLevel,
			Type:         NodeTypeFile,
		})
	}

	return result, nil
}

// DownloadFile retrieves file data from Spectra and returns it as a ReadCloser.
func (s *SpectraFS) DownloadFile(identifier string) (io.ReadCloser, error) {
	// Get file data from Spectra
	fileData, _, err := s.fs.GetFileData(identifier)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log(
				"error",
				fmt.Sprintf("Failed to get file data for %s: %v", identifier, err),
				"fsservices",
				"spectra",
			)
		}
		return nil, err
	}

	// Convert file data to ReadCloser
	return io.NopCloser(strings.NewReader(string(fileData))), nil
}

// CreateFolder creates a new folder under the specified parent node.
func (s *SpectraFS) CreateFolder(parentId, name string) (Folder, error) {
	// Create folder in Spectra using request struct with world
	node, err := s.fs.CreateFolder(&sdk.CreateFolderRequest{
		ParentID:  parentId,
		Name:      name,
		TableName: s.world, // Create in the configured world
	})
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log(
				"error",
				fmt.Sprintf("Failed to create folder %s in parent %s: %v", name, parentId, err),
				"fsservices",
				"spectra",
			)
		}
		return Folder{}, err
	}


	if logservice.LS != nil {
		_ = logservice.LS.Log(
			"info",
			fmt.Sprintf("Created folder %s", strings.Join([]string{node.ParentPath, node.Name}, "/")),
			"fsservices",
			"spectra",
		)
	}

	return Folder{
		Id:           node.ID,
		ParentId:     parentId,
		ParentPath:   node.ParentPath,
		DisplayName:  node.Name,
		LocationPath: node.Path,
		LastUpdated:  node.LastUpdated.Format(time.RFC3339),
		DepthLevel:   node.DepthLevel,
		Type:         NodeTypeFolder,
	}, nil
}

// UploadFile uploads a file to Spectra under the specified parent node.
func (s *SpectraFS) UploadFile(parentId string, content io.Reader) (File, error) {
	// Read all content
	data, err := io.ReadAll(content)
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log(
				"error",
				fmt.Sprintf("Failed to read content for upload to %s: %v", parentId, err),
				"fsservices",
				"spectra",
			)
		}
		return File{}, err
	}

	// Generate a name for the file (in a real scenario, this might come from metadata)
	fileName := fmt.Sprintf("uploaded_file_%d", time.Now().Unix())

	// Upload file to Spectra using request struct with world
	node, err := s.fs.UploadFile(&sdk.UploadFileRequest{
		ParentID:  parentId,
		Name:      fileName,
		Data:      data,
		TableName: s.world, // Upload to the configured world
	})
	if err != nil {
		if logservice.LS != nil {
			_ = logservice.LS.Log(
				"error",
				fmt.Sprintf("Failed to upload file %s to parent %s: %v", fileName, parentId, err),
				"fsservices",
				"spectra",
			)
		}
		return File{}, err
	}

	if logservice.LS != nil {
		_ = logservice.LS.Log(
			"info",
			fmt.Sprintf("Uploaded file %s (%d bytes)", node.Path, len(data)),
			"fsservices",
			"spectra",
		)
	}

	return File{
		Id:           node.ID,
		ParentId:     parentId,
		ParentPath:   node.ParentPath,
		DisplayName:  node.Name,
		LocationPath: node.Path,
		LastUpdated:  node.LastUpdated.Format(time.RFC3339),
		Size:         node.Size,
		DepthLevel:   node.DepthLevel,
		Type:         NodeTypeFile,
	}, nil
}

// NormalizePath normalizes a Spectra node ID or path string.
func (s *SpectraFS) NormalizePath(path string) string {
	// For Spectra, we primarily work with node IDs
	// This could be enhanced to handle path-based lookups if needed
	return strings.TrimSpace(path)
}
