// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package fsservices

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Project-Sylos/Migration-Engine/internal/logservice"
	"github.com/Project-Sylos/Spectra/sdk"
)

// SpectraFS implements FSAdapter for Spectra filesystem simulator.
type SpectraFS struct {
	fs     *sdk.SpectraFS
	rootID string // The root node ID for this migration context
}

// NewSpectraFS constructs a new SpectraFS adapter.
func NewSpectraFS(spectraFS *sdk.SpectraFS, rootPath string) (*SpectraFS, error) {
	if spectraFS == nil {
		return nil, fmt.Errorf("spectra filesystem instance cannot be nil")
	}

	// Validate that the root node exists
	rootNode, err := spectraFS.GetNode(rootPath)
	if err != nil {
		return nil, fmt.Errorf("invalid root node ID '%s': %w", rootPath, err)
	}

	if rootNode.Type != NodeTypeFolder {
		return nil, fmt.Errorf("root node must be a folder, got type: %s", rootNode.Type)
	}

	return &SpectraFS{
		fs:     spectraFS,
		rootID: rootPath,
	}, nil
}

// relativize turns a Spectra node path into a logical relative path.
func (s *SpectraFS) relativize(nodeID string, nodeName string) string {
	if nodeID == s.rootID {
		return "/"
	}

	// For now, we'll use the node name as the relative path
	// In a more sophisticated implementation, we might traverse up to build the full path
	return "/" + nodeName
}

// ListChildren lists immediate children of the given node identifier (Spectra node ID).
func (s *SpectraFS) ListChildren(identifier string) (ListResult, error) {
	var result ListResult

	// Get the node to verify it exists and is a folder
	parentNode, err := s.fs.GetNode(identifier)
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

	// List children from Spectra
	listResult, err := s.fs.ListChildren(identifier)
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
		relPath := s.relativize(node.ID, node.Name)
		result.Folders = append(result.Folders, Folder{
			Id:           node.ID,
			ParentId:     identifier,
			DisplayName:  node.Name,
			LocationPath: relPath,
			LastUpdated:  node.LastUpdated.Format(time.RFC3339),
			DepthLevel:   node.DepthLevel,
			Type:         NodeTypeFolder,
		})
	}
	for _, node := range listResult.Files {
		relPath := s.relativize(node.ID, node.Name)
		result.Files = append(result.Files, File{
			Id:           node.ID,
			ParentId:     identifier,
			DisplayName:  node.Name,
			LocationPath: relPath,
			LastUpdated:  node.LastUpdated.Format(time.RFC3339),
			Size:         node.Size,
			DepthLevel:   node.DepthLevel,
			Type:         NodeTypeFile,
		})
	}

	if logservice.LS != nil {
		_ = logservice.LS.Log(
			"trace",
			fmt.Sprintf("Listed %d folders, %d files in %s", len(result.Folders), len(result.Files), s.relativize(identifier, parentNode.Name)),
			"fsservices",
			"spectra",
		)
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
	// Create folder in Spectra
	node, err := s.fs.CreateFolder(parentId, name)
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

	relPath := s.relativize(node.ID, node.Name)

	if logservice.LS != nil {
		_ = logservice.LS.Log(
			"info",
			fmt.Sprintf("Created folder %s", relPath),
			"fsservices",
			"spectra",
		)
	}

	return Folder{
		Id:           node.ID,
		ParentId:     parentId,
		DisplayName:  node.Name,
		LocationPath: relPath,
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

	// Upload file to Spectra
	node, err := s.fs.UploadFile(parentId, fileName, data)
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

	relPath := s.relativize(node.ID, node.Name)

	if logservice.LS != nil {
		_ = logservice.LS.Log(
			"info",
			fmt.Sprintf("Uploaded file %s (%d bytes)", relPath, len(data)),
			"fsservices",
			"spectra",
		)
	}

	return File{
		Id:           node.ID,
		ParentId:     parentId,
		DisplayName:  node.Name,
		LocationPath: relPath,
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
