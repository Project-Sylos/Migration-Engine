// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"database/sql"
	"fmt"

	"github.com/Project-Sylos/Migration-Engine/pkg/db"
	"github.com/Project-Sylos/Migration-Engine/pkg/fsservices"
)

// LoadRootFolders returns root folder rows (depth_level=0) with traversal_status='Pending' from the given table.
func LoadRootFolders(database *db.DB, table string) ([]fsservices.Folder, error) {
	query := fmt.Sprintf("SELECT id, parent_id, name, path, parent_path, type, depth_level, size, last_updated FROM %s WHERE type = 'folder' AND traversal_status = 'Pending' AND depth_level = 0", table)
	rows, err := database.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var folders []fsservices.Folder
	for rows.Next() {
		var (
			id, parentID, name, path, parentPath, nodeType string
			depthLevel                                     int
			size                                           sql.NullInt64
			lastUpdated                                    sql.NullString
		)
		if err := rows.Scan(&id, &parentID, &name, &path, &parentPath, &nodeType, &depthLevel, &size, &lastUpdated); err != nil {
			return nil, err
		}
		if nodeType != fsservices.NodeTypeFolder {
			continue
		}

		folder := fsservices.Folder{
			Id:           id,
			ParentId:     parentID,
			ParentPath:   fsservices.NormalizeParentPath(parentPath),
			DisplayName:  name,
			LocationPath: fsservices.NormalizeLocationPath(path),
			DepthLevel:   depthLevel,
			Type:         fsservices.NodeTypeFolder,
		}
		if lastUpdated.Valid {
			folder.LastUpdated = lastUpdated.String
		}
		folders = append(folders, folder)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return folders, nil
}

// LoadPendingFolders returns all folder rows with traversal_status='Pending' from the given table.
func LoadPendingFolders(database *db.DB, table string) ([]fsservices.Folder, error) {
	query := fmt.Sprintf("SELECT id, parent_id, name, path, parent_path, type, depth_level, size, last_updated FROM %s WHERE type = 'folder' AND traversal_status = 'Pending' ORDER BY depth_level, path", table)
	rows, err := database.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var folders []fsservices.Folder
	for rows.Next() {
		var (
			id, parentID, name, path, parentPath, nodeType string
			depthLevel                                     int
			size                                           sql.NullInt64
			lastUpdated                                    sql.NullString
		)
		if err := rows.Scan(&id, &parentID, &name, &path, &parentPath, &nodeType, &depthLevel, &size, &lastUpdated); err != nil {
			return nil, err
		}
		if nodeType != fsservices.NodeTypeFolder {
			continue
		}

		folder := fsservices.Folder{
			Id:           id,
			ParentId:     parentID,
			ParentPath:   fsservices.NormalizeParentPath(parentPath),
			DisplayName:  name,
			LocationPath: fsservices.NormalizeLocationPath(path),
			DepthLevel:   depthLevel,
			Type:         fsservices.NodeTypeFolder,
		}
		if lastUpdated.Valid {
			folder.LastUpdated = lastUpdated.String
		}
		folders = append(folders, folder)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return folders, nil
}

// LoadExpectedChildren returns the expected folders and files for a destination folder path based on src_nodes.
func LoadExpectedChildren(database *db.DB, parentPath string) ([]fsservices.Folder, []fsservices.File, error) {
	normalizedParent := fsservices.NormalizeLocationPath(parentPath)

	rows, err := database.Query(
		"SELECT id, parent_id, name, path, parent_path, type, depth_level, size, last_updated FROM src_nodes WHERE parent_path IN (?)",
		normalizedParent,
	)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var folders []fsservices.Folder
	var files []fsservices.File

	for rows.Next() {
		var (
			id, parentID, name, path, nodeParentPath, nodeType string
			depthLevel                                         int
			size                                               sql.NullInt64
			lastUpdated                                        sql.NullString
		)
		if err := rows.Scan(&id, &parentID, &name, &path, &nodeParentPath, &nodeType, &depthLevel, &size, &lastUpdated); err != nil {
			return nil, nil, err
		}

		last := ""
		if lastUpdated.Valid {
			last = lastUpdated.String
		}

		switch nodeType {
		case fsservices.NodeTypeFolder:
			folders = append(folders, fsservices.Folder{
				Id:           id,
				ParentId:     parentID,
				ParentPath:   fsservices.NormalizeParentPath(nodeParentPath),
				DisplayName:  name,
				LocationPath: fsservices.NormalizeLocationPath(path),
				LastUpdated:  last,
				DepthLevel:   depthLevel,
				Type:         fsservices.NodeTypeFolder,
			})
		case fsservices.NodeTypeFile:
			file := fsservices.File{
				Id:           id,
				ParentId:     parentID,
				ParentPath:   fsservices.NormalizeParentPath(nodeParentPath),
				DisplayName:  name,
				LocationPath: fsservices.NormalizeLocationPath(path),
				LastUpdated:  last,
				DepthLevel:   depthLevel,
				Type:         fsservices.NodeTypeFile,
			}
			if size.Valid {
				file.Size = size.Int64
			}
			files = append(files, file)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	return folders, files, nil
}
