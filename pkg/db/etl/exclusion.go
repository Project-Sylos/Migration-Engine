// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package etl

import (
	"database/sql"
	"fmt"
	"strings"
)

// normalizePathForPrefix ensures the path doesn't end with '/' for proper prefix matching
func normalizePathForPrefix(path string) string {
	return strings.TrimSuffix(path, "/")
}

// ExclusionResult represents the result of an exclusion operation
type ExclusionResult struct {
	NodesAffected int64
	TableName     string
	Path          string
	Depth         int
}

// SetNodeExclusion marks a node as excluded or unexcluded and propagates the change to all descendants.
// This uses DuckDB's set-based SQL operations instead of BFS sweeps.
// exclude: true to exclude (mark as excluded_explicit/inherited), false to unexclude (mark as pending/successful)
// Returns the number of nodes affected (including the node itself and all descendants).
func SetNodeExclusion(duckDB *sql.DB, tableName string, nodePath string, nodeDepth int, exclude bool) (*ExclusionResult, error) {
	var updateExplicit, updateDescendants string
	var operation string

	if exclude {
		operation = "exclusion"
		// Mark node as explicitly excluded
		updateExplicit = fmt.Sprintf(`
			UPDATE %s
			SET traversal_status = 'excluded_explicit'
			WHERE path = ?
		`, tableName)
		// Mark descendants as inherited excluded
		updateDescendants = fmt.Sprintf(`
			UPDATE %s
			SET traversal_status = 'excluded_inherited'
			WHERE
				depth > ?
				AND (
					path = ?
					OR starts_with(path, ? || '/')
				)
				AND traversal_status != 'excluded_explicit'
		`, tableName)
	} else {
		operation = "unexclusion"
		// Mark node as not excluded (pending or successful based on copy status)
		updateExplicit = fmt.Sprintf(`
			UPDATE %s
			SET traversal_status = CASE
				WHEN copy_status = 'completed' THEN 'successful'
				ELSE 'pending'
			END
			WHERE path = ?
			AND traversal_status IN ('excluded_explicit', 'excluded_inherited')
		`, tableName)
		// Mark descendants as not excluded
		updateDescendants = fmt.Sprintf(`
			UPDATE %s
			SET traversal_status = CASE
				WHEN copy_status = 'completed' THEN 'successful'
				ELSE 'pending'
			END
			WHERE
				depth > ?
				AND (
					path = ?
					OR starts_with(path, ? || '/')
				)
				AND traversal_status = 'excluded_inherited'
		`, tableName)
	}

	// Step 1: Update the node itself
	result, err := duckDB.Exec(updateExplicit, nodePath)
	if err != nil {
		return nil, fmt.Errorf("failed to apply %s to node: %w", operation, err)
	}

	explicitRows, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get rows affected for explicit %s: %w", operation, err)
	}

	// Step 2: Update all descendants using a single SQL update
	// This replaces the BFS sweep with a single set-based operation
	normalizedPath := normalizePathForPrefix(nodePath)
	result, err = duckDB.Exec(updateDescendants, nodeDepth, nodePath, normalizedPath)
	if err != nil {
		return nil, fmt.Errorf("failed to propagate %s to descendants: %w", operation, err)
	}

	descendantRows, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get rows affected for descendant %s: %w", operation, err)
	}

	totalAffected := explicitRows + descendantRows

	return &ExclusionResult{
		NodesAffected: totalAffected,
		TableName:     tableName,
		Path:          nodePath,
		Depth:         nodeDepth,
	}, nil
}

// ExcludeNode marks a node as explicitly excluded and propagates exclusion to all descendants.
// This is a convenience wrapper around SetNodeExclusion(duckDB, tableName, nodePath, nodeDepth, true).
func ExcludeNode(duckDB *sql.DB, tableName string, nodePath string, nodeDepth int) (*ExclusionResult, error) {
	return SetNodeExclusion(duckDB, tableName, nodePath, nodeDepth, true)
}

// UnexcludeNode removes exclusion from a node and all its descendants.
// This is a convenience wrapper around SetNodeExclusion(duckDB, tableName, nodePath, nodeDepth, false).
func UnexcludeNode(duckDB *sql.DB, tableName string, nodePath string, nodeDepth int) (*ExclusionResult, error) {
	return SetNodeExclusion(duckDB, tableName, nodePath, nodeDepth, false)
}

// GetExclusionStatus returns the exclusion status of a node and its subtree.
// Returns counts of explicitly excluded, inherited excluded, and total nodes in the subtree.
type ExclusionStatus struct {
	ExplicitlyExcluded int64
	InheritedExcluded  int64
	TotalNodes         int64
	Path               string
}

func GetExclusionStatus(duckDB *sql.DB, tableName string, nodePath string) (*ExclusionStatus, error) {
	// Count nodes in the subtree (the node itself and all descendants)
	query := fmt.Sprintf(`
		SELECT
			COUNT(*) FILTER (WHERE traversal_status = 'excluded_explicit') as explicit_count,
			COUNT(*) FILTER (WHERE traversal_status = 'excluded_inherited') as inherited_count,
			COUNT(*) as total_count
		FROM %s
		WHERE
			path = ?
			OR starts_with(path, ? || '/')
	`, tableName)

	// Normalize path for prefix matching
	normalizedPath := normalizePathForPrefix(nodePath)
	var status ExclusionStatus
	err := duckDB.QueryRow(query, nodePath, normalizedPath).Scan(
		&status.ExplicitlyExcluded,
		&status.InheritedExcluded,
		&status.TotalNodes,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get exclusion status: %w", err)
	}

	status.Path = nodePath
	return &status, nil
}
