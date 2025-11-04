// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package queue

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Project-Sylos/Migration-Engine/internal/fsservices"
)

// RoundData stores children nodes for a specific round, indexed by parent path.
type RoundData struct {
	mu       sync.RWMutex
	children map[string][]any // parentPath -> []children (mixed Folder/File)
}

// OutputBuffer provides an in-memory visibility layer between src and dst queues.
// It temporarily stores newly discovered children from src workers until dst queries them.
type OutputBuffer struct {
	mu     sync.RWMutex
	rounds map[int]*RoundData // round -> children data
}

// NewOutputBuffer creates a new output buffer.
func NewOutputBuffer() *OutputBuffer {
	return &OutputBuffer{
		rounds: make(map[int]*RoundData),
	}
}

// getOrCreateRoundData returns the RoundData for the specified round, creating it if needed.
func (ob *OutputBuffer) getOrCreateRoundData(round int) *RoundData {
	ob.mu.RLock()
	roundData, exists := ob.rounds[round]
	ob.mu.RUnlock()

	if exists {
		return roundData
	}

	// Create new round data
	ob.mu.Lock()
	defer ob.mu.Unlock()

	// Double-check after acquiring write lock
	if roundData, exists := ob.rounds[round]; exists {
		return roundData
	}

	roundData = &RoundData{
		children: make(map[string][]any),
	}
	ob.rounds[round] = roundData
	return roundData
}

// Add adds a child node to the buffer for the specified round and parent path.
func (ob *OutputBuffer) Add(round int, parentPath string, child fsservices.Node) {
	roundData := ob.getOrCreateRoundData(round)
	roundData.mu.Lock()
	defer roundData.mu.Unlock()

	roundData.children[parentPath] = append(roundData.children[parentPath], child)
}

// GetChildrenForPaths returns all children for the specified round and parent paths.
// Returns a map of parentPath -> children slice.
// Also removes the retrieved paths from the buffer to prevent re-reading.
func (ob *OutputBuffer) GetChildrenForPaths(round int, paths []string) map[string][]any {
	ob.mu.RLock()
	roundData, exists := ob.rounds[round]
	ob.mu.RUnlock()

	if !exists {
		return make(map[string][]any)
	}

	// First, read the data while holding read lock
	roundData.mu.RLock()
	result := make(map[string][]any)
	for _, path := range paths {
		if children, found := roundData.children[path]; found {
			result[path] = children
		} else {
			result[path] = make([]any, 0)
		}
	}
	roundData.mu.RUnlock()

	// Then, delete the retrieved paths while holding write lock
	if len(result) > 0 {
		roundData.mu.Lock()
		for path := range result {
			delete(roundData.children, path)
		}
		roundData.mu.Unlock()
	}

	return result
}

// HasRound returns true if the buffer contains data for the specified round.
func (ob *OutputBuffer) HasRound(round int) bool {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	_, exists := ob.rounds[round]
	return exists
}

// DebugPrint prints the entire buffer contents as JSON for debugging.
func (ob *OutputBuffer) DebugPrint() {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	if len(ob.rounds) == 0 {
		fmt.Println("[OutputBuffer] Empty")
		return
	}

	// Build a printable structure
	printable := make(map[string]any)
	for round, roundData := range ob.rounds {
		roundData.mu.RLock()
		roundKey := fmt.Sprintf("round_%d", round)
		roundMap := make(map[string]any)

		for path, children := range roundData.children {
			roundMap[path] = len(children)
		}
		roundMap["_total_paths"] = len(roundData.children)
		roundMap["_total_children"] = ob.countChildren(roundData.children)

		printable[roundKey] = roundMap
		roundData.mu.RUnlock()
	}

	jsonBytes, err := json.MarshalIndent(printable, "", "  ")
	if err != nil {
		fmt.Printf("[OutputBuffer] Failed to marshal: %v\n", err)
		return
	}
	fmt.Println("[OutputBuffer]")
	fmt.Println(string(jsonBytes))
}

// DebugPrintRound prints only the specified round for debugging.
func (ob *OutputBuffer) DebugPrintRound(round int) {
	ob.mu.RLock()
	roundData, exists := ob.rounds[round]
	ob.mu.RUnlock()

	if !exists {
		fmt.Printf("[OutputBuffer] Round %d does not exist\n", round)
		return
	}

	roundData.mu.RLock()
	defer roundData.mu.RUnlock()

	// Build printable structure for this round
	printable := make(map[string]any)
	for path, children := range roundData.children {
		printable[path] = len(children)
	}
	printable["_total_paths"] = len(roundData.children)
	printable["_total_children"] = ob.countChildren(roundData.children)

	jsonBytes, err := json.MarshalIndent(printable, "", "  ")
	if err != nil {
		fmt.Printf("[OutputBuffer] Failed to marshal: %v\n", err)
		return
	}
	fmt.Printf("[OutputBuffer] Round %d:\n", round)
	fmt.Println(string(jsonBytes))
}

// countChildren counts total number of children across all paths.
func (ob *OutputBuffer) countChildren(childrenByPath map[string][]any) int {
	total := 0
	for _, children := range childrenByPath {
		total += len(children)
	}
	return total
}
