// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package migration

import (
	"fmt"
	"strings"
	"time"

	"codeberg.org/Sylos/Migration-Engine/pkg/db"
	"codeberg.org/Sylos/Migration-Engine/pkg/db/pull"
	"codeberg.org/Sylos/Migration-Engine/pkg/opsdb"
	"codeberg.org/Sylos/go-path-linter/pkg/gpl"
)

const pathCascadeSampleCap = 50

// PathCascadeAction selects how the SRC folder resolves against destination.
const (
	PathCascadeActionUnique = "unique"
	PathCascadeActionRemap  = "remap"
)

// PathCascadeFolderCollision is one nested folder name collision (not exact-name pair).
type PathCascadeFolderCollision struct {
	SrcID   string `json:"srcId"`
	SrcName string `json:"srcName"`
	SrcPath string `json:"srcPath,omitempty"`
	DstID   string `json:"dstId,omitempty"`
	DstName string `json:"dstName"`
	DstPath string `json:"dstPath,omitempty"`
}

// PathCascadeFileOverwrite is one exact-name file pair with mtime default winner.
type PathCascadeFileOverwrite struct {
	SrcID         string `json:"srcId"`
	SrcName       string `json:"srcName"`
	SrcPath       string `json:"srcPath,omitempty"`
	SrcSize       int64  `json:"srcSize,omitempty"`
	SrcMTime      string `json:"srcMTime,omitempty"`
	DstID         string `json:"dstId"`
	DstName       string `json:"dstName"`
	DstPath       string `json:"dstPath,omitempty"`
	DstSize       int64  `json:"dstSize,omitempty"`
	DstMTime      string `json:"dstMTime,omitempty"`
	DefaultWinner string `json:"defaultWinner"` // "src" | "dst"
}

// PathResolutionCascadePreview is a dry-run of nested unique/remap effects.
type PathResolutionCascadePreview struct {
	Action               string                       `json:"action"`
	SrcFolderID          string                       `json:"srcFolderId"`
	DstFolderID          string                       `json:"dstFolderId,omitempty"`
	FolderCollisionCount int                          `json:"folderCollisionCount"`
	FileOverwriteCount   int                          `json:"fileOverwriteCount"`
	FolderCollisions     []PathCascadeFolderCollision `json:"folderCollisions,omitempty"`
	FileOverwrites       []PathCascadeFileOverwrite   `json:"fileOverwrites,omitempty"`
	DstSubtreeListed     bool                         `json:"dstSubtreeListed"`
	PathChecksEnabled    bool                         `json:"pathChecksEnabled"`
}

// PreviewPathResolutionCascade dry-runs nested folder collisions and file overwrite winners.
// action is unique | remap (required). proposedName is used for unique/remap when non-empty.
func (m *Migration) PreviewPathResolutionCascade(nodeID, action, proposedName string) (PathResolutionCascadePreview, error) {
	out := PathResolutionCascadePreview{
		Action:            action,
		SrcFolderID:       nodeID,
		PathChecksEnabled: m != nil && m.PathChecksEnabled(),
	}
	if m == nil || m.DB == nil {
		return out, fmt.Errorf("migration db not open")
	}
	src, dstID, err := m.resolveCascadeFolders(nodeID, action, proposedName)
	if err != nil {
		return out, err
	}
	if src == nil {
		return out, fmt.Errorf("node %s not found", nodeID)
	}
	out.SrcFolderID = src.ID
	out.DstFolderID = dstID
	if src.Type == db.NodeTypeFile {
		return out, fmt.Errorf("cascade preview is only available for folders")
	}
	if dstID == "" {
		// Unique/remap onto a brand-new destination name: no DST children yet.
		return out, nil
	}
	scan, err := m.scanPathCascade(src.ID, dstID)
	if err != nil {
		return out, err
	}
	out.DstSubtreeListed = scan.dstChildCount > 0
	out.FolderCollisionCount = len(scan.folderCollisions)
	out.FileOverwriteCount = len(scan.fileOverwrites)
	if n := pathCascadeSampleCap; len(scan.folderCollisions) > n {
		out.FolderCollisions = scan.folderCollisions[:n]
	} else {
		out.FolderCollisions = scan.folderCollisions
	}
	if n := pathCascadeSampleCap; len(scan.fileOverwrites) > n {
		out.FileOverwrites = scan.fileOverwrites[:n]
	} else {
		out.FileOverwrites = scan.fileOverwrites
	}
	return out, nil
}

func (m *Migration) resolveCascadeFolders(nodeID, action, proposedName string) (*db.NodeState, string, error) {
	node, err := pull.GetNodeByID(m.DB, "SRC", nodeID)
	if err != nil {
		return nil, "", err
	}
	if node == nil {
		return nil, "", nil
	}
	switch action {
	case PathCascadeActionUnique, PathCascadeActionRemap:
		name := strings.TrimSpace(proposedName)
		if name == "" && action == PathCascadeActionUnique {
			base := preferredUniqueBase(node)
			u, err := m.allocateUniqueSiblingName(nodeID, base, node.Type == db.NodeTypeFile)
			if err != nil {
				return node, "", err
			}
			name = u
		}
		if name == "" {
			return node, "", nil
		}
		dstID, err := m.findDSTSiblingFolderByName(node, name)
		return node, dstID, err
	default:
		return node, "", fmt.Errorf("cascade action must be %q or %q, got %q", PathCascadeActionUnique, PathCascadeActionRemap, action)
	}
}

func (m *Migration) findDSTSiblingFolderByName(node *db.NodeState, wantName string) (string, error) {
	if node == nil || node.ParentID == "" || wantName == "" {
		return "", nil
	}
	mapped, ok, err := m.DB.Ops().GetMapBySrc(node.ParentID)
	if err != nil || !ok || mapped.DstID == "" || mapped.Status != db.IDMapStatusActive {
		return "", err
	}
	ids, err := m.DB.Ops().ListChildren("dst", mapped.DstID, "", 10000)
	if err != nil {
		return "", err
	}
	nodes, err := m.DB.Ops().BatchGetNode("dst", ids)
	if err != nil {
		return "", err
	}
	target := m.dstGPLTarget()
	fold := gpl.CaseInsensitiveSiblingTarget(target) || m.WindowsCompat()
	want := db.NormalizeNodeBasename(wantName)
	for _, id := range ids {
		n := nodes[id]
		if n.Type == db.NodeTypeFile {
			continue
		}
		if gpl.SiblingNamesEqual(db.NormalizeNodeBasename(n.Name), want, fold) {
			return id, nil
		}
	}
	return "", nil
}

type dstCascadeEntry struct {
	id   string
	name string
	rec  opsdb.NodeRecord
}

type cascadeScan struct {
	dstChildCount    int
	folderCollisions []PathCascadeFolderCollision
	fileOverwrites   []PathCascadeFileOverwrite
}

func (m *Migration) scanPathCascade(srcFolderID, dstFolderID string) (cascadeScan, error) {
	var out cascadeScan
	type frame struct {
		srcParent, dstParent string
	}
	queue := []frame{{srcParent: srcFolderID, dstParent: dstFolderID}}
	target := m.dstGPLTarget()
	fold := gpl.CaseInsensitiveSiblingTarget(target) || m.WindowsCompat()

	for len(queue) > 0 {
		fr := queue[0]
		queue = queue[1:]

		srcKids, err := pull.GetChildrenByParentID(m.DB, "SRC", fr.srcParent, 100_000)
		if err != nil {
			return out, err
		}
		dstIDs, err := m.DB.Ops().ListChildren(opsdb.SideDST, fr.dstParent, "", 100_000)
		if err != nil {
			return out, err
		}
		dstNodes, err := m.DB.Ops().BatchGetNode(opsdb.SideDST, dstIDs)
		if err != nil {
			return out, err
		}
		out.dstChildCount += len(dstIDs)

		exactFolder := map[string]dstCascadeEntry{}
		exactFile := map[string]dstCascadeEntry{}
		foldFolder := map[string]dstCascadeEntry{}
		foldFile := map[string]dstCascadeEntry{}
		for _, id := range dstIDs {
			n := dstNodes[id]
			name := db.NormalizeNodeBasename(n.Name)
			e := dstCascadeEntry{id: id, name: name, rec: n}
			key := name
			fkey := siblingFoldKey(name, fold)
			if db.NormalizeQueueNodeType(n.Type) == db.NodeTypeFile {
				exactFile[key] = e
				if _, ok := foldFile[fkey]; !ok {
					foldFile[fkey] = e
				}
			} else {
				exactFolder[key] = e
				if _, ok := foldFolder[fkey]; !ok {
					foldFolder[fkey] = e
				}
			}
		}

		for _, sk := range srcKids {
			if sk == nil {
				continue
			}
			rawName := db.NormalizeNodeBasename(sk.Name)
			matchName := preferredUniqueBase(sk)
			if db.NormalizeQueueNodeType(sk.Type) == db.NodeTypeFile {
				m.appendCascadeFileHit(&out, sk, rawName, matchName, exactFile, foldFile, fold)
				continue
			}
			if d, ok := lookupCascadeDST(exactFolder, foldFolder, rawName, matchName, fold); ok {
				// Recurse into same-name peer folders so nested file overwrites surface.
				queue = append(queue, frame{srcParent: sk.ID, dstParent: d.id})
			}
		}
	}
	return out, nil
}

func lookupCascadeDST(
	exact, foldMap map[string]dstCascadeEntry,
	rawName, matchName string,
	fold bool,
) (dstCascadeEntry, bool) {
	if d, ok := exact[rawName]; ok {
		return d, true
	}
	if matchName != "" && matchName != rawName {
		if d, ok := exact[matchName]; ok {
			return d, true
		}
	}
	if !fold {
		return dstCascadeEntry{}, false
	}
	if d, ok := foldMap[siblingFoldKey(rawName, true)]; ok {
		return d, true
	}
	if matchName != "" && matchName != rawName {
		if d, ok := foldMap[siblingFoldKey(matchName, true)]; ok {
			return d, true
		}
	}
	return dstCascadeEntry{}, false
}

func (m *Migration) appendCascadeFileHit(
	out *cascadeScan,
	sk *db.NodeState,
	rawName, matchName string,
	exactFile, foldFile map[string]dstCascadeEntry,
	fold bool,
) {
	d, ok := lookupCascadeDST(exactFile, foldFile, rawName, matchName, fold)
	if !ok {
		return
	}
	out.fileOverwrites = append(out.fileOverwrites, PathCascadeFileOverwrite{
		SrcID: sk.ID, SrcName: rawName, SrcPath: sk.DisplayPath,
		SrcSize: sk.Size, SrcMTime: sk.MTime,
		DstID: d.id, DstName: d.name, DstPath: d.rec.DisplayPath,
		DstSize: d.rec.Size, DstMTime: d.rec.MTime,
		DefaultWinner: m.cascadeOverwriteWinner(sk.MTime, d.rec.MTime, sk.Size, d.rec.Size),
	})
}

func (m *Migration) cascadeOverwriteWinner(srcMTime, dstMTime string, srcSize, dstSize int64) string {
	switch m.OverwritePolicy() {
	case OverwriteDecide, OverwriteSourceWins:
		return "src"
	default:
		return defaultOverwriteWinner(srcMTime, dstMTime, srcSize, dstSize)
	}
}

func siblingFoldKey(name string, fold bool) string {
	if fold {
		return strings.ToLower(name)
	}
	return name
}

// defaultOverwriteWinner mirrors queue/worker compareTimestamps: DST newer/equal → dst;
// SRC newer → src; unparseable with same positive size → dst; else src (copy pending).
func defaultOverwriteWinner(srcMTime, dstMTime string, srcSize, dstSize int64) string {
	switch compareCascadeMTimes(srcMTime, dstMTime) {
	case "Successful":
		return "dst"
	case "Unparseable":
		if srcSize > 0 && srcSize == dstSize {
			return "dst"
		}
		return "src"
	default:
		return "src"
	}
}

func compareCascadeMTimes(srcMTime, dstMTime string) string {
	srcTime, srcOK := parseCascadeMTime(srcMTime)
	dstTime, dstOK := parseCascadeMTime(dstMTime)
	if !srcOK || !dstOK {
		return "Unparseable"
	}
	if !dstTime.Before(srcTime) {
		return "Successful"
	}
	return "Pending"
}

func parseCascadeMTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	if t, err := time.Parse("2006-01-02T15:04:05.999999999", s); err == nil {
		return t.UTC(), true
	}
	if t, err := time.Parse("2006-01-02T15:04:05", s); err == nil {
		return t.UTC(), true
	}
	return time.Time{}, false
}
