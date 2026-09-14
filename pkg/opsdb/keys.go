// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	SideSRC = "src"
	SideDST = "dst"
)

// Phase prefixes for pending frontiers.
const (
	PhaseTrav = "trav"
	PhaseCopy = "copy"
	PhaseDel  = "del"
)

// Node types stored in pend:/schedcnt: keys (homogeneous per prefix).
const (
	NodeTypeFolder = "folder"
	NodeTypeFile   = "file"
)

func pendingNodeType(nodeType string) string {
	if nodeType == NodeTypeFile {
		return NodeTypeFile
	}
	return NodeTypeFolder
}

func nodeKey(side, id string) []byte {
	return []byte("node:" + side + ":" + id)
}

func nodePrefix(side string) []byte {
	return []byte("node:" + side + ":")
}

func childKey(side, parentID, id string) []byte {
	return []byte("child:" + side + ":" + parentID + ":" + id)
}

func childPrefix(side, parentID string) []byte {
	return []byte("child:" + side + ":" + parentID + ":")
}

func kidsKey(side, parentID string) []byte {
	return []byte("kids:" + side + ":" + parentID)
}

func statusKey(side, id string) []byte {
	return []byte("st:" + side + ":" + id)
}

func statusPrefix(side string) []byte {
	return []byte("st:" + side + ":")
}

func statusTravKey(side, id string) []byte {
	return []byte("st:trav:" + side + ":" + id)
}

func statusCopyKey(side, id string) []byte {
	return []byte("st:copy:" + side + ":" + id)
}

func statusDelKey(side, id string) []byte {
	return []byte("st:del:" + side + ":" + id)
}

func statusPhasePrefix(side, phase string) []byte {
	return []byte("st:" + phase + ":" + side + ":")
}

func parseStatusKey(key []byte) (id string, ok bool) {
	s := string(key)
	if !strings.HasPrefix(s, "st:") {
		return "", false
	}
	rest := s[len("st:"):]
	i := strings.IndexByte(rest, ':')
	if i < 0 || i+1 >= len(rest) {
		return "", false
	}
	id = rest[i+1:]
	return id, id != ""
}

func parseStatusPhaseKey(key []byte) (side, phase, id string, ok bool) {
	s := string(key)
	if !strings.HasPrefix(s, "st:") {
		return "", "", "", false
	}
	rest := s[len("st:"):]
	parts := strings.SplitN(rest, ":", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", false
	}
	return parts[1], parts[0], parts[2], true
}

func mapSrcKey(srcID string) []byte {
	return []byte("map:src:" + srcID)
}

func mapDstKey(dstID string) []byte {
	return []byte("map:dst:" + dstID)
}

func pendingKey(side, phase string, depth int, nodeType, id string) []byte {
	return fmt.Appendf(nil, "pend:%s:%s:%d:%s:%s", side, phase, depth, pendingNodeType(nodeType), id)
}

func pendingPrefix(side, phase string, depth int, nodeType string) []byte {
	return fmt.Appendf(nil, "pend:%s:%s:%d:%s:", side, phase, depth, pendingNodeType(nodeType))
}

func schedCountKey(side, phase string, depth int, nodeType string) []byte {
	return fmt.Appendf(nil, "schedcnt:%s:%s:%d:%s", side, phase, depth, pendingNodeType(nodeType))
}

func statKey(key string) []byte {
	return []byte("stat:review:" + key)
}

func depthStatKey(side string, depth int, key string) []byte {
	return fmt.Appendf(nil, "stat:depth:%s:%d:%s", side, depth, key)
}

func depthTotKey(side, key string) []byte {
	return fmt.Appendf(nil, "stat:dtot:%s:%s", side, key)
}

func depthMaxKey(side string) []byte {
	return []byte("stat:dmax:" + side)
}

func foldSealedDepthKey(side string) []byte {
	return []byte("stat:fold:" + side + ":sealed_depth")
}

func foldTicketKey(side string, depth int, id string) []byte {
	return fmt.Appendf(nil, "fold:ticket:%s:%d:%s", side, depth, id)
}

func foldTicketPrefix(side string, depth int) []byte {
	return fmt.Appendf(nil, "fold:ticket:%s:%d:", side, depth)
}

func foldNextKey(side, id string) []byte {
	return []byte("fold:next:" + side + ":" + id)
}

func foldNextPrefix(side string) []byte {
	return []byte("fold:next:" + side + ":")
}

func logKey(unixNano int64, id string) []byte {
	return fmt.Appendf(nil, "log:%020d:%s", unixNano, id)
}

func logIDKey(id string) []byte {
	return []byte("logid:" + id)
}

func opKey(unixNano int64, seq uint64) []byte {
	return fmt.Appendf(nil, "op:%020d:%09d", unixNano, seq)
}

func qstatKey(queueKey, phase string, unixNano int64) []byte {
	return fmt.Appendf(nil, "qstat:%s:%s:%020d", queueKey, phase, unixNano)
}

func qstatLatestKey(queueKey, phase string) []byte {
	return []byte("qstat:latest:" + queueKey + ":" + phase)
}

func taskErrKey(unixNano int64, id string) []byte {
	return fmt.Appendf(nil, "taskerr:%020d:%s", unixNano, id)
}

func parsePendingKey(key []byte) (side, phase string, depth int, nodeType, id string, ok bool) {
	s := string(key)
	if !strings.HasPrefix(s, "pend:") {
		return "", "", 0, "", "", false
	}
	parts := strings.SplitN(s, ":", 6)
	if len(parts) != 6 {
		return "", "", 0, "", "", false
	}
	d, err := strconv.Atoi(parts[3])
	if err != nil {
		return "", "", 0, "", "", false
	}
	return parts[1], parts[2], d, parts[4], parts[5], true
}

func parseChildKey(key []byte) (childID string, ok bool) {
	s := string(key)
	const p = "child:"
	if !strings.HasPrefix(s, p) {
		return "", false
	}
	rest := s[len(p):]
	parts := strings.SplitN(rest, ":", 3)
	if len(parts) != 3 {
		return "", false
	}
	return parts[2], true
}
