// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"encoding/json"
	"fmt"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
)

const remapPageSize = 500

// NormalizeLocalRootPath canonicalizes an absolute local root for prefix remaps.
// Uses forward slashes and strips a trailing slash (except drive roots like "C:/").
func NormalizeLocalRootPath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return ""
	}
	p = strings.ReplaceAll(p, "\\", "/")
	for strings.Contains(p, "//") {
		p = strings.ReplaceAll(p, "//", "/")
	}
	if len(p) > 1 && strings.HasSuffix(p, "/") {
		// Keep "C:/" style roots; strip trailing slash otherwise.
		if !(len(p) == 3 && p[1] == ':') {
			p = strings.TrimSuffix(p, "/")
		}
	}
	return p
}

// RemapPathPrefix rewrites absPath when it equals oldRoot or is under oldRoot/.
// Returns rewritten path and whether a change was made.
func RemapPathPrefix(absPath, oldRoot, newRoot string) (string, bool) {
	oldRoot = NormalizeLocalRootPath(oldRoot)
	newRoot = NormalizeLocalRootPath(newRoot)
	absPath = strings.ReplaceAll(strings.TrimSpace(absPath), "\\", "/")
	if oldRoot == "" || newRoot == "" || absPath == "" || oldRoot == newRoot {
		return absPath, false
	}
	if absPath == oldRoot {
		return newRoot, true
	}
	prefix := oldRoot + "/"
	if strings.HasPrefix(absPath, prefix) {
		return newRoot + absPath[len(oldRoot):], true
	}
	return absPath, false
}

func remapIncludeOnlyJSON(raw, oldRoot, newRoot string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" {
		return raw, false
	}
	var ids []string
	if err := json.Unmarshal([]byte(raw), &ids); err != nil || len(ids) == 0 {
		return raw, false
	}
	changed := false
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		next, ok := RemapPathPrefix(id, oldRoot, newRoot)
		if ok {
			changed = true
		}
		out = append(out, next)
	}
	if !changed {
		return raw, false
	}
	b, err := json.Marshal(out)
	if err != nil {
		return raw, false
	}
	return string(b), true
}

// RemapLocalServiceIDs rewrites absolute LocalFS ServiceIDs under oldRoot to newRoot
// for one catalog side (src or dst). Updates nodes, kids packs, IncludeOnly JSON,
// and when side is dst also SRC status XferDstRef values under the old DST root.
// Path / DisplayPath / idx:* / map:* are left unchanged.
func (s *Store) RemapLocalServiceIDs(side, oldRoot, newRoot string) (int, error) {
	if s == nil {
		return 0, fmt.Errorf("opsdb RemapLocalServiceIDs: nil store")
	}
	side = kidsReplaceSide(side)
	oldRoot = NormalizeLocalRootPath(oldRoot)
	newRoot = NormalizeLocalRootPath(newRoot)
	if oldRoot == "" || newRoot == "" {
		return 0, fmt.Errorf("opsdb RemapLocalServiceIDs: empty root")
	}
	if oldRoot == newRoot {
		return 0, nil
	}

	rewritten := 0
	after := ""
	for {
		ids, err := s.ListNodeIDs(side, after, remapPageSize)
		if err != nil {
			return rewritten, err
		}
		if len(ids) == 0 {
			break
		}
		nodes, err := s.BatchGetNode(side, ids)
		if err != nil {
			return rewritten, err
		}
		sts, err := s.BatchGetStatus(side, ids)
		if err != nil {
			return rewritten, err
		}
		parentIDs := make([]string, 0, len(ids))
		for _, id := range ids {
			parentIDs = append(parentIDs, id)
		}
		kidsByParent, err := s.BatchGetKids(side, parentIDs)
		if err != nil {
			return rewritten, err
		}

		for _, id := range ids {
			n, ok := nodes[id]
			if !ok {
				continue
			}
			nChanged := false
			if next, ok := RemapPathPrefix(n.ServiceID, oldRoot, newRoot); ok {
				n.ServiceID = next
				nChanged = true
			}
			if next, ok := RemapPathPrefix(n.ParentServiceID, oldRoot, newRoot); ok {
				n.ParentServiceID = next
				nChanged = true
			}
			if next, ok := remapIncludeOnlyJSON(n.IncludeOnly, oldRoot, newRoot); ok {
				n.IncludeOnly = next
				nChanged = true
			}
			if nChanged {
				if err := s.PutNode(side, n); err != nil {
					return rewritten, err
				}
				rewritten++
			}

			if st, sok := sts[id]; sok {
				stChanged := false
				if next, ok := remapIncludeOnlyJSON(st.IncludeOnly, oldRoot, newRoot); ok {
					st.IncludeOnly = next
					stChanged = true
				}
				if stChanged {
					if err := s.PutStatus(side, id, st); err != nil {
						return rewritten, err
					}
				}
			}

			kids, kok := kidsByParent[id]
			if !kok || len(kids) == 0 {
				continue
			}
			kidsChanged := false
			for i := range kids {
				if next, ok := RemapPathPrefix(kids[i].ServiceID, oldRoot, newRoot); ok {
					kids[i].ServiceID = next
					kidsChanged = true
				}
				if next, ok := RemapPathPrefix(kids[i].ParentServiceID, oldRoot, newRoot); ok {
					kids[i].ParentServiceID = next
					kidsChanged = true
				}
			}
			if kidsChanged {
				if _, err := s.WriteSealBatchTrusted(nil, nil, []SealKidsReplace{{
					Side:     side,
					ParentID: id,
					Kids:     kids,
				}}, nil, nil); err != nil {
					return rewritten, err
				}
			}
		}

		after = ids[len(ids)-1]
		if len(ids) < remapPageSize {
			break
		}
	}

	if side == SideDST {
		n, err := s.remapSRCXferDstRefs(oldRoot, newRoot)
		if err != nil {
			return rewritten, err
		}
		rewritten += n
	}
	return rewritten, nil
}

func (s *Store) remapSRCXferDstRefs(oldRoot, newRoot string) (int, error) {
	rewritten := 0
	after := ""
	for {
		ids, err := s.ListNodeIDs(SideSRC, after, remapPageSize)
		if err != nil {
			return rewritten, err
		}
		if len(ids) == 0 {
			break
		}
		sts, err := s.BatchGetStatus(SideSRC, ids)
		if err != nil {
			return rewritten, err
		}
		for _, id := range ids {
			st, ok := sts[id]
			if !ok || st.XferDstRef == "" {
				continue
			}
			next, changed := RemapPathPrefix(st.XferDstRef, oldRoot, newRoot)
			if !changed {
				continue
			}
			st.XferDstRef = next
			if err := s.PutStatus(SideSRC, id, st); err != nil {
				return rewritten, err
			}
			rewritten++
		}
		after = ids[len(ids)-1]
		if len(ids) < remapPageSize {
			break
		}
	}
	return rewritten, nil
}

// ListOAuthConnectionIDs returns connection ids for all oauth: keys.
func (s *Store) ListOAuthConnectionIDs() ([]string, error) {
	if s == nil {
		return nil, nil
	}
	prefix := []byte("oauth:")
	var ids []string
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			id := string(key[len(prefix):])
			if id != "" {
				ids = append(ids, id)
			}
		}
		return nil
	})
	return ids, err
}

// DeleteAllOAuthCreds removes every oauth: credential payload.
func (s *Store) DeleteAllOAuthCreds() (int, error) {
	ids, err := s.ListOAuthConnectionIDs()
	if err != nil {
		return 0, err
	}
	for _, id := range ids {
		if err := s.DeleteOAuthCred(id); err != nil {
			return 0, err
		}
	}
	return len(ids), nil
}
