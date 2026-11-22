// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package opsdb

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Path-segment and trigram posting keys. Overview: docs/search_indexes.md.
const (
	indexPathPrefix  = "idx:path:"
	indexNamePrefix  = "idx:name:"
	indexSizePrefix  = "idx:size:"
	indexMTimePrefix = "idx:mtime:"
	indexSegPrefix   = "idx:seg:"
	indexTriPrefix   = "idx:tri:"
	indexNul         = '\x00'
)

// NormalizeIndexPath returns the canonical path used in idx:path keys (leading /,
// no trailing slash except root "/"). Callers append "/" for the stored key.
func NormalizeIndexPath(p string) string {
	if p == "" {
		return "/"
	}
	for strings.Contains(p, "//") {
		p = strings.ReplaceAll(p, "//", "/")
	}
	if p != "/" && !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	if p != "/" && strings.HasSuffix(p, "/") {
		p = strings.TrimRight(p, "/")
	}
	return p
}

// PathIndexKey is idx:path:{side}:{path}/ (exact node lives under the trailing slash).
func PathIndexKey(side, p string) []byte {
	return fmt.Appendf(nil, "%s%s:%s/", indexPathPrefix, side, NormalizeIndexPath(p))
}

// PathIndexPrefix is the seek prefix for subtree scans under root (includes root itself).
func PathIndexPrefix(side, root string) []byte {
	if NormalizeIndexPath(root) == "/" {
		return []byte(indexPathPrefix + side + ":/")
	}
	return PathIndexKey(side, root)
}

// indexKey is prefix+side+":"+parts joined by NUL. Dropping the trailing part yields the seek prefix.
func indexKey(prefix, side string, parts ...string) []byte {
	b := append([]byte(prefix+side), ':')
	for i, p := range parts {
		if i > 0 {
			b = append(b, indexNul)
		}
		b = append(b, p...)
	}
	return b
}

// ExtractTrigrams returns unique lowercase trigrams for s (edge-padded when short).
func ExtractTrigrams(s string) []string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return nil
	}
	runes := []rune(s)
	for len(runes) < 3 {
		runes = append([]rune{' '}, runes...)
		if len(runes) < 3 {
			runes = append(runes, ' ')
		}
	}
	seen := make(map[string]struct{}, len(runes))
	out := make([]string, 0, len(runes)-2)
	for i := 0; i+3 <= len(runes); i++ {
		g := string(runes[i : i+3])
		if _, ok := seen[g]; ok {
			continue
		}
		seen[g] = struct{}{}
		out = append(out, g)
	}
	return out
}

// stripOverlappingExtensionForTrigrams removes "."+ext from the basename of s
// only when that basename ends with the extension (case-insensitive). Used only
// by trigram index/query paths; Name/DisplayName/segment indexes stay full.
func stripOverlappingExtensionForTrigrams(s, ext string) string {
	ext = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(ext), "."))
	if ext == "" {
		return s
	}
	s = strings.TrimSpace(s)
	base := s
	prefix := ""
	if i := strings.LastIndexByte(base, '/'); i >= 0 {
		prefix = base[:i+1]
		base = base[i+1:]
	}
	suffix := "." + ext
	if len(base) <= len(suffix) || !strings.EqualFold(base[len(base)-len(suffix):], suffix) {
		return s
	}
	return prefix + base[:len(base)-len(suffix)]
}

// TrigramSourceText prepares a query needle for trigram intersect by stripping a
// trailing extension only when the needle ends with that extension. Indexing
// uses NodeTrigrams with the stored file Extension instead.
func TrigramSourceText(needle string) string {
	needle = strings.TrimSpace(needle)
	if needle == "" {
		return needle
	}
	base := needle
	if i := strings.LastIndexByte(base, '/'); i >= 0 {
		base = base[i+1:]
	}
	i := strings.LastIndexByte(base, '.')
	if i <= 0 || i == len(base)-1 {
		return needle
	}
	return stripOverlappingExtensionForTrigrams(needle, base[i+1:])
}

// NodeTrigrams returns unique trigrams for name plus each path segment (not across '/').
// Uses displayPath when non-empty, else path. For files, strips Extension from
// each token only when that token ends with "."+Extension (folders never strip).
func NodeTrigrams(path, displayPath, name, nodeType, extension string) []string {
	seen := map[string]struct{}{}
	var out []string
	stripExt := ""
	if nodeType == NodeTypeFile {
		stripExt = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(extension), "."))
	}
	add := func(s string) {
		if stripExt != "" {
			s = stripOverlappingExtensionForTrigrams(s, stripExt)
		}
		for _, g := range ExtractTrigrams(s) {
			if _, ok := seen[g]; ok {
				continue
			}
			seen[g] = struct{}{}
			out = append(out, g)
		}
	}
	if name != "" {
		add(name)
	}
	p := strings.TrimSpace(displayPath)
	if p == "" {
		p = path
	}
	p = NormalizeIndexPath(p)
	if p != "/" {
		for _, part := range strings.Split(strings.Trim(p, "/"), "/") {
			add(part)
		}
	}
	return out
}

func u64BE(n int64) string {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return string(b)
}

// ParseMTimeUnixNano parses NodeRecord.MTime into unix nanos for indexing.
// Empty or unparseable values map to 0.
func ParseMTimeUnixNano(mtime string) int64 {
	mtime = strings.TrimSpace(mtime)
	if mtime == "" {
		return 0
	}
	if n, err := strconv.ParseInt(mtime, 10, 64); err == nil {
		if n > 1e15 {
			return n
		}
		if n > 1e12 {
			return n * 1e3
		}
		return n * 1e9
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, mtime); err == nil {
			return t.UnixNano()
		}
	}
	return 0
}

// SizeBucketKey is a coarse cardinality counter for size log2 buckets.
func SizeBucketKey(side string, size int64) string {
	return fmt.Sprintf("idx:size:%s:%d", side, sizeLog2Bucket(size))
}

// MTimeBucketKey is a coarse cardinality counter for mtime month buckets.
func MTimeBucketKey(side string, unixNano int64) string {
	return fmt.Sprintf("idx:mtime:%s:%d", side, mtimeMonthBucket(unixNano))
}

func sizeLog2Bucket(size int64) int {
	if size <= 0 {
		return 0
	}
	b := 0
	for size > 1 {
		size >>= 1
		b++
	}
	return b
}

func mtimeMonthBucket(unixNano int64) int {
	if unixNano <= 0 {
		return 0
	}
	t := time.Unix(0, unixNano).UTC()
	return t.Year()*12 + int(t.Month())
}

// PathSegments returns lowercased basename tokens for idx:seg (path parts + name).
func PathSegments(p, name string) []string {
	p = NormalizeIndexPath(p)
	seen := map[string]struct{}{}
	var out []string
	add := func(tok string) {
		tok = strings.ToLower(strings.TrimSpace(tok))
		if tok == "" || tok == "/" {
			return
		}
		if _, ok := seen[tok]; ok {
			return
		}
		seen[tok] = struct{}{}
		out = append(out, tok)
	}
	if name != "" {
		add(name)
	}
	if p == "/" {
		return out
	}
	for _, part := range strings.Split(strings.Trim(p, "/"), "/") {
		add(part)
	}
	return out
}

func parseIndexID(key []byte) (id string, ok bool) {
	i := len(key) - 1
	for i >= 0 && key[i] != indexNul {
		i--
	}
	if i < 0 || i+1 >= len(key) {
		return "", false
	}
	return string(key[i+1:]), true
}
