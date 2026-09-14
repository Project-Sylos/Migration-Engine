// Copyright 2025 Sylos contributors
// SPDX-License-Identifier: LGPL-2.1-or-later

package filter

import "strings"

// Category names for the static extension lookup.
const (
	CategoryDocument    = "document"
	CategorySpreadsheet = "spreadsheet"
	CategoryPresentation = "presentation"
	CategoryImage       = "image"
	CategoryVideo       = "video"
	CategoryAudio       = "audio"
	CategoryArchive     = "archive"
	CategoryCode        = "code"
	CategoryPDF         = "pdf"
	CategoryOther       = "other"
)

// extensionCategory is extension (no leading dot, lower) → category.
var extensionCategory = map[string]string{
	"doc": CategoryDocument, "docx": CategoryDocument, "odt": CategoryDocument,
	"rtf": CategoryDocument, "txt": CategoryDocument, "md": CategoryDocument,
	"pages": CategoryDocument,

	"xls": CategorySpreadsheet, "xlsx": CategorySpreadsheet, "xlsm": CategorySpreadsheet,
	"xltx": CategorySpreadsheet, "ods": CategorySpreadsheet, "csv": CategorySpreadsheet,
	"tsv": CategorySpreadsheet, "numbers": CategorySpreadsheet,

	"ppt": CategoryPresentation, "pptx": CategoryPresentation, "odp": CategoryPresentation,
	"key": CategoryPresentation,

	"jpg": CategoryImage, "jpeg": CategoryImage, "png": CategoryImage, "gif": CategoryImage,
	"webp": CategoryImage, "bmp": CategoryImage, "tif": CategoryImage, "tiff": CategoryImage,
	"svg": CategoryImage, "heic": CategoryImage, "ico": CategoryImage,

	"mp4": CategoryVideo, "mov": CategoryVideo, "avi": CategoryVideo, "mkv": CategoryVideo,
	"webm": CategoryVideo, "wmv": CategoryVideo, "m4v": CategoryVideo,

	"mp3": CategoryAudio, "wav": CategoryAudio, "flac": CategoryAudio, "aac": CategoryAudio,
	"m4a": CategoryAudio, "ogg": CategoryAudio, "wma": CategoryAudio,

	"zip": CategoryArchive, "rar": CategoryArchive, "7z": CategoryArchive, "tar": CategoryArchive,
	"gz": CategoryArchive, "tgz": CategoryArchive, "bz2": CategoryArchive,

	"go": CategoryCode, "js": CategoryCode, "ts": CategoryCode, "jsx": CategoryCode,
	"tsx": CategoryCode, "py": CategoryCode, "java": CategoryCode, "c": CategoryCode,
	"cpp": CategoryCode, "h": CategoryCode, "rs": CategoryCode, "rb": CategoryCode,
	"php": CategoryCode, "cs": CategoryCode, "swift": CategoryCode, "kt": CategoryCode,
	"html": CategoryCode, "css": CategoryCode, "json": CategoryCode, "xml": CategoryCode,
	"yaml": CategoryCode, "yml": CategoryCode, "toml": CategoryCode, "sql": CategoryCode,
	"sh": CategoryCode, "bash": CategoryCode,

	"pdf": CategoryPDF,
}

// ExtensionOf returns the lower-case extension without a leading dot, or "".
func ExtensionOf(name string) string {
	base := name
	if i := strings.LastIndex(base, "/"); i >= 0 {
		base = base[i+1:]
	}
	if i := strings.LastIndex(base, "."); i >= 0 && i < len(base)-1 {
		return strings.ToLower(base[i+1:])
	}
	return ""
}

// CategoryForExtension returns the static category for an extension (with or without dot).
func CategoryForExtension(ext string) string {
	ext = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(ext)), ".")
	if cat, ok := extensionCategory[ext]; ok {
		return cat
	}
	return CategoryOther
}

// KnownCategories returns the shipped category ids for UI dropdowns.
func KnownCategories() []string {
	return []string{
		CategoryDocument, CategorySpreadsheet, CategoryPresentation,
		CategoryImage, CategoryVideo, CategoryAudio, CategoryArchive,
		CategoryCode, CategoryPDF, CategoryOther,
	}
}
