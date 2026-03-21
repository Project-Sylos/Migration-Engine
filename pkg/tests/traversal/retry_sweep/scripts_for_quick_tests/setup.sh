#!/bin/bash
# setup.sh
# Creates two asymmetric folder trees:
#   A/items  -> access denied (SRC-like, minimal structure)
#   B/items  -> accessible (DST-like, superset structure with extra files/folders)

BaseDir="${HOME}/..sylos_retry_test"
TreeA="${BaseDir}/A/items"
TreeB="${BaseDir}/B/items"

echo "Setting up retry-sweep dual-tree permission test..."

# Clean slate
rm -rf "$BaseDir"

# Create asymmetric folder trees

# Create A/items (SRC)
mkdir -p "$TreeA/subfolder_a"
echo "test file 1" > "$TreeA/file1.txt"
echo "test file 2" > "$TreeA/subfolder_a/file2.txt"
echo "A extra 1" > "$TreeA/extra1.txt"
echo "A extra 2" > "$TreeA/extra2.txt"
# Only A/items has file1.txt, subfolder_a with a file, and two extra files (extra1.txt, extra2.txt)

# Create B/items (DST) with a superset structure
mkdir -p "$TreeB/subfolder_a"
mkdir -p "$TreeB/subfolder_b"
mkdir -p "$TreeB/extra_folder"

echo "test file 1" > "$TreeB/file1.txt"
echo "test file 2" > "$TreeB/subfolder_a/file2.txt"
echo "test file 3" > "$TreeB/subfolder_b/file3.txt"
echo "dst only file" > "$TreeB/extra_folder/dst_extra.txt"

echo "Created asymmetric folder trees: A/items (SRC) and B/items (DST)."

# Deny access ONLY to A/items
echo "Denying access to A/items (SRC simulation)..."

# Remove read, write, and execute permissions
chmod 000 "$TreeA" 2>/dev/null || {
    # Fallback: remove write and execute permissions
    chmod u-wx,go-rwx "$TreeA" 2>/dev/null || {
        echo "Warning: Could not fully deny permissions. You may need appropriate permissions."
    }
}

echo "Access denied for A/items."
echo "B/items remains accessible."

echo ""
echo "Test folder locations:"
echo "  SRC (A): $TreeA"
echo "  DST (B): $TreeB"
echo ""
echo "Use these paths when configuring Sylos:"
echo "  - Source root -> ${BaseDir}/A"
echo "  - Destination root -> ${BaseDir}/B"
echo ""

echo "Ready for initial Sylos traversal."
