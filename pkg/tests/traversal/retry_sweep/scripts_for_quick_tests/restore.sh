#!/bin/bash
# restore.sh
# Restores permissions to A/items folder (Linux version)

BaseDir="${HOME}/sylos_retry_test"
TreeA="${BaseDir}/A/items"

echo "Restoring access to A/items..."

# Restore permissions on A/items if it exists
if [ -d "$TreeA" ]; then
    # Restore normal read, write, and execute permissions for owner
    chmod 755 "$TreeA" 2>/dev/null || {
        # Fallback: restore basic permissions
        chmod u+rwx "$TreeA" 2>/dev/null || {
            echo "Error: Could not restore permissions for A/items"
            exit 1
        }
    }
    
    echo "Permissions restored for A/items."
else
    echo "A/items folder not found at: $TreeA"
    exit 1
fi
