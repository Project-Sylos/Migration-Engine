#!/bin/bash
# deny.sh
# Denies permissions to A/items folder (Linux version)
# Note: On Linux, we use chmod to make the directory read-only or remove execute permissions
# This simulates access denial similar to Windows icacls deny

BaseDir="${HOME}/..sylos_retry_test"
TreeA="${BaseDir}/A/items"

echo "Denying access to A/items..." 

# Deny permissions on A/items if it exists
if [ -d "$TreeA" ]; then
    # Remove read, write, and execute permissions for owner, group, and others
    # This effectively denies access
    chmod 000 "$TreeA" 2>/dev/null || {
        # If chmod fails, try using chattr to make it immutable (requires root or appropriate permissions)
        echo "Warning: Could not change permissions directly. You may need to run with appropriate permissions."
        echo "Attempting alternative method..."
        # Remove write and execute permissions
        chmod u-wx,go-rwx "$TreeA" 2>/dev/null || {
            echo "Error: Could not deny permissions for A/items"
            exit 1
        }
    }
    
    echo "Permissions denied for A/items."
else
    echo "A/items folder not found at: $TreeA"
    exit 1
fi
