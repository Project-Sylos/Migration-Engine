#!/bin/bash
# cleanup.sh
# Restores permissions and removes retry-sweep test folders (Linux version)

BaseDir="${HOME}/sylos_retry_test"
TreeA="${BaseDir}/A/items"

echo "Cleaning up retry-sweep test artifacts..."

# Restore permissions on A/items if it exists
if [ -d "$TreeA" ]; then
    echo "Restoring permissions on A/items..."
    
    # Restore normal permissions before deletion
    chmod 755 "$TreeA" 2>/dev/null || chmod u+rwx "$TreeA" 2>/dev/null
fi

# Remove everything
rm -rf "$BaseDir" 2>/dev/null

echo "Cleanup complete."
