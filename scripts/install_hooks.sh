#!/bin/sh
# Install Fortress git hooks from .githooks/ → .git/hooks/
# Run once per fresh clone:  bash scripts/install_hooks.sh
# Re-run to update hooks after a pull.

set -e

REPO_ROOT=$(git rev-parse --show-toplevel)
HOOKS_SRC="$REPO_ROOT/.githooks"
HOOKS_DEST="$REPO_ROOT/.git/hooks"

if [ ! -d "$HOOKS_SRC" ]; then
    echo "ERROR: .githooks/ not found at $HOOKS_SRC" >&2
    exit 1
fi

count=0
for hook in "$HOOKS_SRC"/*; do
    [ -f "$hook" ] || continue
    name=$(basename "$hook")
    cp "$hook" "$HOOKS_DEST/$name"
    chmod +x "$HOOKS_DEST/$name"
    echo "Installed: $name"
    count=$((count + 1))
done

echo "$count hook(s) installed at $HOOKS_DEST"
echo "To uninstall: rm $HOOKS_DEST/<hook-name>"
