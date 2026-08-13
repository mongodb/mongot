#!/bin/bash

# 1. Capture the absolute path to the format.check binary before we change directories
CHECK_BIN="$(pwd)/$1"

# 2. Find the real, physical workspace directory on your machine.
# Bazel symlinks //:MODULE.bazel into the test. `realpath` resolves that symlink.
MODULE_BAZEL_PATH=$(realpath "$2")
REAL_WORKSPACE_DIR=$(dirname "$MODULE_BAZEL_PATH")

# 3. Export this so the aspect_rules_lint formatters know where to scan
export BUILD_WORKSPACE_DIRECTORY="$REAL_WORKSPACE_DIR"

# 4. Change into the real workspace folder so formatters can see the unformatted files
cd "$REAL_WORKSPACE_DIR" || { echo "$REAL_WORKSPACE_DIR does not exist"; exit 1; }

# 5. Run the check executable
if ! "$CHECK_BIN"; then
    echo ""
    echo "================================================================="
    echo "❌ FORMATTING CHECK FAILED ❌"
    echo "Don't panic! Just run the following command locally to fix your files:"
    echo "  bazel run //bazel/tools/format"
    echo "================================================================="
    echo ""
    HOOKS_DIR="$(git rev-parse --git-path hooks)"
    if ! grep -q "pre-commit" "$HOOKS_DIR/pre-commit" 2>/dev/null; then
      echo "You do not have pre-commit hooks installed"
      echo "Run the following command to have this error resolved automatically on commit:"
      echo "pre-commit install"
    fi
    exit 1
fi