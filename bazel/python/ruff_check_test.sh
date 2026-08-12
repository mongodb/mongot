#!/bin/bash

# Runs `ruff check` over the whole repository as a bazel test so it participates
# in `make lint` alongside checkstyle and buildifier.
#
# Same shape as //bazel/tools/format:format_test: the Python sources under check
# are not all inputs to bazel targets, so instead of declaring them as data we
# resolve the physical workspace directory and scan it in place. The test is
# tagged "external" so it is never cached.

# Capture the absolute path to the ruff binary before we change directories.
RUFF_BIN="$(pwd)/$1"

# Bazel symlinks //:MODULE.bazel into the test; realpath resolves it back to the
# real workspace so ruff sees the developer's actual files.
MODULE_BAZEL_PATH=$(realpath "$2")
REAL_WORKSPACE_DIR=$(dirname "$MODULE_BAZEL_PATH")

cd "$REAL_WORKSPACE_DIR" || { echo "$REAL_WORKSPACE_DIR does not exist"; exit 1; }

# Keep the cache inside the test's scratch dir. ruff otherwise resolves a user
# cache directory and fails outright when HOME is absent from the test env.
if ! "$RUFF_BIN" check --cache-dir "${TEST_TMPDIR:-/tmp}/ruff-cache" .; then
    echo ""
    echo "================================================================="
    echo "❌ PYTHON LINT CHECK FAILED ❌"
    echo "Most findings are autofixable. Run:"
    echo "  bazel run @aspect_rules_lint//lint:ruff_bin -- check --fix \\"
    echo "    \"\$(bazel info workspace)\""
    echo "Rules are configured in .ruff.toml"
    echo "================================================================="
    echo ""
    exit 1
fi
