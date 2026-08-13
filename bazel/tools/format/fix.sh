#!/usr/bin/env bash
# This script formats all files in the repo changed since the current branch diverged from master
# This script is intended to be used by AI agents and other CLI tools.
# Usage: ./bazel/tools/format/fix.sh

set -eou pipefail

# Find most recent commit that diverged from master.
# Fall back to HEAD if origin/master is unavailable (e.g. shallow clones, alternative remotes).
MERGE_BASE="$(git merge-base origin/master HEAD 2>/dev/null)" || MERGE_BASE="HEAD"

# Compute minimal-ish set of changed files (ignoring deletions).
# This reduces runtime from ~6s to <1s.
# Read into an array so each path is passed as a separate argument. We avoid
# `mapfile`/`readarray` because they are unavailable in the bash 3.2 that ships
# with macOS.
FILES=()
while IFS= read -r file; do
  FILES+=("$file")
done < <(git diff --name-only --diff-filter=ACMR "$MERGE_BASE")

# If no files changed relative to merge-base, nothing to format.
if [ "${#FILES[@]}" -eq 0 ]; then
  exit 0
fi

# Reformat files inplace, but don't pollute agent context unless an error occurs.
# Each file is passed as a separate argument so the formatter receives individual paths.
./scripts/tools/bazelisk/run.sh run \
  --ui_event_filters=-info,-warning \
  --noshow_progress \
   //bazel/tools/format -- "${FILES[@]}"