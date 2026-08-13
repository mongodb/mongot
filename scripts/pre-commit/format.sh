#!/usr/bin/env bash
# ==============================================================================
# This script is invoked by pre-commit to ensure any commit passes
# lint tests by reformatting all staged files automatically and amending the commit.
# To format files without staging changes, prefer `make fix`
# Usage: ./format.sh <file1> <file2> ...

# Fail the script if any command fails
set -euo pipefail

if [ $# = 0 ] ; then
  echo 'No files changed'
  exit 0
fi

# Reformat files inplace, but don't pollute agent context unless an error occurs.
# Each file is passed as a separate argument so the formatter receives individual paths.
./scripts/tools/bazelisk/run.sh run \
  --ui_event_filters=-info,-warning \
  --noshow_progress \
  //bazel/tools/format -- "$@"


if ! git diff --quiet -- "$@"; then
  echo 'Some files were re-formatted to comply with Mongot style. Amending commit...'
  git add -u -- "$@" # Ensures we do not add any previously unstaged files
fi