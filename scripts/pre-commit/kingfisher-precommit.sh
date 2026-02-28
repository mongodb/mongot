#!/bin/bash

COMMAND="kingfisher"

# Check if kingfisher is installed
if ! command -v "$COMMAND" &> /dev/null; then
    echo "Error: '$COMMAND' is not installed or not in your PATH." >&2
    echo "Install it using 'brew install kingfisher'" \
        "or following the download instructions at https://github.com/mongodb/kingfisher" >&2
    exit 1
fi

# If installed, call it in staged mode to only scan locally staged changes
kingfisher scan . \
  --staged \
  --quiet \
  --no-update-check \
  --skip-word hunter1 \
  --skip-word xxx-sanitized-xxx \
  --exclude 'src/test/integration/resources/server/*.pem' \
  --exclude 'src/test/integration/resources/atlas/kmip/*.pem' \
  --exclude 'docker/tls/*.pem'