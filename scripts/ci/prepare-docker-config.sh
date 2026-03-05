#!/usr/bin/env bash
set -euo pipefail
# Prepare Docker config for ECR authentication on CI hosts.
# Ensures ~/.docker exists with proper permissions and removes any
# credsStore that fails in headless environments.

mkdir -p ~/.docker

if [ ! -w ~/.docker ]; then
  sudo chown -R "$USER:$USER" ~/.docker
fi

chmod 700 ~/.docker

# Some CI hosts have a credsStore (e.g., "desktop", "secretservice") that fails
# with "not implemented" in headless environments. Remove it so docker login
# falls back to storing credentials in ~/.docker/config.json directly.
if [ -f ~/.docker/config.json ]; then
  python3 -c "
import json, sys
p = sys.argv[1]
with open(p) as f: c = json.load(f)
if 'credsStore' in c:
    del c['credsStore']
    with open(p, 'w') as f: json.dump(c, f, indent=2)
" ~/.docker/config.json
fi
