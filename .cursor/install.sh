#!/usr/bin/env bash
# Idempotent Cloud Agent bootstrap for the Arke Platform monorepo.
# Prepares the runnable, self-contained packages under arke-platform/:
#   - web-app / web-admin  (Vite + React, arkeopen/arkeogis modes)
#   - server/arkeoserver   (Express backend stub)
#   - Python data-processing scripts (invoked via `python3`)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT/arke-platform"

# The base image ships python3 but not pip; install it once so the data
# scripts (which call the system `python3`) can import their dependencies.
if ! python3 -m pip --version >/dev/null 2>&1; then
  sudo apt-get update -y
  sudo apt-get install -y --no-install-recommends python3-pip
fi

# Node workspaces defined in package.json: web-app, web-admin, server/arkeoserver.
npm install

# Compile the backend stub so `npm --prefix server/arkeoserver start` has dist/.
npm --prefix server/arkeoserver run build

# Python dependencies used by scripts/*.py. Installed into the system
# interpreter because the npm scripts invoke `python3` directly (no venv).
python3 -m pip install --break-system-packages \
  -r requirements-google-drive.txt \
  pandas
