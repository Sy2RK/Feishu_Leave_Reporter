#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -f "${PROJECT_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/.env"
  set +a
fi

# launchd may inherit proxy environment variables from the user session.
# Feishu API requests work fine over direct network in this deployment,
# while the websocket client can fail if a SOCKS proxy is configured.
unset ALL_PROXY all_proxy HTTP_PROXY http_proxy HTTPS_PROXY https_proxy

cd "${PROJECT_ROOT}"
UV_BIN="${UV_BIN:-uv}"
exec "${UV_BIN}" run --no-sync feishu-leave-sync
