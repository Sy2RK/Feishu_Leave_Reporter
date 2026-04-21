#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEMPLATE_PATH="${PROJECT_ROOT}/launchd/com.ggbond.feishu-leave-sync.plist.template"
GENERATED_DIR="${PROJECT_ROOT}/launchd/generated"

if [[ -f "${PROJECT_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/.env"
  set +a
fi

LABEL="${LAUNCHD_LABEL:-com.ggbond.feishu-leave-sync}"
UV_BIN="${UV_BIN:-$(command -v uv)}"

if [[ -z "${UV_BIN}" ]]; then
  echo "uv not found in PATH" >&2
  exit 1
fi

for required_var in FEISHU_APP_ID FEISHU_APP_SECRET FEISHU_APPROVAL_CODES; do
  if [[ -z "${!required_var:-}" ]]; then
    echo "Missing required variable ${required_var}. Populate .env first." >&2
    exit 1
  fi
done

mkdir -p "${PROJECT_ROOT}/var/log" "${PROJECT_ROOT}/var/state" "${GENERATED_DIR}" "${HOME}/Library/LaunchAgents"
"${UV_BIN}" sync

RUN_SCRIPT="${PROJECT_ROOT}/scripts/run_service.sh"
STDOUT_LOG="${PROJECT_ROOT}/var/log/feishu-leave-sync.stdout.log"
STDERR_LOG="${PROJECT_ROOT}/var/log/feishu-leave-sync.stderr.log"
GENERATED_PLIST="${GENERATED_DIR}/${LABEL}.plist"
TARGET_PLIST="${HOME}/Library/LaunchAgents/${LABEL}.plist"

python3 - "${TEMPLATE_PATH}" "${GENERATED_PLIST}" "${LABEL}" "${RUN_SCRIPT}" "${PROJECT_ROOT}" "${STDOUT_LOG}" "${STDERR_LOG}" "${UV_BIN}" <<'PY'
from pathlib import Path
import sys

template_path = Path(sys.argv[1])
output_path = Path(sys.argv[2])
label = sys.argv[3]
run_script = sys.argv[4]
project_root = sys.argv[5]
stdout_log = sys.argv[6]
stderr_log = sys.argv[7]
uv_bin = sys.argv[8]

content = template_path.read_text(encoding="utf-8")
content = content.replace("__LABEL__", label)
content = content.replace("__RUN_SCRIPT__", run_script)
content = content.replace("__PROJECT_ROOT__", project_root)
content = content.replace("__STDOUT_LOG__", stdout_log)
content = content.replace("__STDERR_LOG__", stderr_log)
content = content.replace("__UV_BIN__", uv_bin)
output_path.write_text(content, encoding="utf-8")
PY

chmod +x "${RUN_SCRIPT}"
plutil -lint "${GENERATED_PLIST}"
cp "${GENERATED_PLIST}" "${TARGET_PLIST}"

launchctl bootout "gui/$(id -u)" "${TARGET_PLIST}" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${TARGET_PLIST}"
launchctl enable "gui/$(id -u)/${LABEL}" || true
launchctl kickstart -k "gui/$(id -u)/${LABEL}"

echo "Installed and started ${LABEL}"
echo "plist: ${TARGET_PLIST}"
