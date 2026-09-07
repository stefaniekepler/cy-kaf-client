#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <unbundled-tauri-executable>" >&2
  exit 2
fi
if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "desktop smoke requires macOS" >&2
  exit 2
fi

for command_name in lsof osascript paste pgrep ps sed sort python3; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "required command not found: $command_name" >&2
    exit 2
  fi
done

app_directory="$(cd "$(dirname "$1")" && pwd -P)"
app_path="$app_directory/$(basename "$1")"
if [[ ! -x "$app_path" ]]; then
  echo "desktop executable is not runnable: $app_path" >&2
  exit 2
fi

smoke_tmp_root="${TMPDIR:-/tmp}"
smoke_tmp_root="${smoke_tmp_root%/}"
smoke_directory="$(mktemp -d "$smoke_tmp_root/cy-kaf-desktop-smoke.XXXXXX")"
config_path="$smoke_directory/config.yaml"
stdout_log="$smoke_directory/stdout.log"
stderr_log="$smoke_directory/stderr.log"
shell_pid=""
sidecar_pid=""

process_is_running() {
  local target_pid="$1"
  local state
  state="$(ps -p "$target_pid" -o state= 2>/dev/null | tr -d '[:space:]' || true)"
  [[ -n "$state" && "$state" != Z* ]]
}

cleanup() {
  if [[ -n "$shell_pid" ]] && process_is_running "$shell_pid"; then
    kill -TERM "$shell_pid" 2>/dev/null || true
  fi
  if [[ -n "$sidecar_pid" ]] && process_is_running "$sidecar_pid"; then
    kill -TERM "$sidecar_pid" 2>/dev/null || true
  fi
  if [[ -n "$shell_pid" ]]; then
    wait "$shell_pid" 2>/dev/null || true
  fi
  case "$smoke_directory" in
    "$smoke_tmp_root"/cy-kaf-desktop-smoke.*)
      rm -rf -- "$smoke_directory"
      ;;
  esac
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

fail() {
  echo "DESKTOP SMOKE FAILED: $1" >&2
  if [[ -s "$stdout_log" ]]; then
    echo "desktop stdout:" >&2
    sed -n '1,120p' "$stdout_log" >&2
  fi
  if [[ -s "$stderr_log" ]]; then
    echo "desktop stderr:" >&2
    sed -n '1,120p' "$stderr_log" >&2
  fi
  exit 1
}

safari_tab_count() {
  osascript <<'APPLESCRIPT'
tell application "Safari"
  set totalTabs to 0
  repeat with currentWindow in windows
    set totalTabs to totalTabs + (count of tabs of currentWindow)
  end repeat
  return totalTabs
end tell
APPLESCRIPT
}

chrome_tab_count() {
  osascript <<'APPLESCRIPT'
tell application "Google Chrome"
  set totalTabs to 0
  repeat with currentWindow in windows
    set totalTabs to totalTabs + (count of tabs of currentWindow)
  end repeat
  return totalTabs
end tell
APPLESCRIPT
}

browser_state() {
  local safari_pids chrome_pids safari_tabs chrome_tabs
  safari_pids="$(pgrep -x Safari | sort -n | paste -sd, - || true)"
  chrome_pids="$(pgrep -x "Google Chrome" | sort -n | paste -sd, - || true)"
  safari_tabs="-"
  chrome_tabs="-"
  if [[ -n "$safari_pids" ]]; then
    safari_tabs="$(safari_tab_count)" || fail "cannot count Safari tabs"
  fi
  if [[ -n "$chrome_pids" ]]; then
    chrome_tabs="$(chrome_tab_count)" || fail "cannot count Chrome tabs"
  fi
  printf 'safari=%s:%s;chrome=%s:%s\n' \
    "$safari_pids" "$safari_tabs" "$chrome_pids" "$chrome_tabs"
}

window_is_ready() {
  osascript \
    -e 'on run argv' \
    -e 'set targetPid to item 1 of argv as integer' \
    -e 'tell application "System Events"' \
    -e 'set candidates to every application process whose unix id is targetPid' \
    -e 'if (count of candidates) is 0 then return "missing"' \
    -e 'set targetProcess to item 1 of candidates' \
    -e 'tell targetProcess' \
    -e 'if frontmost and (count of windows) > 0 and name of window 1 is "Cy KafClient" then return "ready"' \
    -e 'end tell' \
    -e 'end tell' \
    -e 'return "waiting"' \
    -e 'end run' \
    "$shell_pid" 2>/dev/null | grep -qx "ready"
}

close_main_window() {
  osascript \
    -e 'on run argv' \
    -e 'set targetPid to item 1 of argv as integer' \
    -e 'tell application "System Events"' \
    -e 'set targetProcess to first application process whose unix id is targetPid' \
    -e 'tell targetProcess to click button 1 of window 1' \
    -e 'end tell' \
    -e 'end run' \
    "$shell_pid" >/dev/null
}

click_web_button() {
  osascript - "$shell_pid" "$1" <<'APPLESCRIPT'
on run argv
  set targetPid to item 1 of argv as integer
  set targetLabel to item 2 of argv
  tell application "System Events"
    tell first application process whose unix id is targetPid
      set elementsList to get entire contents of window 1
      repeat with itemRef in elementsList
        if role of itemRef is "AXButton" then
          if name of itemRef is targetLabel or description of itemRef is targetLabel then
            perform action "AXPress" of itemRef
            return "clicked"
          end if
        end if
      end repeat
    end tell
  end tell
  error "requested button is not ready"
end run
APPLESCRIPT
}

config_export_count() {
  python3 - "$smoke_directory/downloads" <<'PYCOUNT'
import pathlib, sys
print(len(list(pathlib.Path(sys.argv[1]).glob("kafka-environments-*.yaml"))))
PYCOUNT
}

printf 'kafka:\n  clusters: []\n' >"$config_path"
browser_before="$(browser_state)"

CFFIXED_USER_HOME="$smoke_directory" CY_KAF_DESKTOP_TEST_CONFIG="$config_path" \
  "$app_path" >"$stdout_log" 2>"$stderr_log" &
shell_pid=$!

window_ready=""
for _ in $(seq 1 60); do
  if ! process_is_running "$shell_pid"; then
    fail "desktop shell exited before its window became ready"
  fi
  if window_is_ready; then
    window_ready=1
    break
  fi
  sleep 0.5
done
[[ -n "$window_ready" ]] || fail "no foreground Cy KafClient window within 30 seconds"

for _ in $(seq 1 60); do
  while IFS= read -r child_pid; do
    [[ -n "$child_pid" ]] || continue
    child_command="$(ps -p "$child_pid" -o command= 2>/dev/null || true)"
    if [[ "$child_command" == *"/cy-kaf-client --desktop --no-browser "* ]]; then
      sidecar_pid="$child_pid"
      break 2
    fi
  done < <(pgrep -P "$shell_pid" || true)
  sleep 0.5
done
[[ -n "$sidecar_pid" ]] || fail "bundled Go sidecar is not a direct shell child"

listen_output=""
listen_count="0"
port=""
for _ in $(seq 1 60); do
  listen_output="$(lsof -nP -a -p "$sidecar_pid" -iTCP -sTCP:LISTEN 2>/dev/null || true)"
  listen_count="$(printf '%s\n' "$listen_output" | sed '1d' | sed '/^[[:space:]]*$/d' | wc -l | tr -d '[:space:]')"
  port="$(printf '%s\n' "$listen_output" | sed -nE 's/.*TCP 127\.0\.0\.1:([0-9]+) \(LISTEN\)$/\1/p')"
  if [[ "$listen_count" == "1" && "$port" =~ ^[1-9][0-9]*$ ]]; then
    break
  fi
  sleep 0.5
done
if [[ "$listen_count" != "1" ]]; then
  fail "sidecar must own exactly one TCP listener; lsof=[$listen_output]"
fi
[[ "$port" =~ ^[1-9][0-9]*$ ]] || fail "sidecar listener is not restricted to 127.0.0.1"

settings_ready=""
for _ in $(seq 1 30); do
  if click_web_button "Settings" >/dev/null 2>&1; then
    settings_ready=1
    break
  fi
  sleep 0.5
done
[[ -n "$settings_ready" ]] || fail "Settings did not become available"

# Actual WKWebView downloads: a blob anchor alone silently cancels on macOS
# without the native download handler. Use only the isolated empty fixture.
for expected_exports in 1 2; do
  click_web_button "一键导出配置" >/dev/null || fail "could not click configuration export"
  exported=""
  for _ in $(seq 1 30); do
    if [[ "$(config_export_count)" == "$expected_exports" ]]; then
      exported=1
      break
    fi
    sleep 0.5
  done
  [[ -n "$exported" ]] || fail "configuration export did not create a distinct download"
done
python3 - "$smoke_directory/downloads" <<'PYEXPORT' || fail "configuration export content mismatch"
import pathlib, sys
files = list(pathlib.Path(sys.argv[1]).glob("kafka-environments-*.yaml"))
assert len(files) == 2
for file in files:
    assert "".join(file.read_text().split()) == "kafka:clusters:[]"
PYEXPORT

browser_after="$(browser_state)"
[[ "$browser_after" == "$browser_before" ]] || {
  fail "browser process or tab state changed: before=[$browser_before] after=[$browser_after]"
}

close_main_window || fail "could not click the native window close button"

closed=""
for _ in $(seq 1 50); do
  shell_running=""
  sidecar_running=""
  port_listening=""
  process_is_running "$shell_pid" && shell_running=1
  process_is_running "$sidecar_pid" && sidecar_running=1
  lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1 && port_listening=1
  if [[ -z "$shell_running" && -z "$sidecar_running" && -z "$port_listening" ]]; then
    closed=1
    break
  fi
  sleep 0.1
done
[[ -n "$closed" ]] || fail "shell, sidecar, or loopback listener survived normal close"

wait "$shell_pid"
shell_pid=""
sidecar_pid=""
echo "DESKTOP SMOKE OK"
