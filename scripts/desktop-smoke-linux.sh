#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <appimage>" >&2
  exit 2
fi
if [[ "$(uname -s)" != "Linux" ]]; then
  echo "desktop Linux smoke requires Linux" >&2
  exit 2
fi

for command_name in lsof openbox pgrep ps readlink sed xdotool xprop; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "required command not found: $command_name" >&2
    exit 2
  fi
done

appimage_directory="$(cd "$(dirname "$1")" && pwd -P)"
appimage_path="$appimage_directory/$(basename "$1")"
if [[ ! -x "$appimage_path" ]]; then
  echo "AppImage is not executable: $appimage_path" >&2
  exit 2
fi

smoke_tmp_root="${TMPDIR:-/tmp}"
smoke_tmp_root="${smoke_tmp_root%/}"
smoke_directory="$(mktemp -d "$smoke_tmp_root/cy-kaf-desktop-linux-smoke.XXXXXX")"
config_path="$smoke_directory/config.yaml"
stdout_log="$smoke_directory/stdout.log"
stderr_log="$smoke_directory/stderr.log"
window_manager_stdout_log="$smoke_directory/window-manager-stdout.log"
window_manager_stderr_log="$smoke_directory/window-manager-stderr.log"
shell_pid=""
desktop_pid=""
sidecar_pid=""
window_id=""
window_manager_pid=""

process_is_running() {
  local target_pid="$1"
  local state
  state="$(ps -p "$target_pid" -o state= 2>/dev/null | tr -d '[:space:]' || true)"
  [[ -n "$state" && "$state" != Z* ]]
}

cleanup() {
  if [[ -n "$sidecar_pid" ]] && process_is_running "$sidecar_pid"; then
    kill -TERM "$sidecar_pid" 2>/dev/null || true
  fi
  if [[ -n "$desktop_pid" && "$desktop_pid" != "$shell_pid" ]] &&
    process_is_running "$desktop_pid"; then
    kill -TERM "$desktop_pid" 2>/dev/null || true
  fi
  if [[ -n "$shell_pid" ]] && process_is_running "$shell_pid"; then
    kill -TERM "$shell_pid" 2>/dev/null || true
  fi
  if [[ -n "$window_manager_pid" ]] && process_is_running "$window_manager_pid"; then
    kill -TERM "$window_manager_pid" 2>/dev/null || true
  fi
  if [[ -n "$shell_pid" ]]; then
    wait "$shell_pid" 2>/dev/null || true
  fi
  if [[ -n "$desktop_pid" && "$desktop_pid" != "$shell_pid" ]]; then
    wait "$desktop_pid" 2>/dev/null || true
  fi
  if [[ -n "$window_manager_pid" ]]; then
    wait "$window_manager_pid" 2>/dev/null || true
  fi
  case "$smoke_directory" in
    "$smoke_tmp_root"/cy-kaf-desktop-linux-smoke.*)
      rm -rf -- "$smoke_directory"
      ;;
  esac
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

fail() {
  echo "DESKTOP LINUX SMOKE FAILED: $1" >&2
  if [[ -s "$stdout_log" ]]; then
    echo "desktop stdout:" >&2
    sed -n '1,120p' "$stdout_log" >&2
  fi
  if [[ -s "$stderr_log" ]]; then
    echo "desktop stderr:" >&2
    sed -n '1,120p' "$stderr_log" >&2
  fi
  if [[ -s "$window_manager_stderr_log" ]]; then
    echo "window manager stderr:" >&2
    sed -n '1,120p' "$window_manager_stderr_log" >&2
  fi
  exit 1
}

printf 'kafka:\n  clusters: []\n' >"$config_path"
mkdir -p \
  "$smoke_directory/cache" \
  "$smoke_directory/config" \
  "$smoke_directory/data"

openbox --sm-disable \
  >"$window_manager_stdout_log" \
  2>"$window_manager_stderr_log" &
window_manager_pid=$!

window_manager_ready=""
for _ in $(seq 1 50); do
  if ! process_is_running "$window_manager_pid"; then
    fail "Openbox exited before becoming ready"
  fi
  window_manager_state="$(xprop -root _NET_SUPPORTING_WM_CHECK 2>/dev/null || true)"
  if [[ "$window_manager_state" == *"window id #"* ]]; then
    window_manager_ready=1
    break
  fi
  sleep 0.1
done
[[ -n "$window_manager_ready" ]] || fail "Openbox did not become ready within five seconds"

APPIMAGE_EXTRACT_AND_RUN=1 \
  CY_KAF_DESKTOP_TEST_CONFIG="$config_path" \
  XDG_CACHE_HOME="$smoke_directory/cache" \
  XDG_CONFIG_HOME="$smoke_directory/config" \
  XDG_DATA_HOME="$smoke_directory/data" \
  "$appimage_path" >"$stdout_log" 2>"$stderr_log" &
shell_pid=$!

for _ in $(seq 1 60); do
  if ! process_is_running "$shell_pid"; then
    fail "desktop shell exited before its window became ready"
  fi
  matching_window_ids=()
  matching_desktop_pids=()
  while IFS= read -r candidate_window_id; do
    [[ -n "$candidate_window_id" ]] || continue
    candidate_desktop_pid="$(
      xdotool getwindowpid "$candidate_window_id" 2>/dev/null || true
    )"
    candidate_desktop_executable="$(
      readlink -f "/proc/${candidate_desktop_pid}/exe" 2>/dev/null || true
    )"
    if process_is_running "$candidate_desktop_pid" &&
      [[ "$candidate_desktop_executable" == */cy-kaf-client-desktop ]]; then
      matching_window_ids+=("$candidate_window_id")
      matching_desktop_pids+=("$candidate_desktop_pid")
    fi
  done < <(xdotool search --onlyvisible --name '^Cy KafClient$' 2>/dev/null || true)
  if [[ "${#matching_window_ids[@]}" == "1" ]]; then
    window_id="${matching_window_ids[0]}"
    desktop_pid="${matching_desktop_pids[0]}"
    break
  fi
  sleep 0.5
done
[[ -n "$window_id" ]] || fail "no unique visible Cy KafClient window within 30 seconds"

for _ in $(seq 1 60); do
  matching_sidecars=()
  while IFS= read -r child_pid; do
    [[ -n "$child_pid" ]] || continue
    child_command="$(ps -p "$child_pid" -o command= 2>/dev/null || true)"
    if [[ "$child_command" == *"/cy-kaf-client --desktop --no-browser "* ]]; then
      matching_sidecars+=("$child_pid")
    fi
  done < <(pgrep -P "$desktop_pid" || true)
  if [[ "${#matching_sidecars[@]}" == "1" ]]; then
    sidecar_pid="${matching_sidecars[0]}"
    break
  fi
  sleep 0.5
done
[[ -n "$sidecar_pid" ]] || fail "bundled Go sidecar is not a unique direct desktop child"

listen_output=""
listen_count="0"
port=""
for _ in $(seq 1 60); do
  listen_output="$(lsof -nP -a -p "$sidecar_pid" -iTCP -sTCP:LISTEN 2>/dev/null || true)"
  listen_count="$(
    printf '%s\n' "$listen_output" |
      sed '1d' |
      sed '/^[[:space:]]*$/d' |
      wc -l |
      tr -d '[:space:]'
  )"
  port="$(
    printf '%s\n' "$listen_output" |
      sed -nE 's/.*TCP 127\.0\.0\.1:([0-9]+) \(LISTEN\)$/\1/p'
  )"
  if [[ "$listen_count" == "1" && "$port" =~ ^[1-9][0-9]*$ ]]; then
    break
  fi
  sleep 0.5
done
if [[ "$listen_count" != "1" ]]; then
  fail "sidecar must own exactly one TCP listener; lsof=[$listen_output]"
fi
[[ "$port" =~ ^[1-9][0-9]*$ ]] || fail "sidecar listener is not restricted to 127.0.0.1"

xdotool windowclose "$window_id" || fail "could not close the native window"

closed=""
for _ in $(seq 1 50); do
  shell_running=""
  desktop_running=""
  sidecar_running=""
  port_listening=""
  process_is_running "$shell_pid" && shell_running=1
  process_is_running "$desktop_pid" && desktop_running=1
  process_is_running "$sidecar_pid" && sidecar_running=1
  lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1 && port_listening=1
  if [[
    -z "$shell_running" &&
    -z "$desktop_running" &&
    -z "$sidecar_running" &&
    -z "$port_listening"
  ]]; then
    closed=1
    break
  fi
  sleep 0.1
done
[[ -n "$closed" ]] || fail "launcher, desktop, sidecar, or loopback listener survived normal close"

wait "$shell_pid"
shell_pid=""
desktop_pid=""
sidecar_pid=""
if process_is_running "$window_manager_pid"; then
  kill -TERM "$window_manager_pid"
fi
wait "$window_manager_pid" 2>/dev/null || true
window_manager_pid=""
echo "DESKTOP LINUX SMOKE OK"
