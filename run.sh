#!/usr/bin/env bash
# run.sh — supervisor: chạy auto.mjs, tự restart khi crash, dừng khi auto exit 0 (hết việc/budget/SIGTERM).
# Local + GitHub Actions. Env: P4K_BUDGET P4K_FEED_PAGES P4K_FEED=0 P4K_DB P4K_LOG P4K_LOCK
cd /sdcard/Download/P4k || exit 1
LOG="${P4K_LOG:-/tmp/opencode/aw/auto.log}"
LOCK="${P4K_LOCK:-/tmp/opencode/aw/auto.lock}"
mkdir -p "$(dirname "$LOG")" "$(dirname "$LOCK")"
if [ -f "$LOCK" ] && kill -0 "$(cat "$LOCK" 2>/dev/null)" 2>/dev/null; then
  echo "run.sh: da co process $(cat "$LOCK"), thoat" >> "$LOG"
  exit 0
fi
echo $$ > "$LOCK"
CHILD=
cleanup() { [ -n "$CHILD" ] && kill "$CHILD" 2>/dev/null; rm -f "$LOCK"; }
trap 'cleanup; exit 0' INT TERM
trap cleanup EXIT
n=0
while :; do
  n=$((n + 1))
  echo "[$(date +%H:%M:%S)] run.sh start attempt=$n pid=$$" >> "$LOG"
  node auto.mjs "${P4K_BUDGET:-21000}" >> "$LOG" 2>&1 &
  CHILD=$!
  wait "$CHILD"
  rc=$?
  CHILD=
  echo "[$(date +%H:%M:%S)] run.sh attempt=$n rc=$rc" >> "$LOG"
  [ "$rc" -eq 0 ] && break
  [ "$n" -ge 5 ] && { echo "[$(date +%H:%M:%S)] run.sh that bai 5 lan, dung" >> "$LOG"; exit "$rc"; }
  sleep $((15 * n))
done
