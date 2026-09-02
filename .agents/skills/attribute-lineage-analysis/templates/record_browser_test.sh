#!/usr/bin/env bash
# GATE 2 helper: start/stop a REAL screen recording of the live desktop while the
# lineage viz is clicked through in a visible browser. Uses ffmpeg x11grab on the
# display the browser is actually rendered on — never a headless/offscreen page.
#
#   record_browser_test.sh start <OUTPUT_DIR>/browser_test [display]   # -> prints pid file
#   record_browser_test.sh stop  <OUTPUT_DIR>/browser_test             # -> finalizes .mp4, prints duration/size
#
# Prefer the platform's built-in recording tools (recording_start/recording_stop)
# when available; this script is the fallback so the gate never lacks a recording.
set -euo pipefail

cmd="${1:-}"; out="${2:-analysis/browser_test}"; disp="${3:-${DISPLAY:-:0}}"
mkdir -p "$out"
pidf="$out/.ffmpeg.pid"
mp4="$out/lineage_viz_walkthrough.mp4"

case "$cmd" in
  start)
    geom="$(xrandr --display "$disp" 2>/dev/null | awk '/\*/{print $1; exit}')"
    geom="${geom:-1600x1200}"
    # -draw_mouse 1 so the cursor is visible in the recording (proves real interaction)
    nohup ffmpeg -y -loglevel error -f x11grab -draw_mouse 1 -framerate 15 -video_size "$geom" -i "$disp" \
      -vf "scale=trunc(iw/2)*2:trunc(ih/2)*2" -c:v libx264 -preset veryfast -pix_fmt yuv420p "$mp4" \
      >"$out/.ffmpeg.log" 2>&1 &
    echo $! > "$pidf"
    sleep 1
    kill -0 "$(cat "$pidf")" 2>/dev/null || { echo "ffmpeg failed to start:"; cat "$out/.ffmpeg.log"; exit 1; }
    echo "recording $disp ($geom) -> $mp4 (pid $(cat "$pidf"))"
    ;;
  stop)
    [ -f "$pidf" ] || { echo "no recording in progress under $out"; exit 1; }
    kill -INT "$(cat "$pidf")" 2>/dev/null || true
    for _ in $(seq 1 30); do kill -0 "$(cat "$pidf")" 2>/dev/null || break; sleep 0.5; done
    rm -f "$pidf"
    dur="$(ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "$mp4" 2>/dev/null || echo '?')"
    echo "saved $mp4 — duration ${dur}s, $(du -h "$mp4" | cut -f1)"
    ;;
  *)
    echo "usage: $0 start|stop <browser_test_dir> [display]"; exit 2
    ;;
esac
