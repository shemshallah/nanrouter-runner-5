#!/bin/sh
set -eu

role="$(cat role 2>/dev/null || printf 'agent')"
case "$role" in
  hub)
    exec python hub.py "$@"
    ;;
  agent)
    exec python runner.py "$@"
    ;;
  *)
    echo "unknown runner role: $role" >&2
    exit 2
    ;;
esac
