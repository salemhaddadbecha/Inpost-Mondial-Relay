#!/usr/bin/env bash
set -e

case "$1" in
bootstrap-data)
    python src/setup_bootstrap.py
    ;;
*)
    echo "Usage: $0 bootstrap-data"
    exit 1
    ;;
esac
