#!/usr/bin/env nix-shell
#!nix-shell --pure -p poetry -i bash
# shellcheck shell=bash

set -euo pipefail

if [ "$#" -ne "1" ]; then
  echo "error: exactly one argument required, got: $#"
  exit 1
fi

dry_run="$1"

if [ -n "${dry_run}" ]; then
  echo "error: dry_run not passed as argument"
  exit 1
fi

if "$dry_run"; then
  poetry publish --dry-run
else
  poetry publish
fi
