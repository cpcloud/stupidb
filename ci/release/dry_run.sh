#!/usr/bin/env nix-shell
#!nix-shell --pure -p util-linux git nodejs -i bash
# shellcheck shell=bash

set -euo pipefail

curdir="$PWD"
worktree="$(mktemp -d)"
branch="$(basename "$worktree")"

git worktree add "$worktree"

function cleanup() {
  cd "$curdir" || exit 1
  git worktree remove "$worktree"
  git worktree prune
  git branch -D "$branch"
}

trap cleanup EXIT ERR

cd "$worktree" || exit 1

npx --yes semantic-release \
  --no-ci \
  --dry-run \
  --plugins \
  --branches "$branch" \
  --repository-url "file://$PWD"
