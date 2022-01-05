#!/usr/bin/env nix-shell
#!nix-shell --pure --keep GITHUB_ACTIONS --keep GITHUB_TOKEN --keep POETRY_PYPI_TOKEN_PYPI -p cacert poetry git nodejs nix -i bash
# shellcheck shell=bash

set -euo pipefail

npx --yes \
  -p semantic-release \
  -p "@semantic-release/commit-analyzer" \
  -p "@semantic-release/release-notes-generator" \
  -p "@semantic-release/changelog" \
  -p "@semantic-release/github" \
  -p "@semantic-release/exec" \
  -p "@semantic-release/git" \
  semantic-release --ci
