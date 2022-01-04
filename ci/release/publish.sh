#!/usr/bin/env nix-shell
#!nix-shell --pure -p poetry -i bash
# shellcheck shell=bash

set -euo pipefail

poetry publish "$@"
