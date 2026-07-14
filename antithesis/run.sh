#!/usr/bin/env bash
#
# run.sh — launch a single pristine (baseline) hashicorp-raft benchmark run.
#
# The run launches via `snouty launch --webhook benchmark`; hashicorp-raft's
# specifics (the readable name, the config image, and the containers spared from
# faults) are passed as params. The description is prefixed "[baseline]" so
# triage can tell runs apart.
#
# Usage:
#   run.sh [duration] [description]
#
#   duration      run length in minutes (default 15)
#   description   free-text suffix added to the run's description (default "")
#
# Env:
#   BUILD_TAG     if set, passed as `--param benchmark.tag=<tag>` to pin the image
#                 tag (the same env var build.sh / build-and-push.sh use). Unset =
#                 the notebook default (:latest).
#
# Network is required to launch, so run this outside the build sandbox.

set -euo pipefail

DURATION="${1:-15}"
DESCRIPTION="${2:-}"

# hashicorp-raft's harness specifics, supplied to the `benchmark` notebook.
BENCHMARK_NAME="Hashicorp Raft"
BENCHMARK_IMAGE="hashicorp-raft-config"
EXCLUDE_FAULTS="raft-client,raft-monitor"
SOURCE="benchmarks/hashicorp-raft"
DESC="${DESCRIPTION}"

PARAMS=(--param "benchmark.name=${BENCHMARK_NAME}"
        --param "benchmark.image=${BENCHMARK_IMAGE}"
        --param "benchmark.exclude_faults=${EXCLUDE_FAULTS}")
[[ -n "${BUILD_TAG:-}" ]] && PARAMS+=(--param "benchmark.tag=${BUILD_TAG}")

echo "==> launching baseline: description='${DESC}'"

# `</dev/null`: don't let the launcher read stdin, so a child that drains stdin
# can't disturb an interactive caller.
snouty launch \
    --webhook benchmark \
    "${PARAMS[@]}" \
    --source "$SOURCE" \
    --duration "$DURATION" \
    --description "$DESC" \
    </dev/null
