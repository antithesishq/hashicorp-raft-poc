#!/usr/bin/env bash
#
# build.sh — build the four Antithesis raft harness images locally. Does NOT
# push; use `docker push` or equivalent to push to the registry.
#
# Building without pushing lets you compile-check the SUT before touching the
# registry.
#
# Tags everything :latest — the launch notebook references images by name and
# pulls latest; no SHA pinning. Pass a tag as the first argument to override
# (e.g. `build.sh verbose` tags everything :verbose for an experiment branch).

set -euo pipefail

REGISTRY="${REGISTRY:-us-central1-docker.pkg.dev/molten-verve-216720/ant-presearch-repository}"
BUILD_TAG="${1:-latest}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG_DIR="${SCRIPT_DIR}/config"

# Prefer podman; fall back to docker.
if command -v podman >/dev/null 2>&1; then
    ENGINE="podman"
elif command -v docker >/dev/null 2>&1; then
    ENGINE="docker"
else
    echo "build.sh: neither podman nor docker found in PATH" >&2
    exit 1
fi

echo "build.sh: using engine=${ENGINE}, registry=${REGISTRY}, tag=${BUILD_TAG}"

# ---------- hashicorp-raft-node ----------
echo "==> Building hashicorp-raft-node"
${ENGINE} build \
    --platform linux/amd64 \
    -f "${CONFIG_DIR}/Dockerfile.raft-node" \
    -t "${REGISTRY}/hashicorp-raft-node:${BUILD_TAG}" \
    "${REPO_ROOT}"

# ---------- hashicorp-raft-client ----------
# Build context is the self-contained antithesis/raft-client/ module — the
# harness shares no Go code with the SUT, so it doesn't need the upstream
# raft source tree.
echo "==> Building hashicorp-raft-client"
${ENGINE} build \
    --platform linux/amd64 \
    -f "${CONFIG_DIR}/Dockerfile.raft-client" \
    -t "${REGISTRY}/hashicorp-raft-client:${BUILD_TAG}" \
    "${SCRIPT_DIR}/raft-client"

# ---------- hashicorp-raft-monitor ----------
# Build context is antithesis/raft-monitor/ itself — the sidecar is a
# self-contained Python module.
echo "==> Building hashicorp-raft-monitor"
${ENGINE} build \
    --platform linux/amd64 \
    -f "${CONFIG_DIR}/Dockerfile.raft-monitor" \
    -t "${REGISTRY}/hashicorp-raft-monitor:${BUILD_TAG}" \
    "${SCRIPT_DIR}/raft-monitor"

# ---------- hashicorp-raft-config ----------
# Tiny image carrying docker-compose.yaml + a .env file that pins ${BUILD_TAG}
# for the compose loader. Antithesis pulls this and reads the compose to know
# how to bring up the SUT. Build context = CONFIG_DIR so the COPY works
# without going up the tree.
echo "==> Building hashicorp-raft-config"
echo "BUILD_TAG=${BUILD_TAG}" > "${CONFIG_DIR}/.env"
trap 'rm -f "${CONFIG_DIR}/.env"' EXIT
${ENGINE} build \
    --platform linux/amd64 \
    -f "${CONFIG_DIR}/Dockerfile.raft-config" \
    -t "${REGISTRY}/hashicorp-raft-config:${BUILD_TAG}" \
    "${CONFIG_DIR}"

echo
echo "Done. Four images built:"
echo "  ${REGISTRY}/hashicorp-raft-node:${BUILD_TAG}"
echo "  ${REGISTRY}/hashicorp-raft-client:${BUILD_TAG}"
echo "  ${REGISTRY}/hashicorp-raft-monitor:${BUILD_TAG}"
echo "  ${REGISTRY}/hashicorp-raft-config:${BUILD_TAG}"
