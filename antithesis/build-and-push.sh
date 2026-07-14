#!/usr/bin/env bash
#
# build-and-push.sh — build the Antithesis raft harness images (via build.sh)
# and push them to the ant-presearch dev-tenant registry.
#
# The build itself lives in build.sh — run that alone to compile-check locally
# without touching the registry. This script builds and then pushes.
#
# Tags everything :latest — the launch notebook references images by name and
# pulls latest; no SHA pinning. Pass a tag as the first argument to override
# (e.g. `build-and-push.sh verbose` tags and pushes everything :verbose for an
# experiment branch).

set -euo pipefail

REGISTRY="${ANTITHESIS_REGISTRY:-us-central1-docker.pkg.dev/molten-verve-216720/ant-presearch-repository}"
BUILD_TAG="${1:-latest}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Prefer podman; fall back to docker.
if command -v podman >/dev/null 2>&1; then
    ENGINE="podman"
elif command -v docker >/dev/null 2>&1; then
    ENGINE="docker"
else
    echo "build-and-push.sh: neither podman nor docker found in PATH" >&2
    exit 1
fi

# Build all four images first (forward the tag arg; build.sh inherits REGISTRY from env).
"${SCRIPT_DIR}/build.sh" "${BUILD_TAG}"

# Then push them. Image names are deterministic from REGISTRY/BUILD_TAG.
for image in hashicorp-raft-node hashicorp-raft-client hashicorp-raft-monitor hashicorp-raft-config; do
    echo "==> Pushing ${image}:${BUILD_TAG}"
    ${ENGINE} push "${REGISTRY}/${image}:${BUILD_TAG}"
done

echo
echo "Done. Four images pushed:"
echo "  ${REGISTRY}/hashicorp-raft-node:${BUILD_TAG}"
echo "  ${REGISTRY}/hashicorp-raft-client:${BUILD_TAG}"
echo "  ${REGISTRY}/hashicorp-raft-monitor:${BUILD_TAG}"
echo "  ${REGISTRY}/hashicorp-raft-config:${BUILD_TAG}"
