#!/usr/bin/env bash
# Regenerates bazel/java/maven_pin_info-community.json.
#
# Community builds use a different netty-tcnative version than Atlas builds (CLOUDP-413451).
# Because maven_search_systems.MODULE.bazel holds the Atlas version, this script temporarily
# swaps in maven_search_systems-Community.MODULE.bazel, runs the Maven pin, copies the result
# to maven_pin_info-community.json, then restores both files.
#
# TODO(CLOUDP-413451): Remove script once Atlas and community use the same netty-tcnative version.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../vars.sh"

SYSTEMS_BZL="${MONGOT_PATH}/bazel/java/maven_search_systems.MODULE.bazel"
COMMUNITY_SYSTEMS_BZL="${MONGOT_PATH}/bazel/java/maven_search_systems-Community.MODULE.bazel"
ATLAS_PIN="${MONGOT_PATH}/bazel/java/maven_pin_info.json"
COMMUNITY_PIN="${MONGOT_PATH}/bazel/java/maven_pin_info-community.json"

SYSTEMS_BZL_BACKUP="$(mktemp)"
ATLAS_PIN_BACKUP="$(mktemp)"

cleanup() {
    cp "${SYSTEMS_BZL_BACKUP}" "${SYSTEMS_BZL}"
    cp "${ATLAS_PIN_BACKUP}" "${ATLAS_PIN}"
    rm -f "${SYSTEMS_BZL_BACKUP}" "${ATLAS_PIN_BACKUP}"
}
trap cleanup EXIT

cp "${SYSTEMS_BZL}" "${SYSTEMS_BZL_BACKUP}"
cp "${ATLAS_PIN}" "${ATLAS_PIN_BACKUP}"
cp "${COMMUNITY_SYSTEMS_BZL}" "${SYSTEMS_BZL}"

cd "${MONGOT_PATH}"
"${MONGOT_PATH}/scripts/tools/bazelisk/run.sh" run @maven//:pin

cp "${ATLAS_PIN}" "${COMMUNITY_PIN}"

echo "Community pin file updated: bazel/java/maven_pin_info-community.json"
