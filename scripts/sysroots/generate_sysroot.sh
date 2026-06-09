#!/usr/bin/env bash
# generate_sysroot.sh — generates a cross-compilation sysroot from a Docker image.
#
# Produces a .tar.gz suitable for uploading to S3 and referencing via http_archive,
# with absolute symlinks corrected to relative so the linker resolves them against
# the sysroot root rather than the host filesystem.
#
# Usage:
#   ./scripts/sysroots/generate_sysroot.sh [OPTIONS]
#
# Options:
#   --distro DISTRO       Linux distribution Docker image name (default: ubuntu)
#   --version VERSION     Distribution version tag (default: 18.04)
#   --arch ARCH           Target architecture: x86_64 or aarch64 (default: x86_64)
#   --output-tar PATH     Output tarball path (default: sysroots/<name>.tar.gz)
#   --packages PKGS       Space-separated extra packages to install alongside the defaults
#   -h, --help            Show this message and exit
#
# Supported distros and their default packages:
#   ubuntu / debian  (apt)   libc6-dev libstdc++-7-dev linux-libc-dev
#                            Note: libstdc++ version is Ubuntu 18.04 specific.
#                                  For 20.04 use --packages libstdc++-9-dev, etc.
#   amazonlinux / centos / rhel / fedora  (yum)  glibc-devel libstdc++-devel kernel-headers
#
# Examples:
#   # Ubuntu 20.04 aarch64.
#   ./scripts/sysroots/generate_sysroot.sh --version 20.04 --arch aarch64 \
#       --output-tar /tmp/ubuntu2004-aarch64.tar.gz
#
#   # Amazon Linux 2 x86_64 (glibc 2.26).
#   ./scripts/sysroots/generate_sysroot.sh --distro amazonlinux --version 2 \
#       --output-tar /tmp/amazonlinux2-x86_64.tar.gz

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# --- Defaults ---
DISTRO="ubuntu"
VERSION="18.04"
ARCH="x86_64"
OUTPUT_TAR=""
EXTRA_PACKAGES=""

# --- Argument parsing ---
usage() {
    grep '^#' "$0" | sed 's/^# \{0,1\}//' | awk '/^Usage:/{ p=1 } p && /^---/{ exit } p'
    exit 0
}

die() {
    echo "error: $*" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --distro)      DISTRO="$2";         shift 2 ;;
        --version)     VERSION="$2";        shift 2 ;;
        --arch)        ARCH="$2";           shift 2 ;;
        --output-tar)  OUTPUT_TAR="$2";     shift 2 ;;
        --packages)    EXTRA_PACKAGES="$2"; shift 2 ;;
        -h|--help)     usage ;;
        *) die "unknown option: $1" ;;
    esac
done

# --- Resolve architecture-specific values ---
case "$ARCH" in
    x86_64)  DOCKER_PLATFORM="linux/amd64" ;;
    aarch64) DOCKER_PLATFORM="linux/arm64" ;;
    *) die "unsupported architecture: ${ARCH} (supported: x86_64, aarch64)" ;;
esac

# --- Resolve distro-specific values ---
# Sets PKG_MANAGER and DEFAULT_PACKAGES based on the distro family.
case "$DISTRO" in
    ubuntu|debian)
        PKG_MANAGER="apt"
        # Provides: glibc headers + startup objects (libc6-dev), C++ stdlib
        # (libstdc++-7-dev), and kernel headers (linux-libc-dev).
        # libstdc++ version is Ubuntu 18.04 specific; use --packages to override
        # for other versions (e.g. libstdc++-9-dev for Ubuntu 20.04).
        DEFAULT_PACKAGES="libc6-dev libstdc++-7-dev linux-libc-dev"
        ;;
    amazonlinux|centos|rhel|fedora)
        PKG_MANAGER="yum"
        # Provides: glibc headers + startup objects (glibc-devel), C++ stdlib
        # (libstdc++-devel), and kernel headers (kernel-headers).
        DEFAULT_PACKAGES="glibc-devel libstdc++-devel kernel-headers"
        ;;
    *)
        die "unsupported distro: ${DISTRO} (supported: ubuntu, debian, amazonlinux, centos, rhel, fedora)"
        ;;
esac

VERSION_NODOT="${VERSION//./}"
SYSROOT_NAME="${DISTRO}${VERSION_NODOT}-${ARCH}"
DOCKER_IMAGE="${DISTRO}:${VERSION}"
PACKAGES="${DEFAULT_PACKAGES}${EXTRA_PACKAGES:+ ${EXTRA_PACKAGES}}"

if [[ -z "$OUTPUT_TAR" ]]; then
    OUTPUT_TAR="${REPO_ROOT}/sysroots/${SYSROOT_NAME}.tar.gz"
fi

# --- Preflight checks ---
command -v docker  >/dev/null 2>&1 || die "docker is required but not found"
command -v python3 >/dev/null 2>&1 || die "python3 is required but not found"

echo "Generating sysroot: ${SYSROOT_NAME}"
echo "  Docker image:  ${DOCKER_IMAGE} (${DOCKER_PLATFORM})"
echo "  Package mgr:   ${PKG_MANAGER}"
echo "  Output:        ${OUTPUT_TAR}"
echo "  Packages:      ${PACKAGES}"
echo

# --- Setup ---
STAGING_DIR="$(mktemp -d /tmp/sysroot-staging-XXXXXX)"
SYSROOT_DIR="${STAGING_DIR}/${SYSROOT_NAME}"
mkdir -p "${SYSROOT_DIR}"

CONTAINER_NAME="sysroot-build-${SYSROOT_NAME}-$$"
EXPORT_TAR="$(mktemp /tmp/sysroot-export-XXXXXX.tar)"

cleanup() {
    docker rm -f "${CONTAINER_NAME}" 2>/dev/null || true
    rm -f "${EXPORT_TAR}"
    rm -rf "${STAGING_DIR}"
}
trap cleanup EXIT

# --- Build ---
echo "==> Pulling ${DOCKER_IMAGE} for ${DOCKER_PLATFORM}..."
docker pull --platform "${DOCKER_PLATFORM}" "${DOCKER_IMAGE}"

echo "==> Installing packages..."
case "$PKG_MANAGER" in
    apt)
        docker run \
            --name "${CONTAINER_NAME}" \
            --platform "${DOCKER_PLATFORM}" \
            "${DOCKER_IMAGE}" \
            bash -c "
                set -e
                apt-get update -qq
                DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ${PACKAGES}
                apt-get clean
                rm -rf /var/lib/apt/lists/*
            "
        ;;
    yum)
        docker run \
            --name "${CONTAINER_NAME}" \
            --platform "${DOCKER_PLATFORM}" \
            "${DOCKER_IMAGE}" \
            bash -c "
                set -e
                yum install -y ${PACKAGES}
                yum clean all
                rm -rf /var/cache/yum
            "
        ;;
esac

echo "==> Exporting filesystem..."
docker export "${CONTAINER_NAME}" -o "${EXPORT_TAR}"

echo "==> Extracting sysroot directories..."
# docker export produces a flat tar with paths relative to / (no leading slash).
# We extract only the directories needed for linking; the rest (binaries, docs,
# etc.) are not useful in a sysroot and would bloat the output.
# usr/lib64 is required for RHEL-family distros (AL2, CentOS, etc.) which place
# libstdc++ and other runtime libs there rather than in usr/lib.
for dir in lib lib64 usr/lib usr/lib64 usr/include; do
    tar -xf "${EXPORT_TAR}" -C "${SYSROOT_DIR}" "${dir}" 2>/dev/null || true
done

# Docker-exported tars preserve container permissions (e.g. r-xr-xr-x directories).
# Ownership falls back to the current user since we're not root, but the restrictive
# permission bits remain and would cause the symlink-fix step and cleanup to fail
# with "permission denied". Make everything user-writable before proceeding.
chmod -R u+rwX "${SYSROOT_DIR}"

echo "==> Fixing absolute symlinks..."
# Docker exports preserve absolute symlinks (e.g. /lib/x86_64-linux-gnu/libc.so.6).
# The linker resolves these against the host root, not the sysroot, so they must
# be made relative before the sysroot is usable for cross-compilation.
python3 - "${SYSROOT_DIR}" <<'PYEOF'
import os
import sys

sysroot = os.path.abspath(sys.argv[1])
fixed = 0

for dirpath, dirnames, filenames in os.walk(sysroot, followlinks=False):
    for name in filenames + dirnames:
        path = os.path.join(dirpath, name)
        if not os.path.islink(path):
            continue
        target = os.readlink(path)
        if not os.path.isabs(target):
            continue
        new_target = os.path.relpath(
            os.path.join(sysroot, target.lstrip("/")),
            dirpath,
        )
        os.unlink(path)
        os.symlink(new_target, path)
        fixed += 1

print(f"  Fixed {fixed} absolute symlink(s).")
PYEOF

echo "==> Writing Bazel BUILD file..."

# The archive is consumed via http_archive in bazel/cc/extensions.bzl, which
# under bzlmod synthesizes the necessary repo metadata. Only the BUILD file
# inside the archive needs to exist; no WORKSPACE or MODULE.bazel is required.
cat > "${SYSROOT_DIR}/BUILD" <<'EOF'
# Auto-generated by scripts/sysroots/generate_sysroot.sh — do not edit by hand.
filegroup(
    name = "sysroot",
    srcs = glob(
        ["**"],
        exclude = [
            "BUILD",
            "BUILD.bazel",
        ],
    ),
    visibility = ["//visibility:public"],
)
EOF

echo "==> Creating tarball..."
mkdir -p "$(dirname "${OUTPUT_TAR}")"
# The tarball contains a single top-level directory named after the sysroot so that
# http_archive can use strip_prefix = "<sysroot_name>" to unpack it correctly.
tar -czf "${OUTPUT_TAR}" -C "${STAGING_DIR}" "${SYSROOT_NAME}"

echo "==> Computing checksum..."
INTEGRITY="$(python3 - "${OUTPUT_TAR}" <<'PYEOF'
import hashlib, base64, sys
digest = hashlib.sha256(open(sys.argv[1], "rb").read()).digest()
print("sha256-" + base64.b64encode(digest).decode())
PYEOF
)"

SIZE="$(du -sh "${OUTPUT_TAR}" | cut -f1)"

echo
echo "Done. Tarball written to: ${OUTPUT_TAR} (${SIZE})"
echo "  integrity: ${INTEGRITY}"
echo
echo "Sample http_archive entry for bazel/cc/extensions.bzl (update 'urls' after uploading to S3):"
cat <<EOF

http_archive(
    name = "sysroot_${DISTRO}${VERSION_NODOT}_${ARCH}",
    integrity = "${INTEGRITY}",
    strip_prefix = "${SYSROOT_NAME}",
    urls = [
        # TODO: upload ${OUTPUT_TAR} to S3 and set the URL here
        # "https://search-build-and-release-3p-tools.s3.us-east-2.amazonaws.com/sysroots/${SYSROOT_NAME}.tar.gz",
    ],
)
EOF
