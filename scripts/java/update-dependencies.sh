#!/usr/bin/env bash
# Refresh the Maven pin files (@maven, @lucene_fork, @protobuf_maven) and
# regenerate bazel/java/generated_pom/pom.xml (used by SNYK and Dependabot to
# scan for vulnerabilities).
#
# rules_jvm_external's coursier needs a JRE on disk to execute. To stay
# hermetic — and to match the JDK used everywhere else in the build — set
# JAVA_HOME from the Bazel-managed Adoptium 21 toolchain fetched via
# bazel/java/extensions.bzl. The host-platform Adoptium repo is selected
# automatically below.
set -eu

JAVA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${JAVA_DIR}/../vars.sh"

cd "${MONGOT_PATH}"

# Detect host platform → Adoptium JDK repo name.
case "$(uname -s)" in
    Darwin) host_os="macos"; jdk_subpath="/Contents/Home" ;;
    Linux)  host_os="linux"; jdk_subpath="" ;;
    *) echo "Unsupported OS: $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
    arm64|aarch64) host_arch="aarch64" ;;
    x86_64)        host_arch="x86_64" ;;
    *) echo "Unsupported arch: $(uname -m)" >&2; exit 1 ;;
esac
adoptium_repo="adoptium_jdk_${host_os}_${host_arch}"

# Ensure the Adoptium JDK is fetched, then point JAVA_HOME at it. Under bzlmod
# the fetched repo lives at <output_base>/external/+java_repos+<name>/.
bazel fetch "@${adoptium_repo}//:srcs"
JAVA_HOME="$(bazel info output_base)/external/+java_repos+${adoptium_repo}${jdk_subpath}"
export JAVA_HOME

bazel run @lucene_fork//:pin
bazel run @maven//:pin
bazel run @protobuf_maven//:pin

"${JAVA_DIR}/update-pom-file.sh"
