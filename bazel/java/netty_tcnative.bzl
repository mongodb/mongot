"""netty-tcnative configuration for Atlas builds.

This file is swapped with netty_tcnative-Community.bzl by copybara when publishing
to the open source repository. See scripts/copybara/exclusions.yaml.

For Atlas AL2023 x86_64 builds, we use an AL2023-compatible JAR built from source
that dynamically links to OpenSSL 3.x (required for FIPS compliance).
See scripts/netty-tcnative/README.md for details.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_file")

# Note that matching tcnative version for Netty 4.1.130.Final is 2.0.74.Final.
# However, the linux-aarch_64-fedora artifact has been removed for that version,
# hence using 2.0.71.Final.
NETTY_TCNATIVE_VERSION = "2.0.71.Final"

# SHA256 for the AL2023-compatible netty-tcnative JAR with dynamic OpenSSL linking.
# Built and updated using scripts/netty-tcnative/build-and-publish.sh which uploads to S3.
NETTY_TCNATIVE_AL2023_X86_64_SHA256 = "da321e9378fe60e9a3fdde5f832b0039c56237ef8cae429ba548b7f19654e223"

# Maven artifacts for maven_install() - used by both Atlas and Community builds
NETTY_TCNATIVE_MAVEN_ARTIFACTS = [
    "io.netty:netty-tcnative-boringssl-static:{}".format(NETTY_TCNATIVE_VERSION),
    "io.netty:netty-tcnative-classes:{}".format(NETTY_TCNATIVE_VERSION),
    "io.netty:netty-tcnative:jar:linux-aarch_64-fedora:{}".format(NETTY_TCNATIVE_VERSION),
    "io.netty:netty-tcnative:jar:linux-x86_64-fedora:{}".format(NETTY_TCNATIVE_VERSION),
]

# Target references for netty-tcnative JARs.
# Used by both extracted_libraries (root BUILD) and runtime dependencies.
# aarch64 always uses Maven.
NETTY_TCNATIVE_AARCH64_TARGET = "@maven//:io_netty_netty_tcnative_linux_aarch_64_fedora"

# x86_64 targets for dual-library bundling (single artifact for AL2 + AL2023).
# Both libraries are always extracted; runtime selection happens in Java (TcNativeLoader).
# AL2 (OpenSSL 1.x) - standard Maven fedora build
NETTY_TCNATIVE_X86_64_AL2_TARGET = "//bazel/atlas/java:netty-tcnative-al2-linux-x86_64"

# AL2023 (OpenSSL 3.x) - custom build from S3 with dynamic OpenSSL linking
NETTY_TCNATIVE_X86_64_AL2023_TARGET = "//bazel/atlas/java:netty-tcnative-openssl-dynamic-linux-x86_64"

# For runtime classpath resolution, use the AL2 target (Maven).
# The actual library selection happens at runtime via TcNativeLoader.
NETTY_TCNATIVE_X86_64_TARGET = "//bazel/atlas/java:netty-tcnative-al2-linux-x86_64"

def netty_tcnative_deps():
    """Fetch AL2023-compatible netty-tcnative JAR from S3.

    This is only used for Atlas AL2023 x86_64 builds. Community builds and other
    Atlas configurations use the Maven artifacts defined in NETTY_TCNATIVE_MAVEN_ARTIFACTS.
    """
    http_file(
        name = "netty_tcnative_openssl_dynamic_linux_x86_64",
        sha256 = NETTY_TCNATIVE_AL2023_X86_64_SHA256,
        downloaded_file_path = "netty-tcnative-{}-linux-x86_64.jar".format(NETTY_TCNATIVE_VERSION),
        urls = ["https://search-build-and-release-3p-tools.s3.us-east-2.amazonaws.com/netty-tcnative/netty-tcnative-{}-linux-x86_64.jar".format(NETTY_TCNATIVE_VERSION)],
    )
