"""netty-tcnative configuration for Community builds.

This file replaces netty_tcnative.bzl in the open source repository.
Community builds use only the standard Maven artifacts.

In bzlmod, the Maven artifact version is controlled by
maven_search_systems-Community.MODULE.bazel (swapped to
maven_search_systems.MODULE.bazel in OSS). NETTY_TCNATIVE_VERSION and
NETTY_TCNATIVE_MAVEN_ARTIFACTS below are vestigial — they are not loaded by
anything since systems_deps.bzl was removed in the bzlmod migration. This file
is kept for the Bazel target label constants and the netty_tcnative_deps() stub.
"""

# 2.0.79.Final is the first release where the linux-aarch_64-fedora artifact
# links against OpenSSL 3.x (matching the x86_64-fedora behavior), enabling
# FIPS compliance on AL2023 aarch64 where OpenSSL 3.x is installed.
NETTY_TCNATIVE_VERSION = "2.0.79.Final"

# Unused in bzlmod — artifact registration moved to maven_search_systems-Community.MODULE.bazel.
NETTY_TCNATIVE_MAVEN_ARTIFACTS = [
    "io.netty:netty-tcnative-boringssl-static:{}".format(NETTY_TCNATIVE_VERSION),
    "io.netty:netty-tcnative-classes:{}".format(NETTY_TCNATIVE_VERSION),
    "io.netty:netty-tcnative:jar:linux-aarch_64-fedora:{}".format(NETTY_TCNATIVE_VERSION),
    "io.netty:netty-tcnative:jar:linux-x86_64-fedora:{}".format(NETTY_TCNATIVE_VERSION),
]

NETTY_TCNATIVE_AARCH64_TARGET = "@maven//:io_netty_netty_tcnative_linux_aarch_64_fedora"

# AL2 and AL2023 targets point to same JAR to maintain compatibility with Atlas build configuration.
# In AL2 dynamic linking to OpenSSL will fail since it does not have OpenSSL3.x
NETTY_TCNATIVE_X86_64_AL2_TARGET = "@maven//:io_netty_netty_tcnative_linux_x86_64_fedora"
NETTY_TCNATIVE_X86_64_AL2023_TARGET = "@maven//:io_netty_netty_tcnative_linux_x86_64_fedora"

# Legacy alias for runtime dependency resolution
NETTY_TCNATIVE_X86_64_TARGET = "@maven//:io_netty_netty_tcnative_linux_x86_64_fedora"

def netty_tcnative_deps():
    """No-op for community builds.
    """
    pass
