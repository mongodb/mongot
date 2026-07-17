"""netty-tcnative configuration for Atlas builds.

This file is swapped with netty_tcnative-Community.bzl by copybara when publishing
to the open source repository. See scripts/copybara/exclusions.yaml.
"""

NETTY_TCNATIVE_VERSION = "2.0.80.Final"

NETTY_TCNATIVE_AARCH64_TARGET = "@maven//:io_netty_netty_tcnative_linux_aarch_64_fedora"
NETTY_TCNATIVE_X86_64_TARGET = "@maven//:io_netty_netty_tcnative_linux_x86_64_fedora"

def netty_tcnative_deps():
    """Fetches netty-tcnative dependencies. No-op since 2.0.80.Final uses standard Maven JARs."""
    pass
