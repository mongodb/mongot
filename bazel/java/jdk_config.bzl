load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@rules_java//toolchains:remote_java_repository.bzl", "remote_java_repository")

def adoptium_jdk():
    _adoptium_jdk_linux_x86_64()
    _adoptium_jdk_linux_x86_64_repo()

    _adoptium_jdk_linux_aarch64()
    _adoptium_jdk_linux_aarch64_repo()

    _adoptium_jdk_macos_x86_64()
    _adoptium_jdk_macos_x86_64_repo()

    _adoptium_jdk_macos_aarch64()
    _adoptium_jdk_macos_aarch64_repo()

def adoptium_jdk_community():
    """Register JDK 21.0.13+9 under _community_ repo names.

    In the public repo there is only one JDK version, so the _community_ repos are
    identical to the standard ones. They must still be registered because deploy/BUILD
    references @adoptium_jdk_community_* for community bundle targets.

    Only http_archive targets are registered here (not remote_java_repository) because
    the community JDK is used solely for bundling into deployment artifacts, not as a
    Java compilation toolchain. Registering remote_java_repository would add a second
    toolchain with the same prefix/version as the standard one, making toolchain
    resolution non-deterministic.
    """
    _adoptium_jdk_community_linux_x86_64()
    _adoptium_jdk_community_linux_aarch64()
    _adoptium_jdk_community_macos_x86_64()
    _adoptium_jdk_community_macos_aarch64()

def _adoptium_jdk_linux_x86_64():
    http_archive(
        name = "adoptium_jdk_linux_x86_64",
        build_file = Label("//bazel/java:jdk_linux.BUILD"),
        sha256 = "d7f5d5cf865a2902b70ff549b97586a7d3f234e016f041ab447ea740c7d66d1c",
        strip_prefix = "jdk-21.0.13+9",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_x64_linux_hotspot_21.0.13_9.tar.gz"],
    )

def _adoptium_jdk_linux_x86_64_repo():
    remote_java_repository(
        name = "adoptium_jdk_linux_x86_64_repo",
        strip_prefix = "jdk-21.0.13+9",
        sha256 = "d7f5d5cf865a2902b70ff549b97586a7d3f234e016f041ab447ea740c7d66d1c",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_x64_linux_hotspot_21.0.13_9.tar.gz"],
        target_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:x86_64",
        ],
        prefix = "adoptium",
        version = "21",
    )

def _adoptium_jdk_linux_aarch64():
    http_archive(
        name = "adoptium_jdk_linux_aarch64",
        build_file = Label("//bazel/java:jdk_linux.BUILD"),
        sha256 = "e3522e707fc7dbe2a72c30746b1e7068301a0213ecd213d4491f35a14eb55578",
        strip_prefix = "jdk-21.0.13+9",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_aarch64_linux_hotspot_21.0.13_9.tar.gz"],
    )

def _adoptium_jdk_linux_aarch64_repo():
    remote_java_repository(
        name = "adoptium_jdk_linux_aarch64_repo",
        strip_prefix = "jdk-21.0.13+9",
        sha256 = "e3522e707fc7dbe2a72c30746b1e7068301a0213ecd213d4491f35a14eb55578",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_aarch64_linux_hotspot_21.0.13_9.tar.gz"],
        target_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:aarch64",
        ],
        prefix = "adoptium",
        version = "21",
    )

def _adoptium_jdk_macos_x86_64():
    http_archive(
        name = "adoptium_jdk_macos_x86_64",
        build_file = Label("//bazel/java:jdk_macos.BUILD"),
        sha256 = "9aa1d1c17ae5467a022679e7e0af5c00797d94ac462ed1a46f4db07a8a34a6dd",
        strip_prefix = "jdk-21.0.13+9",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_x64_mac_hotspot_21.0.13_9.tar.gz"],
    )

def _adoptium_jdk_macos_x86_64_repo():
    remote_java_repository(
        name = "adoptium_jdk_macos_x86_64_repo",
        sha256 = "9aa1d1c17ae5467a022679e7e0af5c00797d94ac462ed1a46f4db07a8a34a6dd",
        strip_prefix = "jdk-21.0.13+9/Contents/Home",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_x64_mac_hotspot_21.0.13_9.tar.gz"],
        target_compatible_with = [
            "@platforms//os:macos",
            "@platforms//cpu:x86_64",
        ],
        prefix = "adoptium",
        version = "21",
    )

def _adoptium_jdk_macos_aarch64():
    http_archive(
        name = "adoptium_jdk_macos_aarch64",
        build_file = Label("//bazel/java:jdk_macos.BUILD"),
        sha256 = "bf036b35b071bfb2f54f2c0af8ee8d88afa95ee6eb3dce02c0f94cfc2524d04a",
        strip_prefix = "jdk-21.0.13+9",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_aarch64_mac_hotspot_21.0.13_9.tar.gz"],
    )

def _adoptium_jdk_macos_aarch64_repo():
    remote_java_repository(
        name = "adoptium_jdk_macos_aarch64_repo",
        sha256 = "bf036b35b071bfb2f54f2c0af8ee8d88afa95ee6eb3dce02c0f94cfc2524d04a",
        strip_prefix = "jdk-21.0.13+9/Contents/Home",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_aarch64_mac_hotspot_21.0.13_9.tar.gz"],
        target_compatible_with = [
            "@platforms//os:macos",
            "@platforms//cpu:aarch64",
        ],
        prefix = "adoptium",
        version = "21",
    )

def _adoptium_jdk_community_linux_x86_64():
    http_archive(
        name = "adoptium_jdk_community_linux_x86_64",
        build_file = Label("//bazel/java:jdk_linux.BUILD"),
        sha256 = "d7f5d5cf865a2902b70ff549b97586a7d3f234e016f041ab447ea740c7d66d1c",
        strip_prefix = "jdk-21.0.13+9",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_x64_linux_hotspot_21.0.13_9.tar.gz"],
    )

def _adoptium_jdk_community_linux_aarch64():
    http_archive(
        name = "adoptium_jdk_community_linux_aarch64",
        build_file = Label("//bazel/java:jdk_linux.BUILD"),
        sha256 = "e3522e707fc7dbe2a72c30746b1e7068301a0213ecd213d4491f35a14eb55578",
        strip_prefix = "jdk-21.0.13+9",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_aarch64_linux_hotspot_21.0.13_9.tar.gz"],
    )

def _adoptium_jdk_community_macos_x86_64():
    http_archive(
        name = "adoptium_jdk_community_macos_x86_64",
        build_file = Label("//bazel/java:jdk_macos.BUILD"),
        sha256 = "9aa1d1c17ae5467a022679e7e0af5c00797d94ac462ed1a46f4db07a8a34a6dd",
        strip_prefix = "jdk-21.0.13+9",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_x64_mac_hotspot_21.0.13_9.tar.gz"],
    )

def _adoptium_jdk_community_macos_aarch64():
    http_archive(
        name = "adoptium_jdk_community_macos_aarch64",
        build_file = Label("//bazel/java:jdk_macos.BUILD"),
        sha256 = "bf036b35b071bfb2f54f2c0af8ee8d88afa95ee6eb3dce02c0f94cfc2524d04a",
        strip_prefix = "jdk-21.0.13+9",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_aarch64_mac_hotspot_21.0.13_9.tar.gz"],
    )
