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
    """Register JDK 21.0.11+10 under _community_ repo names.

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
        sha256 = "4b2220e232a97997b436ca6ab15cbf70171ecff52958a46159dfa5a8c44ca4de",
        strip_prefix = "jdk-21.0.11+10",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.11%2B10/OpenJDK21U-jdk_x64_linux_hotspot_21.0.11_10.tar.gz"],
    )

def _adoptium_jdk_linux_x86_64_repo():
    remote_java_repository(
        name = "adoptium_jdk_linux_x86_64_repo",
        strip_prefix = "jdk-21.0.11+10",
        sha256 = "4b2220e232a97997b436ca6ab15cbf70171ecff52958a46159dfa5a8c44ca4de",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.11%2B10/OpenJDK21U-jdk_x64_linux_hotspot_21.0.11_10.tar.gz"],
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
        sha256 = "8d498ec88e1c1989fab95c6784240ab92d011e29c54d20a3f9c324b13476f9ad",
        strip_prefix = "jdk-21.0.11+10",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.11%2B10/OpenJDK21U-jdk_aarch64_linux_hotspot_21.0.11_10.tar.gz"],
    )

def _adoptium_jdk_linux_aarch64_repo():
    remote_java_repository(
        name = "adoptium_jdk_linux_aarch64_repo",
        strip_prefix = "jdk-21.0.11+10",
        sha256 = "8d498ec88e1c1989fab95c6784240ab92d011e29c54d20a3f9c324b13476f9ad",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.11%2B10/OpenJDK21U-jdk_aarch64_linux_hotspot_21.0.11_10.tar.gz"],
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
        sha256 = "34180eb03e6d207c388cce3da668f6cc7cd7508c185c24782fadac2c9c0e66f9",
        strip_prefix = "jdk-21.0.11+10",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.11%2B10/OpenJDK21U-jdk_x64_mac_hotspot_21.0.11_10.tar.gz"],
    )

def _adoptium_jdk_macos_x86_64_repo():
    remote_java_repository(
        name = "adoptium_jdk_macos_x86_64_repo",
        sha256 = "34180eb03e6d207c388cce3da668f6cc7cd7508c185c24782fadac2c9c0e66f9",
        strip_prefix = "jdk-21.0.11+10/Contents/Home",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.11%2B10/OpenJDK21U-jdk_x64_mac_hotspot_21.0.11_10.tar.gz"],
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
        sha256 = "6ebcf221c9b41507b14c098e93c6ead6440b8d9bd154f8ec666c4c73abbdb201",
        strip_prefix = "jdk-21.0.11+10",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.11%2B10/OpenJDK21U-jdk_aarch64_mac_hotspot_21.0.11_10.tar.gz"],
    )

def _adoptium_jdk_macos_aarch64_repo():
    remote_java_repository(
        name = "adoptium_jdk_macos_aarch64_repo",
        sha256 = "6ebcf221c9b41507b14c098e93c6ead6440b8d9bd154f8ec666c4c73abbdb201",
        strip_prefix = "jdk-21.0.11+10/Contents/Home",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.11%2B10/OpenJDK21U-jdk_aarch64_mac_hotspot_21.0.11_10.tar.gz"],
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
        sha256 = "4b2220e232a97997b436ca6ab15cbf70171ecff52958a46159dfa5a8c44ca4de",
        strip_prefix = "jdk-21.0.11+10",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.11%2B10/OpenJDK21U-jdk_x64_linux_hotspot_21.0.11_10.tar.gz"],
    )

def _adoptium_jdk_community_linux_aarch64():
    http_archive(
        name = "adoptium_jdk_community_linux_aarch64",
        build_file = Label("//bazel/java:jdk_linux.BUILD"),
        sha256 = "8d498ec88e1c1989fab95c6784240ab92d011e29c54d20a3f9c324b13476f9ad",
        strip_prefix = "jdk-21.0.11+10",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.11%2B10/OpenJDK21U-jdk_aarch64_linux_hotspot_21.0.11_10.tar.gz"],
    )

def _adoptium_jdk_community_macos_x86_64():
    http_archive(
        name = "adoptium_jdk_community_macos_x86_64",
        build_file = Label("//bazel/java:jdk_macos.BUILD"),
        sha256 = "34180eb03e6d207c388cce3da668f6cc7cd7508c185c24782fadac2c9c0e66f9",
        strip_prefix = "jdk-21.0.11+10",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.11%2B10/OpenJDK21U-jdk_x64_mac_hotspot_21.0.11_10.tar.gz"],
    )

def _adoptium_jdk_community_macos_aarch64():
    http_archive(
        name = "adoptium_jdk_community_macos_aarch64",
        build_file = Label("//bazel/java:jdk_macos.BUILD"),
        sha256 = "6ebcf221c9b41507b14c098e93c6ead6440b8d9bd154f8ec666c4c73abbdb201",
        strip_prefix = "jdk-21.0.11+10",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.11%2B10/OpenJDK21U-jdk_aarch64_mac_hotspot_21.0.11_10.tar.gz"],
    )
