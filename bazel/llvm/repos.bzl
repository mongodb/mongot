load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Using 0.10.3 for WORKSPACE compatibility (TODO update once we migrate to bzlmod)
TOOLCHAINS_LLVM_VERSION = "0.10.3"

def llvm_repos():
    http_archive(
        name = "toolchains_llvm",
        sha256 = "b7cd301ef7b0ece28d20d3e778697a5e3b81828393150bed04838c0c52963a01",
        strip_prefix = "toolchains_llvm-{version}".format(version = TOOLCHAINS_LLVM_VERSION),
        canonical_id = TOOLCHAINS_LLVM_VERSION,
        url = "https://github.com/grailbio/bazel-toolchain/releases/download/{version}/toolchains_llvm-{version}.tar.gz".format(version = TOOLCHAINS_LLVM_VERSION),
    )
