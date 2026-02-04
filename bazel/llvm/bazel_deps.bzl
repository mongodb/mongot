load("@toolchains_llvm//toolchain:deps.bzl", "bazel_toolchain_dependencies")
load("@toolchains_llvm//toolchain:rules.bzl", "llvm_toolchain")

LLVM_VERSION = "16.0.6"

def llvm_bazel_deps():
    bazel_toolchain_dependencies()

    llvm_toolchain(
        name = "llvm_toolchain",
        llvm_version = LLVM_VERSION,
    )
