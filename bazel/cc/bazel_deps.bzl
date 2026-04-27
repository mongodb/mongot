load("@toolchains_llvm//toolchain:deps.bzl", "bazel_toolchain_dependencies")
load("@toolchains_llvm//toolchain:rules.bzl", "llvm_toolchain")

def cc_bazel_deps():
    bazel_toolchain_dependencies()

    # Hermetic LLVM cc toolchains for cross compilation.
    # We use different versions for x86_64 and aarch64 based on build constraints.
    # - x86_64 must support ubuntu1804 builds and link against libtinfo.so.5
    # - aarch64 llvm 15 builds require GLIBCXX_3.4.29 which is not present in ubuntu2004
    #
    # Sysroots are generated from ubuntu docker images. See scripts/sysroots/generate_sysroot.sh
    llvm_toolchain(
        name = "llvm_toolchain",
        llvm_versions = {
            "": "17.0.6",
            "linux-x86_64": "15.0.6",
            "linux-aarch64": "17.0.6",
        },
        sysroot = {
            "linux-aarch64": "@sysroot_ubuntu1804_aarch64//:sysroot",
            "linux-x86_64": "@sysroot_ubuntu1804_x86_64//:sysroot",
        },
    )
