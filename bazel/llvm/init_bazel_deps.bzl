def llvm_init_bazel_deps():
    native.register_toolchains("@llvm_toolchain//:all")
