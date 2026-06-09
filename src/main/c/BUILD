load("@rules_cc//cc:cc_binary.bzl", "cc_binary")

cc_binary(
    name = "mongot_vecsim",
    srcs = ["mongot_vecsim.c"],
    copts = [
        "-O3",
        "-DNDEBUG",
    ] + select({
        "//bazel/platforms:is_linux_aarch64": ["-march=armv8.2-a+dotprod+sve"],
        "//bazel/platforms:is_macos_aarch64": ["-march=armv8.2-a+dotprod"],
        "//conditions:default": [],
    }),
    linkshared = True,
    linkstatic = False,
    # No CC toolchain on macOS x86_64; dependents must select() this out.
    target_compatible_with = select({
        "//bazel/platforms:is_macos_x86_64": ["@platforms//:incompatible"],
        "//conditions:default": [],
    }),
    visibility = ["//visibility:public"],
)
