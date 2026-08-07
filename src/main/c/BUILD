load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

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

# Exercises the SIGILL-safe execution probe (sim_probe_i8) directly, without going through
# the JVM: init probes in forked children (parent handler untouched), plus the test-only
# in-process forced-fault path. Compiles mongot_vecsim.c with MONGOT_VECSIM_TEST defined.
cc_test(
    name = "mongot_vecsim_probe_test",
    srcs = [
        "mongot_vecsim.c",
        "mongot_vecsim_probe_test.c",
    ],
    copts = [
        "-DMONGOT_VECSIM_TEST",
    ] + select({
        "//bazel/platforms:is_linux_aarch64": ["-march=armv8.2-a+dotprod+sve"],
        "//bazel/platforms:is_macos_aarch64": ["-march=armv8.2-a+dotprod"],
        "//conditions:default": [],
    }),
    tags = ["unit"],
    # No CC toolchain on macOS x86_64; matches the mongot_vecsim cc_binary above.
    target_compatible_with = select({
        "//bazel/platforms:is_macos_x86_64": ["@platforms//:incompatible"],
        "//conditions:default": [],
    }),
)
