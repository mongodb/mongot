load("@rules_cc//cc:cc_test.bzl", "cc_test")
load(
    "//src/main/c:vecsim.bzl",
    "VECSIM_ACCEL_DEFINES",
    "VECSIM_ACCEL_KERNEL_DEPS",
    "VECSIM_BASE_COPTS",
    "VECSIM_NOT_MACOS_X86_64",
)

# Exercises the int8 similarity kernels directly (against the scalar reference)
# and via the exported dispatch API.  Accelerated kernels are only run when the
# host CPU supports them, so this is safe to run on any CI machine.
cc_test(
    name = "vecsim_test",
    size = "small",
    srcs = ["vecsim_test.c"],
    copts = VECSIM_BASE_COPTS,
    # Must match the dispatcher so the test's #if guards agree with what links.
    local_defines = VECSIM_ACCEL_DEFINES,
    tags = ["unit"],
    target_compatible_with = VECSIM_NOT_MACOS_X86_64,
    deps = [
        "//src/main/c:vecsim_dispatch",
        "//src/main/c:vecsim_internal_hdrs",
        "//src/main/c:vecsim_scalar",
    ] + VECSIM_ACCEL_KERNEL_DEPS,
)
