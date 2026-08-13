load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")
load(
    ":vecsim.bzl",
    "VECSIM_ACCEL_DEFINES",
    "VECSIM_ACCEL_KERNEL_DEPS",
    "VECSIM_BASE_COPTS",
    "VECSIM_NOT_MACOS_X86_64",
)

# CPU feature flags are scoped to the individual kernel libraries below so that
# the baseline scalar fallback and the dispatcher are never compiled with an ISA
# their host may not support.

# Visible to the unit test in addition to this package.
_VECSIM_VISIBILITY = ["//src/test/unit/c:__pkg__"]

cc_library(
    name = "vecsim_internal_hdrs",
    hdrs = [
        "vecsim_cpu.h",
        "vecsim_kernels.h",
    ],
    visibility = _VECSIM_VISIBILITY,
)

# --- Scalar fallback: baseline, always linked ---
cc_library(
    name = "vecsim_scalar",
    srcs = ["vecsim_scalar.c"],
    copts = VECSIM_BASE_COPTS,
    linkstatic = True,
    target_compatible_with = VECSIM_NOT_MACOS_X86_64,
    visibility = _VECSIM_VISIBILITY,
    deps = [":vecsim_internal_hdrs"],
)

# --- NEON dotprod kernels: aarch64 only ---
cc_library(
    name = "vecsim_neon_dotprod",
    srcs = ["vecsim_neon_dotprod.c"],
    copts = VECSIM_BASE_COPTS + ["-march=armv8.2-a+dotprod"],
    linkstatic = True,
    target_compatible_with = select({
        "//bazel/platforms:is_linux_aarch64": [],
        "//bazel/platforms:is_macos_aarch64": [],
        "//conditions:default": ["@platforms//:incompatible"],
    }),
    visibility = _VECSIM_VISIBILITY,
    deps = [":vecsim_internal_hdrs"],
)

# --- SVE kernels: linux aarch64 only ---
cc_library(
    name = "vecsim_sve",
    srcs = ["vecsim_sve.c"],
    copts = VECSIM_BASE_COPTS + ["-march=armv8.2-a+sve"],
    linkstatic = True,
    target_compatible_with = select({
        "//bazel/platforms:is_linux_aarch64": [],
        "//conditions:default": ["@platforms//:incompatible"],
    }),
    visibility = _VECSIM_VISIBILITY,
    deps = [":vecsim_internal_hdrs"],
)

# --- AVX-512 VNNI kernels: linux x86_64 only ---
cc_library(
    name = "vecsim_avx512",
    srcs = ["vecsim_avx512.c"],
    copts = VECSIM_BASE_COPTS + [
        "-mavx512f",
        "-mavx512bw",
        "-mavx512vnni",
    ],
    linkstatic = True,
    target_compatible_with = select({
        "//bazel/platforms:is_linux_x86_64": [],
        "//conditions:default": ["@platforms//:incompatible"],
    }),
    visibility = _VECSIM_VISIBILITY,
    deps = [":vecsim_internal_hdrs"],
)

# --- Dispatcher: baseline detection + runtime kernel selection ---
# alwayslink so the exported entry points and the load-time constructor survive
# into the shared object even though nothing in the .so references them directly.
cc_library(
    name = "vecsim_dispatch",
    srcs = ["mongot_vecsim.c"],
    copts = VECSIM_BASE_COPTS,
    linkstatic = True,
    # Tell the dispatcher which kernel libraries are linked in on this platform.
    local_defines = VECSIM_ACCEL_DEFINES,
    target_compatible_with = VECSIM_NOT_MACOS_X86_64,
    visibility = _VECSIM_VISIBILITY,
    deps = [
        ":vecsim_internal_hdrs",
        ":vecsim_scalar",
    ] + VECSIM_ACCEL_KERNEL_DEPS,
    alwayslink = True,
)

# --- Exported shared object ---
cc_binary(
    name = "mongot_vecsim",
    linkshared = True,
    linkstatic = False,
    # No CC toolchain on macOS x86_64; dependents must select() this out.
    target_compatible_with = VECSIM_NOT_MACOS_X86_64,
    visibility = ["//visibility:public"],
    deps = [":vecsim_dispatch"],
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
    copts = VECSIM_BASE_COPTS,
    # MONGOT_VECSIM_TEST compiles in the forced-fault hooks; the accel defines must match
    # the kernel libraries linked in below, exactly as for :vecsim_dispatch.
    local_defines = ["MONGOT_VECSIM_TEST"] + VECSIM_ACCEL_DEFINES,
    tags = ["unit"],
    # No CC toolchain on macOS x86_64; matches the mongot_vecsim cc_binary above.
    target_compatible_with = VECSIM_NOT_MACOS_X86_64,
    deps = [
        ":vecsim_internal_hdrs",
        ":vecsim_scalar",
    ] + VECSIM_ACCEL_KERNEL_DEPS,
)
