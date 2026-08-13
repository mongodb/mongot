"""Shared platform mappings for the mongot_vecsim kernels.

The dispatcher and its unit test must agree on which accelerated kernels are
linked in on a given platform, so the mapping lives here and is loaded by both
BUILD files rather than being duplicated.
"""

# Baseline optimisation flags for every vecsim translation unit.  CPU feature
# flags (-march / -m...) are NOT here; they are scoped to individual kernels.
VECSIM_BASE_COPTS = [
    "-O3",
    "-DNDEBUG",
]

# There is no CC toolchain on macOS x86_64, so every C target here is
# incompatible there and dependents must select() it out.
VECSIM_NOT_MACOS_X86_64 = select({
    "//bazel/platforms:is_macos_x86_64": ["@platforms//:incompatible"],
    "//conditions:default": [],
})

# Tells the dispatcher / test which accelerated kernel libraries are linked in.
VECSIM_ACCEL_DEFINES = select({
    "//bazel/platforms:is_linux_aarch64": [
        "MONGOT_VECSIM_HAVE_NEON_DOTPROD",
        "MONGOT_VECSIM_HAVE_SVE",
    ],
    "//bazel/platforms:is_linux_x86_64": ["MONGOT_VECSIM_HAVE_AVX512"],
    "//bazel/platforms:is_macos_aarch64": ["MONGOT_VECSIM_HAVE_NEON_DOTPROD"],
    "//conditions:default": [],
})

# The accelerated kernel libraries themselves, matched to VECSIM_ACCEL_DEFINES.
VECSIM_ACCEL_KERNEL_DEPS = select({
    "//bazel/platforms:is_linux_aarch64": [
        "//src/main/c:vecsim_neon_dotprod",
        "//src/main/c:vecsim_sve",
    ],
    "//bazel/platforms:is_linux_x86_64": ["//src/main/c:vecsim_avx512"],
    "//bazel/platforms:is_macos_aarch64": ["//src/main/c:vecsim_neon_dotprod"],
    "//conditions:default": [],
})
