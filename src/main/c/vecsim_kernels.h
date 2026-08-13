// Internal declarations shared between the mongot_vecsim dispatcher and the
// per-ISA kernel component libraries.  Each kernel family lives in its own
// translation unit / cc_library so that CPU feature flags (-march / -m...) are
// scoped to exactly the kernels that require them; in particular the scalar
// fallback is compiled at baseline so it is safe to run on any CPU.
#ifndef SRC_MAIN_C_VECSIM_KERNELS_H_
#define SRC_MAIN_C_VECSIM_KERNELS_H_

#include <math.h>
#include <stddef.h>
#include <stdint.h>

#define HIDDEN __attribute__((visibility("hidden")))

// Combine a raw dot product and the two squared norms into an angular
// similarity score.  Returns the lowest possible score if a norm is zero so we
// never divide by zero.
static inline float sim_cos_combine(int32_t dot, int32_t norm_a, int32_t norm_b) {
  return norm_a == 0 || norm_b == 0 ? -1.0f : (float)dot / sqrtf((float)norm_a * (float)norm_b);
}

// --- Scalar kernels (baseline, always linked) ---
HIDDEN int32_t sim_dot_i8_scalar(const int8_t* a, const int8_t* b, size_t n);
HIDDEN int32_t sim_l2_i8_scalar(const int8_t* a, const int8_t* b, size_t n);
HIDDEN float sim_cos_i8_scalar(const int8_t* a, const int8_t* b, size_t n);

// --- NEON dotprod kernels (aarch64) ---
HIDDEN int32_t sim_dot_i8_dotprod(const int8_t* a, const int8_t* b, size_t n);
HIDDEN int32_t sim_l2_i8_dotprod(const int8_t* a, const int8_t* b, size_t n);
HIDDEN float sim_cos_i8_dotprod(const int8_t* a, const int8_t* b, size_t n);

// --- SVE kernels (aarch64) ---
HIDDEN int32_t sim_dot_i8_sve(const int8_t* a, const int8_t* b, size_t n);
HIDDEN int32_t sim_l2_i8_sve(const int8_t* a, const int8_t* b, size_t n);
HIDDEN float sim_cos_i8_sve(const int8_t* a, const int8_t* b, size_t n);

// --- x86_64 AVX-512 VNNI kernels ---
HIDDEN int32_t sim_dot_i8_avx512vnni(const int8_t* a, const int8_t* b, size_t n);
HIDDEN int32_t sim_l2_i8_avx512vnni(const int8_t* a, const int8_t* b, size_t n);
HIDDEN float sim_cos_i8_avx512vnni(const int8_t* a, const int8_t* b, size_t n);

#endif  // SRC_MAIN_C_VECSIM_KERNELS_H_
