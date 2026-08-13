// Unit test for the int8 vector-similarity kernels.
//
// Framework-free by design (no vendored test library): failures abort via
// assert() / a non-zero exit, which cc_test reports as a failing test.
//
// Two layers of checking:
//   1. Anchor cases with hand-computed results validate the scalar kernels,
//      which then serve as the reference implementation.
//   2. For every accelerated kernel that is linked into this build
//      (MONGOT_VECSIM_HAVE_*), reported by the host CPU (vecsim_has_*), *and*
//      confirmed executable by the dispatcher's SIGILL-safe probe,
//      cross-check its output against scalar across lengths chosen to exercise
//      every unrolled/tail loop.  Integer dot/L2 must match exactly; cosine is
//      derived from the same integer sums via the shared combiner, so it must
//      match exactly too.
#include <assert.h>
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "src/main/c/vecsim_cpu.h"
#include "src/main/c/vecsim_kernels.h"

// The exported dispatch API (defined in the dispatcher translation unit).
extern int32_t mongot_vecsim_dot_i8(const int8_t* a, const int8_t* b, size_t n);
extern int32_t mongot_vecsim_l2_i8(const int8_t* a, const int8_t* b, size_t n);
extern float mongot_vecsim_cos_i8(const int8_t* a, const int8_t* b, size_t n);
extern bool mongot_vecsim_i8_accelerated(void);
// Fork-based execution probe: 1 if the kernel actually ran without trapping.
extern int mongot_vecsim_probe_i8_kernel(int32_t (*fn)(const int8_t*, const int8_t*, size_t));

// Deterministic xorshift32 PRNG so failures are reproducible without pulling in
// <random> or seeding from the environment.
static uint32_t g_rng = 0x9e3779b9u;
static int8_t next_i8(void) {
  g_rng ^= g_rng << 13;
  g_rng ^= g_rng >> 17;
  g_rng ^= g_rng << 5;
  return (int8_t)(g_rng & 0xff);
}

static int g_failures = 0;

#define CHECK(cond, ...)                             \
  do {                                               \
    if (!(cond)) {                                   \
      fprintf(stderr, "FAIL: " __VA_ARGS__);         \
      fprintf(stderr, "  (at %s:%d)\n", __FILE__, __LINE__); \
      ++g_failures;                                  \
    }                                                \
  } while (0)

// --- Layer 1: anchor the scalar reference against hand-computed values ---

static void test_scalar_anchors(void) {
  const int8_t a[] = {1, 2, 3, 4};
  const int8_t b[] = {5, 6, 7, 8};
  // dot = 5 + 12 + 21 + 32 = 70
  CHECK(sim_dot_i8_scalar(a, b, 4) == 70, "scalar dot anchor: got %d\n", sim_dot_i8_scalar(a, b, 4));
  // l2  = 4^2 * 4 = 64
  CHECK(sim_l2_i8_scalar(a, b, 4) == 64, "scalar l2 anchor: got %d\n", sim_l2_i8_scalar(a, b, 4));
  // cos = 70 / sqrt(30 * 174) = 70 / sqrt(5220)
  float expected_cos = 70.0f / sqrtf(30.0f * 174.0f);
  CHECK(fabsf(sim_cos_i8_scalar(a, b, 4) - expected_cos) < 1e-6f,
        "scalar cos anchor: got %f want %f\n", sim_cos_i8_scalar(a, b, 4), expected_cos);

  // Zero norm -> lowest possible similarity, never a divide-by-zero.
  const int8_t zero[] = {0, 0, 0, 0};
  CHECK(sim_cos_i8_scalar(zero, b, 4) == -1.0f, "scalar cos zero-norm should be -1\n");

  // Empty input.
  CHECK(sim_dot_i8_scalar(a, b, 0) == 0, "scalar dot of empty should be 0\n");
  CHECK(sim_l2_i8_scalar(a, b, 0) == 0, "scalar l2 of empty should be 0\n");

  // Extreme int8 values exercise the widening in L2 (|127 - (-128)| = 255).
  const int8_t hi[] = {127, 127};
  const int8_t lo[] = {-128, -128};
  CHECK(sim_l2_i8_scalar(hi, lo, 2) == 255 * 255 * 2, "scalar l2 extremes: got %d\n",
        sim_l2_i8_scalar(hi, lo, 2));
  CHECK(sim_dot_i8_scalar(hi, lo, 2) == 127 * -128 * 2, "scalar dot extremes: got %d\n",
        sim_dot_i8_scalar(hi, lo, 2));
}

// --- Layer 2: cross-check an accelerated kernel against scalar ---

// Lengths chosen to hit the 64/32/16/vl-wide main loops and every scalar tail.
static const size_t kLengths[] = {0,  1,  2,  3,  7,  8,   15,  16,  17,  31,  32,
                                  33, 48, 63, 64, 65, 100, 127, 128, 200, 256, 257,
                                  384, 512, 1000};
#define NUM_LENGTHS (sizeof(kLengths) / sizeof(kLengths[0]))
#define MAX_LEN 1000

typedef int32_t (*i8_fn)(const int8_t*, const int8_t*, size_t);
typedef float (*cos_fn)(const int8_t*, const int8_t*, size_t);

static void cross_check(const char* name, i8_fn dot, i8_fn l2, cos_fn cos) {
  static int8_t a[MAX_LEN];
  static int8_t b[MAX_LEN];
  for (size_t i = 0; i < MAX_LEN; ++i) {
    a[i] = next_i8();
    b[i] = next_i8();
  }
  for (size_t li = 0; li < NUM_LENGTHS; ++li) {
    size_t n = kLengths[li];
    int32_t want_dot = sim_dot_i8_scalar(a, b, n);
    int32_t want_l2  = sim_l2_i8_scalar(a, b, n);
    float   want_cos = sim_cos_i8_scalar(a, b, n);
    CHECK(dot(a, b, n) == want_dot, "%s dot n=%zu: got %d want %d\n", name, n, dot(a, b, n), want_dot);
    CHECK(l2(a, b, n) == want_l2, "%s l2 n=%zu: got %d want %d\n", name, n, l2(a, b, n), want_l2);
    // cos reuses the identical integer sums + combiner, so it must match exactly.
    CHECK(cos(a, b, n) == want_cos, "%s cos n=%zu: got %f want %f\n", name, n, cos(a, b, n), want_cos);
  }
}

// True if a kernel tier is safe to call directly here: the CPU reports the feature and
// the dispatcher's forked probe confirmed the instructions really execute.  Skipping on a
// failed probe matters -- a lying CPUID/HWCAP would otherwise SIGILL this process rather
// than produce a test result, and the dispatcher itself would have fallen back anyway.
static bool g_any_kernel_probed_ok = false;

static bool kernel_runnable(const char* name, int has_feature, i8_fn dot) {
  if (!has_feature) {
    fprintf(stderr, "note: host lacks %s; skipping that kernel\n", name);
    return false;
  }
  if (!mongot_vecsim_probe_i8_kernel(dot)) {
    fprintf(stderr, "note: %s reported by CPU but failed its execution probe; skipping\n", name);
    return false;
  }
  g_any_kernel_probed_ok = true;
  return true;
}

int main(void) {
  test_scalar_anchors();

  // Scalar cross-checks itself (sanity that the harness compares cleanly).
  cross_check("scalar", sim_dot_i8_scalar, sim_l2_i8_scalar, sim_cos_i8_scalar);

#if defined(MONGOT_VECSIM_HAVE_NEON_DOTPROD)
  if (kernel_runnable("FEAT_DotProd", vecsim_has_dotprod(), sim_dot_i8_dotprod)) {
    cross_check("neon_dotprod", sim_dot_i8_dotprod, sim_l2_i8_dotprod, sim_cos_i8_dotprod);
  }
#endif
#if defined(MONGOT_VECSIM_HAVE_SVE)
  if (kernel_runnable("SVE", vecsim_has_sve(), sim_dot_i8_sve)) {
    cross_check("sve", sim_dot_i8_sve, sim_l2_i8_sve, sim_cos_i8_sve);
  }
#endif
#if defined(MONGOT_VECSIM_HAVE_AVX512)
  if (kernel_runnable("AVX-512 VNNI", vecsim_has_avx512vnni(), sim_dot_i8_avx512vnni)) {
    cross_check("avx512vnni", sim_dot_i8_avx512vnni, sim_l2_i8_avx512vnni, sim_cos_i8_avx512vnni);
  }
#endif

  // The exported dispatch API must agree with scalar regardless of which kernel
  // was selected, and its accelerated flag must match what the host supports.
  {
    static int8_t a[MAX_LEN];
    static int8_t b[MAX_LEN];
    for (size_t i = 0; i < MAX_LEN; ++i) {
      a[i] = next_i8();
      b[i] = next_i8();
    }
    for (size_t li = 0; li < NUM_LENGTHS; ++li) {
      size_t n = kLengths[li];
      CHECK(mongot_vecsim_dot_i8(a, b, n) == sim_dot_i8_scalar(a, b, n),
            "dispatch dot n=%zu mismatch\n", n);
      CHECK(mongot_vecsim_l2_i8(a, b, n) == sim_l2_i8_scalar(a, b, n),
            "dispatch l2 n=%zu mismatch\n", n);
      CHECK(mongot_vecsim_cos_i8(a, b, n) == sim_cos_i8_scalar(a, b, n),
            "dispatch cos n=%zu mismatch\n", n);
    }
  }

  // The dispatcher tries each linked tier in order and falls back on probe failure, so if
  // any tier passed its probe above, init must have bound an accelerated kernel.
  if (g_any_kernel_probed_ok) {
    CHECK(mongot_vecsim_i8_accelerated(),
          "a kernel passed its execution probe but dispatch is still scalar\n");
  }

  if (g_failures != 0) {
    fprintf(stderr, "%d check(s) failed\n", g_failures);
    return 1;
  }
  printf("all vecsim checks passed\n");
  return 0;
}
