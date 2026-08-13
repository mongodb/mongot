// NEON dotprod (FEAT_DotProd) int8 similarity kernels for aarch64.  The
// component library is compiled with -march=...+dotprod, so no per-function
// target attribute is needed.  The runtime guard for FEAT_DotProd lives in the
// dispatcher (has_dotprod); these functions are only invoked once it returns
// true.
#include "src/main/c/vecsim_kernels.h"

#if defined(__ARM_NEON) && defined(__ARM_FEATURE_DOTPROD)
#include <arm_neon.h>

HIDDEN int32_t sim_dot_i8_dotprod(const int8_t* a, const int8_t* b, size_t n) {
  int32x4_t acc0 = vdupq_n_s32(0);
  int32x4_t acc1 = vdupq_n_s32(0);
  int32x4_t acc2 = vdupq_n_s32(0);
  int32x4_t acc3 = vdupq_n_s32(0);
  size_t i = 0;
  for (; i + 63 < n; i += 64) {
    acc0 = vdotq_s32(acc0, vld1q_s8(a + i),      vld1q_s8(b + i));
    acc1 = vdotq_s32(acc1, vld1q_s8(a + i + 16), vld1q_s8(b + i + 16));
    acc2 = vdotq_s32(acc2, vld1q_s8(a + i + 32), vld1q_s8(b + i + 32));
    acc3 = vdotq_s32(acc3, vld1q_s8(a + i + 48), vld1q_s8(b + i + 48));
  }
  acc0 = vaddq_s32(vaddq_s32(acc0, acc1), vaddq_s32(acc2, acc3));
  for (; i + 15 < n; i += 16)
    acc0 = vdotq_s32(acc0, vld1q_s8(a + i), vld1q_s8(b + i));
  int32_t sum = vaddvq_s32(acc0);
  for (; i < n; ++i) sum += (int32_t)a[i] * (int32_t)b[i];
  return sum;
}

HIDDEN int32_t sim_l2_i8_dotprod(const int8_t* a, const int8_t* b, size_t n) {
  uint32x4_t acc0 = vdupq_n_u32(0);
  uint32x4_t acc1 = vdupq_n_u32(0);
  uint32x4_t acc2 = vdupq_n_u32(0);
  uint32x4_t acc3 = vdupq_n_u32(0);
  size_t i = 0;
  for (; i + 63 < n; i += 64) {
    uint8x16_t d0 = vreinterpretq_u8_s8(vabdq_s8(vld1q_s8(a + i),      vld1q_s8(b + i)));
    uint8x16_t d1 = vreinterpretq_u8_s8(vabdq_s8(vld1q_s8(a + i + 16), vld1q_s8(b + i + 16)));
    uint8x16_t d2 = vreinterpretq_u8_s8(vabdq_s8(vld1q_s8(a + i + 32), vld1q_s8(b + i + 32)));
    uint8x16_t d3 = vreinterpretq_u8_s8(vabdq_s8(vld1q_s8(a + i + 48), vld1q_s8(b + i + 48)));
    acc0 = vdotq_u32(acc0, d0, d0);
    acc1 = vdotq_u32(acc1, d1, d1);
    acc2 = vdotq_u32(acc2, d2, d2);
    acc3 = vdotq_u32(acc3, d3, d3);
  }
  acc0 = vaddq_u32(vaddq_u32(acc0, acc1), vaddq_u32(acc2, acc3));
  for (; i + 15 < n; i += 16) {
    uint8x16_t d = vreinterpretq_u8_s8(vabdq_s8(vld1q_s8(a + i), vld1q_s8(b + i)));
    acc0 = vdotq_u32(acc0, d, d);
  }
  int32_t sum = (int32_t)vaddvq_u32(acc0);
  for (; i < n; ++i) {
    int32_t diff = (int32_t)a[i] - (int32_t)b[i];
    sum += diff * diff;
  }
  return sum;
}

HIDDEN float sim_cos_i8_dotprod(const int8_t* a, const int8_t* b, size_t n) {
  int32x4_t ab0 = vdupq_n_s32(0), ab1 = vdupq_n_s32(0), ab2 = vdupq_n_s32(0), ab3 = vdupq_n_s32(0);
  int32x4_t aa0 = vdupq_n_s32(0), aa1 = vdupq_n_s32(0), aa2 = vdupq_n_s32(0), aa3 = vdupq_n_s32(0);
  int32x4_t bb0 = vdupq_n_s32(0), bb1 = vdupq_n_s32(0), bb2 = vdupq_n_s32(0), bb3 = vdupq_n_s32(0);
  size_t i = 0;
  for (; i + 63 < n; i += 64) {
    int8x16_t a0 = vld1q_s8(a + i),      b0 = vld1q_s8(b + i);
    int8x16_t a1 = vld1q_s8(a + i + 16), b1 = vld1q_s8(b + i + 16);
    int8x16_t a2 = vld1q_s8(a + i + 32), b2 = vld1q_s8(b + i + 32);
    int8x16_t a3 = vld1q_s8(a + i + 48), b3 = vld1q_s8(b + i + 48);
    ab0 = vdotq_s32(ab0, a0, b0); aa0 = vdotq_s32(aa0, a0, a0); bb0 = vdotq_s32(bb0, b0, b0);
    ab1 = vdotq_s32(ab1, a1, b1); aa1 = vdotq_s32(aa1, a1, a1); bb1 = vdotq_s32(bb1, b1, b1);
    ab2 = vdotq_s32(ab2, a2, b2); aa2 = vdotq_s32(aa2, a2, a2); bb2 = vdotq_s32(bb2, b2, b2);
    ab3 = vdotq_s32(ab3, a3, b3); aa3 = vdotq_s32(aa3, a3, a3); bb3 = vdotq_s32(bb3, b3, b3);
  }
  int32x4_t ab = vaddq_s32(vaddq_s32(ab0, ab1), vaddq_s32(ab2, ab3));
  int32x4_t aa = vaddq_s32(vaddq_s32(aa0, aa1), vaddq_s32(aa2, aa3));
  int32x4_t bb = vaddq_s32(vaddq_s32(bb0, bb1), vaddq_s32(bb2, bb3));
  for (; i + 15 < n; i += 16) {
    int8x16_t ai = vld1q_s8(a + i), bi = vld1q_s8(b + i);
    ab = vdotq_s32(ab, ai, bi); aa = vdotq_s32(aa, ai, ai); bb = vdotq_s32(bb, bi, bi);
  }
  int32_t dot    = vaddvq_s32(ab);
  int32_t norm_a = vaddvq_s32(aa);
  int32_t norm_b = vaddvq_s32(bb);
  for (; i < n; ++i) {
    dot    += (int32_t)a[i] * (int32_t)b[i];
    norm_a += (int32_t)a[i] * (int32_t)a[i];
    norm_b += (int32_t)b[i] * (int32_t)b[i];
  }
  return sim_cos_combine(dot, norm_a, norm_b);
}
#endif  // __ARM_NEON && __ARM_FEATURE_DOTPROD
