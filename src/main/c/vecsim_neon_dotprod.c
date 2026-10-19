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

// (CLOUDP-435168): loads the query once per 16-byte chunk and reuses it across all 4 documents'
// vdotq_s32, instead of the 4-sequential-calls fallback in mongot_vecsim.c reloading and
// re-decoding it 4 times. Unrolled 4-wide per document, matching sim_dot_i8_dotprod's own unroll
// depth -- an under-unrolled first attempt measured slower than 4 sequential calls. Int32
// accumulation is exact, so accumulation order need not match the reference to stay bit-identical.
HIDDEN void sim_dot_i8_bulk4_dotprod(const int8_t* q, const int8_t* v0, const int8_t* v1,
                                     const int8_t* v2, const int8_t* v3, int32_t* out, size_t n) {
  int32x4_t a00 = vdupq_n_s32(0), a01 = vdupq_n_s32(0), a02 = vdupq_n_s32(0), a03 = vdupq_n_s32(0);
  int32x4_t a10 = vdupq_n_s32(0), a11 = vdupq_n_s32(0), a12 = vdupq_n_s32(0), a13 = vdupq_n_s32(0);
  int32x4_t a20 = vdupq_n_s32(0), a21 = vdupq_n_s32(0), a22 = vdupq_n_s32(0), a23 = vdupq_n_s32(0);
  int32x4_t a30 = vdupq_n_s32(0), a31 = vdupq_n_s32(0), a32 = vdupq_n_s32(0), a33 = vdupq_n_s32(0);
  size_t i = 0;
  for (; i + 63 < n; i += 64) {
    int8x16_t q0 = vld1q_s8(q + i),      q1 = vld1q_s8(q + i + 16);
    int8x16_t q2 = vld1q_s8(q + i + 32), q3 = vld1q_s8(q + i + 48);
    a00 = vdotq_s32(a00, q0, vld1q_s8(v0 + i));      a01 = vdotq_s32(a01, q1, vld1q_s8(v0 + i + 16));
    a02 = vdotq_s32(a02, q2, vld1q_s8(v0 + i + 32)); a03 = vdotq_s32(a03, q3, vld1q_s8(v0 + i + 48));
    a10 = vdotq_s32(a10, q0, vld1q_s8(v1 + i));      a11 = vdotq_s32(a11, q1, vld1q_s8(v1 + i + 16));
    a12 = vdotq_s32(a12, q2, vld1q_s8(v1 + i + 32)); a13 = vdotq_s32(a13, q3, vld1q_s8(v1 + i + 48));
    a20 = vdotq_s32(a20, q0, vld1q_s8(v2 + i));      a21 = vdotq_s32(a21, q1, vld1q_s8(v2 + i + 16));
    a22 = vdotq_s32(a22, q2, vld1q_s8(v2 + i + 32)); a23 = vdotq_s32(a23, q3, vld1q_s8(v2 + i + 48));
    a30 = vdotq_s32(a30, q0, vld1q_s8(v3 + i));      a31 = vdotq_s32(a31, q1, vld1q_s8(v3 + i + 16));
    a32 = vdotq_s32(a32, q2, vld1q_s8(v3 + i + 32)); a33 = vdotq_s32(a33, q3, vld1q_s8(v3 + i + 48));
  }
  int32_t sum0 = vaddvq_s32(vaddq_s32(vaddq_s32(a00, a01), vaddq_s32(a02, a03)));
  int32_t sum1 = vaddvq_s32(vaddq_s32(vaddq_s32(a10, a11), vaddq_s32(a12, a13)));
  int32_t sum2 = vaddvq_s32(vaddq_s32(vaddq_s32(a20, a21), vaddq_s32(a22, a23)));
  int32_t sum3 = vaddvq_s32(vaddq_s32(vaddq_s32(a30, a31), vaddq_s32(a32, a33)));
  for (; i + 15 < n; i += 16) {
    int8x16_t qi = vld1q_s8(q + i);
    sum0 += vaddvq_s32(vdotq_s32(vdupq_n_s32(0), qi, vld1q_s8(v0 + i)));
    sum1 += vaddvq_s32(vdotq_s32(vdupq_n_s32(0), qi, vld1q_s8(v1 + i)));
    sum2 += vaddvq_s32(vdotq_s32(vdupq_n_s32(0), qi, vld1q_s8(v2 + i)));
    sum3 += vaddvq_s32(vdotq_s32(vdupq_n_s32(0), qi, vld1q_s8(v3 + i)));
  }
  for (; i < n; ++i) {
    int32_t qv = (int32_t)q[i];
    sum0 += qv * (int32_t)v0[i];
    sum1 += qv * (int32_t)v1[i];
    sum2 += qv * (int32_t)v2[i];
    sum3 += qv * (int32_t)v3[i];
  }
  out[0] = sum0;
  out[1] = sum1;
  out[2] = sum2;
  out[3] = sum3;
}

// Same fusion/tuning rationale as sim_dot_i8_bulk4_dotprod, ported to squared L2 distance:
// query loaded once per 16-byte chunk, 4 accumulators per document (64 bytes/doc/iteration)
// matching sim_l2_i8_dotprod's own unroll depth.
HIDDEN void sim_l2_i8_bulk4_dotprod(const int8_t* q, const int8_t* v0, const int8_t* v1,
                                    const int8_t* v2, const int8_t* v3, int32_t* out, size_t n) {
  uint32x4_t a00 = vdupq_n_u32(0), a01 = vdupq_n_u32(0), a02 = vdupq_n_u32(0), a03 = vdupq_n_u32(0);
  uint32x4_t a10 = vdupq_n_u32(0), a11 = vdupq_n_u32(0), a12 = vdupq_n_u32(0), a13 = vdupq_n_u32(0);
  uint32x4_t a20 = vdupq_n_u32(0), a21 = vdupq_n_u32(0), a22 = vdupq_n_u32(0), a23 = vdupq_n_u32(0);
  uint32x4_t a30 = vdupq_n_u32(0), a31 = vdupq_n_u32(0), a32 = vdupq_n_u32(0), a33 = vdupq_n_u32(0);
  size_t i = 0;
  for (; i + 63 < n; i += 64) {
    int8x16_t q0 = vld1q_s8(q + i),      q1 = vld1q_s8(q + i + 16);
    int8x16_t q2 = vld1q_s8(q + i + 32), q3 = vld1q_s8(q + i + 48);
    uint8x16_t d00 = vreinterpretq_u8_s8(vabdq_s8(q0, vld1q_s8(v0 + i)));
    uint8x16_t d01 = vreinterpretq_u8_s8(vabdq_s8(q1, vld1q_s8(v0 + i + 16)));
    uint8x16_t d02 = vreinterpretq_u8_s8(vabdq_s8(q2, vld1q_s8(v0 + i + 32)));
    uint8x16_t d03 = vreinterpretq_u8_s8(vabdq_s8(q3, vld1q_s8(v0 + i + 48)));
    a00 = vdotq_u32(a00, d00, d00); a01 = vdotq_u32(a01, d01, d01);
    a02 = vdotq_u32(a02, d02, d02); a03 = vdotq_u32(a03, d03, d03);
    uint8x16_t d10 = vreinterpretq_u8_s8(vabdq_s8(q0, vld1q_s8(v1 + i)));
    uint8x16_t d11 = vreinterpretq_u8_s8(vabdq_s8(q1, vld1q_s8(v1 + i + 16)));
    uint8x16_t d12 = vreinterpretq_u8_s8(vabdq_s8(q2, vld1q_s8(v1 + i + 32)));
    uint8x16_t d13 = vreinterpretq_u8_s8(vabdq_s8(q3, vld1q_s8(v1 + i + 48)));
    a10 = vdotq_u32(a10, d10, d10); a11 = vdotq_u32(a11, d11, d11);
    a12 = vdotq_u32(a12, d12, d12); a13 = vdotq_u32(a13, d13, d13);
    uint8x16_t d20 = vreinterpretq_u8_s8(vabdq_s8(q0, vld1q_s8(v2 + i)));
    uint8x16_t d21 = vreinterpretq_u8_s8(vabdq_s8(q1, vld1q_s8(v2 + i + 16)));
    uint8x16_t d22 = vreinterpretq_u8_s8(vabdq_s8(q2, vld1q_s8(v2 + i + 32)));
    uint8x16_t d23 = vreinterpretq_u8_s8(vabdq_s8(q3, vld1q_s8(v2 + i + 48)));
    a20 = vdotq_u32(a20, d20, d20); a21 = vdotq_u32(a21, d21, d21);
    a22 = vdotq_u32(a22, d22, d22); a23 = vdotq_u32(a23, d23, d23);
    uint8x16_t d30 = vreinterpretq_u8_s8(vabdq_s8(q0, vld1q_s8(v3 + i)));
    uint8x16_t d31 = vreinterpretq_u8_s8(vabdq_s8(q1, vld1q_s8(v3 + i + 16)));
    uint8x16_t d32 = vreinterpretq_u8_s8(vabdq_s8(q2, vld1q_s8(v3 + i + 32)));
    uint8x16_t d33 = vreinterpretq_u8_s8(vabdq_s8(q3, vld1q_s8(v3 + i + 48)));
    a30 = vdotq_u32(a30, d30, d30); a31 = vdotq_u32(a31, d31, d31);
    a32 = vdotq_u32(a32, d32, d32); a33 = vdotq_u32(a33, d33, d33);
  }
  int32_t sum0 = (int32_t)vaddvq_u32(vaddq_u32(vaddq_u32(a00, a01), vaddq_u32(a02, a03)));
  int32_t sum1 = (int32_t)vaddvq_u32(vaddq_u32(vaddq_u32(a10, a11), vaddq_u32(a12, a13)));
  int32_t sum2 = (int32_t)vaddvq_u32(vaddq_u32(vaddq_u32(a20, a21), vaddq_u32(a22, a23)));
  int32_t sum3 = (int32_t)vaddvq_u32(vaddq_u32(vaddq_u32(a30, a31), vaddq_u32(a32, a33)));
  for (; i + 15 < n; i += 16) {
    int8x16_t qi = vld1q_s8(q + i);
    uint8x16_t d0 = vreinterpretq_u8_s8(vabdq_s8(qi, vld1q_s8(v0 + i)));
    uint8x16_t d1 = vreinterpretq_u8_s8(vabdq_s8(qi, vld1q_s8(v1 + i)));
    uint8x16_t d2 = vreinterpretq_u8_s8(vabdq_s8(qi, vld1q_s8(v2 + i)));
    uint8x16_t d3 = vreinterpretq_u8_s8(vabdq_s8(qi, vld1q_s8(v3 + i)));
    sum0 += (int32_t)vaddvq_u32(vdotq_u32(vdupq_n_u32(0), d0, d0));
    sum1 += (int32_t)vaddvq_u32(vdotq_u32(vdupq_n_u32(0), d1, d1));
    sum2 += (int32_t)vaddvq_u32(vdotq_u32(vdupq_n_u32(0), d2, d2));
    sum3 += (int32_t)vaddvq_u32(vdotq_u32(vdupq_n_u32(0), d3, d3));
  }
  for (; i < n; ++i) {
    int32_t qv = (int32_t)q[i];
    int32_t d0 = qv - (int32_t)v0[i], d1 = qv - (int32_t)v1[i];
    int32_t d2 = qv - (int32_t)v2[i], d3 = qv - (int32_t)v3[i];
    sum0 += d0 * d0; sum1 += d1 * d1; sum2 += d2 * d2; sum3 += d3 * d3;
  }
  out[0] = sum0;
  out[1] = sum1;
  out[2] = sum2;
  out[3] = sum3;
}
#endif  // __ARM_NEON && __ARM_FEATURE_DOTPROD
