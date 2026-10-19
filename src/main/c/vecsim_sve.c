// SVE int8 similarity kernels for aarch64.  The component library is compiled
// with -march=...+sve, so no per-function target attribute is needed.  The
// runtime guard for FEAT_SVE lives in the dispatcher (has_sve); these functions
// are only invoked once it returns true.
#include "src/main/c/vecsim_kernels.h"

#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE)
#include <arm_sve.h>

HIDDEN int32_t sim_dot_i8_sve(const int8_t* a, const int8_t* b, size_t n) {
  svint32_t acc0 = svdup_n_s32(0), acc1 = svdup_n_s32(0);
  const size_t vl = svcntb();
  size_t i = 0;
  for (; i + 2 * vl <= n; i += 2 * vl) {
    acc0 = svdot_s32(acc0, svld1_s8(svptrue_b8(), a + i),      svld1_s8(svptrue_b8(), b + i));
    acc1 = svdot_s32(acc1, svld1_s8(svptrue_b8(), a + i + vl), svld1_s8(svptrue_b8(), b + i + vl));
  }
  acc0 = svadd_s32_x(svptrue_b32(), acc0, acc1);
  for (; i + vl <= n; i += vl)
    acc0 = svdot_s32(acc0, svld1_s8(svptrue_b8(), a + i), svld1_s8(svptrue_b8(), b + i));
  if (i < n) {
    svbool_t pg = svwhilelt_b8_u64(i, (uint64_t)n);
    acc0 = svdot_s32(acc0, svld1_s8(pg, a + i), svld1_s8(pg, b + i));
  }
  return (int32_t)svaddv_s32(svptrue_b32(), acc0);
}

HIDDEN int32_t sim_l2_i8_sve(const int8_t* a, const int8_t* b, size_t n) {
  svuint32_t acc0 = svdup_n_u32(0), acc1 = svdup_n_u32(0);
  const size_t vl = svcntb();
  size_t i = 0;
  for (; i + 2 * vl <= n; i += 2 * vl) {
    svuint8_t d0 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), svld1_s8(svptrue_b8(), a + i),      svld1_s8(svptrue_b8(), b + i)));
    svuint8_t d1 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), svld1_s8(svptrue_b8(), a + i + vl), svld1_s8(svptrue_b8(), b + i + vl)));
    acc0 = svdot_u32(acc0, d0, d0);
    acc1 = svdot_u32(acc1, d1, d1);
  }
  acc0 = svadd_u32_x(svptrue_b32(), acc0, acc1);
  for (; i + vl <= n; i += vl) {
    svuint8_t d = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), svld1_s8(svptrue_b8(), a + i), svld1_s8(svptrue_b8(), b + i)));
    acc0 = svdot_u32(acc0, d, d);
  }
  if (i < n) {
    svbool_t pg = svwhilelt_b8_u64(i, (uint64_t)n);
    // Inactive lanes from svld1 are zero, so |0-0|=0 contributes nothing.
    svuint8_t d = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), svld1_s8(pg, a + i), svld1_s8(pg, b + i)));
    acc0 = svdot_u32(acc0, d, d);
  }
  return (int32_t)svaddv_u32(svptrue_b32(), acc0);
}

HIDDEN float sim_cos_i8_sve(const int8_t* a, const int8_t* b, size_t n) {
  svint32_t ab0 = svdup_n_s32(0), ab1 = svdup_n_s32(0);
  svint32_t aa0 = svdup_n_s32(0), aa1 = svdup_n_s32(0);
  svint32_t bb0 = svdup_n_s32(0), bb1 = svdup_n_s32(0);
  const size_t vl = svcntb();
  size_t i = 0;
  for (; i + 2 * vl <= n; i += 2 * vl) {
    svint8_t ai = svld1_s8(svptrue_b8(), a + i),      bi = svld1_s8(svptrue_b8(), b + i);
    svint8_t aj = svld1_s8(svptrue_b8(), a + i + vl), bj = svld1_s8(svptrue_b8(), b + i + vl);
    ab0 = svdot_s32(ab0, ai, bi); aa0 = svdot_s32(aa0, ai, ai); bb0 = svdot_s32(bb0, bi, bi);
    ab1 = svdot_s32(ab1, aj, bj); aa1 = svdot_s32(aa1, aj, aj); bb1 = svdot_s32(bb1, bj, bj);
  }
  svint32_t ab = svadd_s32_x(svptrue_b32(), ab0, ab1);
  svint32_t aa = svadd_s32_x(svptrue_b32(), aa0, aa1);
  svint32_t bb = svadd_s32_x(svptrue_b32(), bb0, bb1);
  for (; i + vl <= n; i += vl) {
    svint8_t ai = svld1_s8(svptrue_b8(), a + i), bi = svld1_s8(svptrue_b8(), b + i);
    ab = svdot_s32(ab, ai, bi); aa = svdot_s32(aa, ai, ai); bb = svdot_s32(bb, bi, bi);
  }
  if (i < n) {
    svbool_t pg = svwhilelt_b8_u64(i, (uint64_t)n);
    svint8_t ai = svld1_s8(pg, a + i), bi = svld1_s8(pg, b + i);
    ab = svdot_s32(ab, ai, bi); aa = svdot_s32(aa, ai, ai); bb = svdot_s32(bb, bi, bi);
  }
  return sim_cos_combine(
    (int32_t)svaddv_s32(svptrue_b32(), ab),
    (int32_t)svaddv_s32(svptrue_b32(), aa),
    (int32_t)svaddv_s32(svptrue_b32(), bb)
  );
}

// (CLOUDP-435168): loads the query once per chunk and reuses it across all 4 documents'
// svdot_s32, instead of the 4-sequential-calls fallback in mongot_vecsim.c reloading and
// re-decoding it 4 times. Unrolled 2-wide per document over 2*vl chunks, matching sim_dot_i8_sve's
// own unroll depth -- an under-unrolled first attempt measured slower than 4 sequential calls.
// Int32 accumulation is exact, so accumulation order need not match the reference to stay
// bit-identical.
HIDDEN void sim_dot_i8_bulk4_sve(const int8_t* q, const int8_t* v0, const int8_t* v1,
                                 const int8_t* v2, const int8_t* v3, int32_t* out, size_t n) {
  svint32_t a00 = svdup_n_s32(0), a01 = svdup_n_s32(0);
  svint32_t a10 = svdup_n_s32(0), a11 = svdup_n_s32(0);
  svint32_t a20 = svdup_n_s32(0), a21 = svdup_n_s32(0);
  svint32_t a30 = svdup_n_s32(0), a31 = svdup_n_s32(0);
  const size_t vl = svcntb();
  size_t i = 0;
  for (; i + 2 * vl <= n; i += 2 * vl) {
    svint8_t q0 = svld1_s8(svptrue_b8(), q + i);
    svint8_t q1 = svld1_s8(svptrue_b8(), q + i + vl);
    a00 = svdot_s32(a00, q0, svld1_s8(svptrue_b8(), v0 + i));
    a01 = svdot_s32(a01, q1, svld1_s8(svptrue_b8(), v0 + i + vl));
    a10 = svdot_s32(a10, q0, svld1_s8(svptrue_b8(), v1 + i));
    a11 = svdot_s32(a11, q1, svld1_s8(svptrue_b8(), v1 + i + vl));
    a20 = svdot_s32(a20, q0, svld1_s8(svptrue_b8(), v2 + i));
    a21 = svdot_s32(a21, q1, svld1_s8(svptrue_b8(), v2 + i + vl));
    a30 = svdot_s32(a30, q0, svld1_s8(svptrue_b8(), v3 + i));
    a31 = svdot_s32(a31, q1, svld1_s8(svptrue_b8(), v3 + i + vl));
  }
  svint32_t acc0 = svadd_s32_x(svptrue_b32(), a00, a01);
  svint32_t acc1 = svadd_s32_x(svptrue_b32(), a10, a11);
  svint32_t acc2 = svadd_s32_x(svptrue_b32(), a20, a21);
  svint32_t acc3 = svadd_s32_x(svptrue_b32(), a30, a31);
  for (; i + vl <= n; i += vl) {
    svint8_t qi = svld1_s8(svptrue_b8(), q + i);
    acc0 = svdot_s32(acc0, qi, svld1_s8(svptrue_b8(), v0 + i));
    acc1 = svdot_s32(acc1, qi, svld1_s8(svptrue_b8(), v1 + i));
    acc2 = svdot_s32(acc2, qi, svld1_s8(svptrue_b8(), v2 + i));
    acc3 = svdot_s32(acc3, qi, svld1_s8(svptrue_b8(), v3 + i));
  }
  if (i < n) {
    svbool_t pg = svwhilelt_b8_u64(i, (uint64_t)n);
    svint8_t qi = svld1_s8(pg, q + i);
    acc0 = svdot_s32(acc0, qi, svld1_s8(pg, v0 + i));
    acc1 = svdot_s32(acc1, qi, svld1_s8(pg, v1 + i));
    acc2 = svdot_s32(acc2, qi, svld1_s8(pg, v2 + i));
    acc3 = svdot_s32(acc3, qi, svld1_s8(pg, v3 + i));
  }
  out[0] = (int32_t)svaddv_s32(svptrue_b32(), acc0);
  out[1] = (int32_t)svaddv_s32(svptrue_b32(), acc1);
  out[2] = (int32_t)svaddv_s32(svptrue_b32(), acc2);
  out[3] = (int32_t)svaddv_s32(svptrue_b32(), acc3);
}

// Same fusion/tuning rationale as sim_dot_i8_bulk4_sve, ported to squared L2 distance: query
// loaded once per chunk, 2 accumulators per document (8 total) over 2*vl chunks, matching
// sim_l2_i8_sve's own unroll depth.
HIDDEN void sim_l2_i8_bulk4_sve(const int8_t* q, const int8_t* v0, const int8_t* v1,
                                const int8_t* v2, const int8_t* v3, int32_t* out, size_t n) {
  svuint32_t a00 = svdup_n_u32(0), a01 = svdup_n_u32(0);
  svuint32_t a10 = svdup_n_u32(0), a11 = svdup_n_u32(0);
  svuint32_t a20 = svdup_n_u32(0), a21 = svdup_n_u32(0);
  svuint32_t a30 = svdup_n_u32(0), a31 = svdup_n_u32(0);
  const size_t vl = svcntb();
  size_t i = 0;
  for (; i + 2 * vl <= n; i += 2 * vl) {
    svint8_t q0 = svld1_s8(svptrue_b8(), q + i);
    svint8_t q1 = svld1_s8(svptrue_b8(), q + i + vl);
    svuint8_t d00 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), q0, svld1_s8(svptrue_b8(), v0 + i)));
    svuint8_t d01 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), q1, svld1_s8(svptrue_b8(), v0 + i + vl)));
    a00 = svdot_u32(a00, d00, d00);
    a01 = svdot_u32(a01, d01, d01);
    svuint8_t d10 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), q0, svld1_s8(svptrue_b8(), v1 + i)));
    svuint8_t d11 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), q1, svld1_s8(svptrue_b8(), v1 + i + vl)));
    a10 = svdot_u32(a10, d10, d10);
    a11 = svdot_u32(a11, d11, d11);
    svuint8_t d20 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), q0, svld1_s8(svptrue_b8(), v2 + i)));
    svuint8_t d21 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), q1, svld1_s8(svptrue_b8(), v2 + i + vl)));
    a20 = svdot_u32(a20, d20, d20);
    a21 = svdot_u32(a21, d21, d21);
    svuint8_t d30 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), q0, svld1_s8(svptrue_b8(), v3 + i)));
    svuint8_t d31 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), q1, svld1_s8(svptrue_b8(), v3 + i + vl)));
    a30 = svdot_u32(a30, d30, d30);
    a31 = svdot_u32(a31, d31, d31);
  }
  svuint32_t acc0 = svadd_u32_x(svptrue_b32(), a00, a01);
  svuint32_t acc1 = svadd_u32_x(svptrue_b32(), a10, a11);
  svuint32_t acc2 = svadd_u32_x(svptrue_b32(), a20, a21);
  svuint32_t acc3 = svadd_u32_x(svptrue_b32(), a30, a31);
  for (; i + vl <= n; i += vl) {
    svint8_t qi = svld1_s8(svptrue_b8(), q + i);
    svuint8_t d0 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), qi, svld1_s8(svptrue_b8(), v0 + i)));
    svuint8_t d1 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), qi, svld1_s8(svptrue_b8(), v1 + i)));
    svuint8_t d2 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), qi, svld1_s8(svptrue_b8(), v2 + i)));
    svuint8_t d3 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), qi, svld1_s8(svptrue_b8(), v3 + i)));
    acc0 = svdot_u32(acc0, d0, d0);
    acc1 = svdot_u32(acc1, d1, d1);
    acc2 = svdot_u32(acc2, d2, d2);
    acc3 = svdot_u32(acc3, d3, d3);
  }
  if (i < n) {
    svbool_t pg = svwhilelt_b8_u64(i, (uint64_t)n);
    svint8_t qi = svld1_s8(pg, q + i);
    // Inactive lanes from svld1 are zero, so |0-0|=0 contributes nothing (same as sim_l2_i8_sve).
    svuint8_t d0 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), qi, svld1_s8(pg, v0 + i)));
    svuint8_t d1 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), qi, svld1_s8(pg, v1 + i)));
    svuint8_t d2 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), qi, svld1_s8(pg, v2 + i)));
    svuint8_t d3 = svreinterpret_u8_s8(svabd_s8_x(svptrue_b8(), qi, svld1_s8(pg, v3 + i)));
    acc0 = svdot_u32(acc0, d0, d0);
    acc1 = svdot_u32(acc1, d1, d1);
    acc2 = svdot_u32(acc2, d2, d2);
    acc3 = svdot_u32(acc3, d3, d3);
  }
  out[0] = (int32_t)svaddv_u32(svptrue_b32(), acc0);
  out[1] = (int32_t)svaddv_u32(svptrue_b32(), acc1);
  out[2] = (int32_t)svaddv_u32(svptrue_b32(), acc2);
  out[3] = (int32_t)svaddv_u32(svptrue_b32(), acc3);
}
#endif  // __aarch64__ && __ARM_FEATURE_SVE
