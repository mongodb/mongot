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
#endif  // __aarch64__ && __ARM_FEATURE_SVE
