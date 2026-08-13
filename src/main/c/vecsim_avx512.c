// x86_64 AVX-512 VNNI int8 similarity kernels.  The component library is
// compiled with -mavx512f -mavx512bw -mavx512vnni, so no per-function target
// attribute is needed.  The runtime guard for these features lives in the
// dispatcher (has_avx512vnni); these functions are only invoked once it returns
// true.
#include "src/main/c/vecsim_kernels.h"

#if defined(__x86_64__)
#include <immintrin.h>

// Load 32 signed i8 via a 256-bit load, widen to i16, then accumulate pairs
// into i32 with vpdpwssd (AVX-512VNNI).  Avoids the XOR-128 bias correction
// needed by vpdpbusd and uses fewer zmm registers than a 512-bit load path.
HIDDEN int32_t sim_dot_i8_avx512vnni(const int8_t* a, const int8_t* b, size_t n) {
  __m512i acc0 = _mm512_setzero_si512();
  __m512i acc1 = _mm512_setzero_si512();
  size_t i = 0;
  for (; i + 63 < n; i += 64) {
    __m512i a0 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(a + i)));
    __m512i b0 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(b + i)));
    __m512i a1 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(a + i + 32)));
    __m512i b1 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(b + i + 32)));
    acc0 = _mm512_dpwssd_epi32(acc0, a0, b0);
    acc1 = _mm512_dpwssd_epi32(acc1, a1, b1);
  }
  acc0 = _mm512_add_epi32(acc0, acc1);
  for (; i + 31 < n; i += 32) {
    __m512i ai = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(a + i)));
    __m512i bi = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(b + i)));
    acc0 = _mm512_dpwssd_epi32(acc0, ai, bi);
  }
  int32_t sum = _mm512_reduce_add_epi32(acc0);
  for (; i < n; ++i) sum += (int32_t)a[i] * (int32_t)b[i];
  return sum;
}

HIDDEN int32_t sim_l2_i8_avx512vnni(const int8_t* a, const int8_t* b, size_t n) {
  __m512i acc0 = _mm512_setzero_si512();
  __m512i acc1 = _mm512_setzero_si512();
  size_t i = 0;
  for (; i + 63 < n; i += 64) {
    __m512i a0 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(a + i)));
    __m512i b0 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(b + i)));
    __m512i a1 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(a + i + 32)));
    __m512i b1 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(b + i + 32)));
    __m512i d0 = _mm512_sub_epi16(a0, b0);
    __m512i d1 = _mm512_sub_epi16(a1, b1);
    acc0 = _mm512_dpwssd_epi32(acc0, d0, d0);
    acc1 = _mm512_dpwssd_epi32(acc1, d1, d1);
  }
  acc0 = _mm512_add_epi32(acc0, acc1);
  for (; i + 31 < n; i += 32) {
    __m512i di = _mm512_sub_epi16(_mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(a + i))),
                                  _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(b + i))));
    acc0 = _mm512_dpwssd_epi32(acc0, di, di);
  }
  int32_t sum = _mm512_reduce_add_epi32(acc0);
  for (; i < n; ++i) {
    int32_t diff = (int32_t)a[i] - (int32_t)b[i];
    sum += diff * diff;
  }
  return sum;
}

HIDDEN float sim_cos_i8_avx512vnni(const int8_t* a, const int8_t* b, size_t n) {
  __m512i ab0 = _mm512_setzero_si512(), ab1 = _mm512_setzero_si512();
  __m512i aa0 = _mm512_setzero_si512(), aa1 = _mm512_setzero_si512();
  __m512i bb0 = _mm512_setzero_si512(), bb1 = _mm512_setzero_si512();
  size_t i = 0;
  for (; i + 63 < n; i += 64) {
    __m512i a0 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(a + i)));
    __m512i b0 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(b + i)));
    __m512i a1 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(a + i + 32)));
    __m512i b1 = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(b + i + 32)));
    ab0 = _mm512_dpwssd_epi32(ab0, a0, b0);
    aa0 = _mm512_dpwssd_epi32(aa0, a0, a0);
    bb0 = _mm512_dpwssd_epi32(bb0, b0, b0);
    ab1 = _mm512_dpwssd_epi32(ab1, a1, b1);
    aa1 = _mm512_dpwssd_epi32(aa1, a1, a1);
    bb1 = _mm512_dpwssd_epi32(bb1, b1, b1);
  }
  __m512i ab = _mm512_add_epi32(ab0, ab1);
  __m512i aa = _mm512_add_epi32(aa0, aa1);
  __m512i bb = _mm512_add_epi32(bb0, bb1);
  for (; i + 31 < n; i += 32) {
    __m512i ai = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(a + i)));
    __m512i bi = _mm512_cvtepi8_epi16(_mm256_loadu_si256((const __m256i*)(b + i)));
    ab = _mm512_dpwssd_epi32(ab, ai, bi);
    aa = _mm512_dpwssd_epi32(aa, ai, ai);
    bb = _mm512_dpwssd_epi32(bb, bi, bi);
  }
  int32_t dot    = _mm512_reduce_add_epi32(ab);
  int32_t norm_a = _mm512_reduce_add_epi32(aa);
  int32_t norm_b = _mm512_reduce_add_epi32(bb);
  for (; i < n; ++i) {
    dot    += (int32_t)a[i] * (int32_t)b[i];
    norm_a += (int32_t)a[i] * (int32_t)a[i];
    norm_b += (int32_t)b[i] * (int32_t)b[i];
  }
  return sim_cos_combine(dot, norm_a, norm_b);
}
#endif  // __x86_64__
