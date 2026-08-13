// Scalar int8 similarity kernels.  Compiled at baseline (no -march), so these
// are the safe fallback used whenever no accelerated ISA is available.
#include "src/main/c/vecsim_kernels.h"

HIDDEN int32_t sim_dot_i8_scalar(const int8_t* a, const int8_t* b, size_t n) {
  int32_t sum = 0;
  for (size_t i = 0; i < n; ++i) {
    sum += a[i] * b[i];
  }
  return sum;
}

HIDDEN int32_t sim_l2_i8_scalar(const int8_t* a, const int8_t* b, size_t n) {
  int32_t sum = 0;
  for (size_t i = 0; i < n; ++i) {
    int32_t diff = (int32_t)a[i] - (int32_t)b[i];
    sum += diff * diff;
  }
  return sum;
}

HIDDEN float sim_cos_i8_scalar(const int8_t* a, const int8_t* b, size_t n) {
  int32_t dot = 0, norm_a = 0, norm_b = 0;
  for (size_t i = 0; i < n; ++i) {
    dot    += (int32_t)a[i] * (int32_t)b[i];
    norm_a += (int32_t)a[i] * (int32_t)a[i];
    norm_b += (int32_t)b[i] * (int32_t)b[i];
  }
  return sim_cos_combine(dot, norm_a, norm_b);
}
