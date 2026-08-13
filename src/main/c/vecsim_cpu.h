// Runtime CPU feature detection shared between the mongot_vecsim dispatcher and
// its unit test.  These helpers are compiled at baseline and are safe to call
// on any CPU of the target architecture: they only probe for feature support,
// they never execute the accelerated instructions themselves.
#ifndef SRC_MAIN_C_VECSIM_CPU_H_
#define SRC_MAIN_C_VECSIM_CPU_H_

#include <stddef.h>

#if defined(__aarch64__)
#if defined(__APPLE__)
#include <sys/sysctl.h>
#elif defined(__linux__)
#include <asm/hwcap.h>
#include <sys/auxv.h>
#endif

// True if the CPU implements the Advanced SIMD dot product extension (FEAT_DotProd).
static inline int vecsim_has_dotprod(void) {
#if defined(__APPLE__)
  int val = 0;
  size_t len = sizeof(val);
  if (sysctlbyname("hw.optional.arm.FEAT_DotProd", &val, &len, NULL, 0) == 0)
    return val;
  return 0;
#elif defined(__linux__)
#ifndef HWCAP_ASIMDDP
#define HWCAP_ASIMDDP (1 << 20)
#endif
  return (getauxval(AT_HWCAP) & HWCAP_ASIMDDP) != 0;
#else
  return 0;
#endif
}

// True if the CPU implements SVE.  Apple SVE is 128-bit (same width as NEON) so
// it offers no throughput benefit here; report it as unavailable there.
static inline int vecsim_has_sve(void) {
#if defined(__linux__)
#ifndef HWCAP_SVE
#define HWCAP_SVE (1 << 22)
#endif
  return (getauxval(AT_HWCAP) & HWCAP_SVE) != 0;
#else
  return 0;
#endif
}
#endif  // __aarch64__

#if defined(__x86_64__)
#include <cpuid.h>
#include <immintrin.h>

// True if the CPU and OS support the AVX-512 subsets the VNNI kernels use.
// target("xsave") enables the _xgetbv intrinsic; the xgetbv instruction itself
// is only executed after the OSXSAVE check below confirms it is available.
__attribute__((target("xsave"))) static inline int vecsim_has_avx512vnni(void) {
  // Detect via CPUID/XGETBV rather than __builtin_cpu_supports: that builtin
  // references the __cpu_features2 symbol with a PC-relative relocation that
  // cannot be linked into a shared object under this toolchain.
  unsigned eax, ebx, ecx, edx;
  if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx))
    return 0;
  // Require OSXSAVE (CPUID.1:ECX[27]) before reading XCR0.
  if (!(ecx & (1u << 27)))
    return 0;
  // Confirm the OS saves the state AVX-512 needs: XMM, YMM, opmask,
  // ZMM_Hi256, and Hi16_ZMM (XCR0 bits 1, 2, 5, 6, 7).
  const unsigned kAvx512State = (1u << 1) | (1u << 2) | (1u << 5) | (1u << 6) | (1u << 7);
  if ((_xgetbv(0) & kAvx512State) != kAvx512State)
    return 0;
  // CPUID leaf 7, subleaf 0: AVX512F=EBX[16], AVX512BW=EBX[30], AVX512VNNI=ECX[11].
  if (!__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx))
    return 0;
  return (ebx & (1u << 16)) && (ebx & (1u << 30)) && (ecx & (1u << 11));
}
#endif  // __x86_64__

#endif  // SRC_MAIN_C_VECSIM_CPU_H_
