// Runtime dispatcher for the int8 vector-similarity kernels.
//
// Each accelerated kernel family lives in its own cc_library so that CPU
// feature flags (-march / -m...) are scoped to just those kernels.  This
// translation unit is compiled at baseline: it detects CPU features and wires
// the exported entry points to the fastest kernel the host actually supports.
//
// The set of kernels linked into the shared object is platform-specific, so the
// build passes MONGOT_VECSIM_HAVE_* defines describing which kernel libraries
// are present; this file only references kernels guarded by the matching define.
#define EXPORT __attribute__((visibility("default")))

#include <errno.h>
#include <math.h>
#include <setjmp.h>
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "src/main/c/vecsim_cpu.h"
#include "src/main/c/vecsim_kernels.h"

// --- Function pointers, initialised at load time ---

typedef int32_t (*i8_fn)(const int8_t*, const int8_t*, size_t);
typedef float (*cos_i8_fn)(const int8_t*, const int8_t*, size_t);

// Fused 4-wide dot / L2 (CLOUDP-435168). NULL on tiers without a fused kernel; the bulk exports
// then fall back to four calls to dot_i8_impl/l2_i8_impl, which keeps bulk results bit-identical
// to the single-vector path either way. Which tiers bind what, and why, is documented at the
// binding sites in mongot_vecsim_init.
typedef void (*i8_bulk4_fn)(const int8_t*, const int8_t*, const int8_t*, const int8_t*,
                            const int8_t*, int32_t*, size_t);

static i8_fn    dot_i8_impl;
static i8_fn    l2_i8_impl;
static cos_i8_fn cos_i8_impl;
static i8_bulk4_fn dot_i8_bulk4_impl;
static i8_bulk4_fn l2_i8_bulk4_impl;

// --- Execution probe ---
//
// CPU-feature bits (vecsim_has_avx512vnni/vecsim_has_sve/vecsim_has_dotprod) only report what the CPU or
// hypervisor *claims* to support; they don't confirm the instruction actually runs. A
// wrong report (VM CPUID passthrough bugs, cgroup/seccomp masking, kernel HWCAP bugs)
// would otherwise bind a kernel that SIGILLs on its first real call. The probe below
// actually executes a candidate kernel once before trusting it: if it traps, we fall
// back to the next tier down instead of crashing.
//
// sim_probe_i8_in_process (test-only) keeps the in-parent handler swap so
// mongot_vecsim_probe_test can verify save/restore without a JVM.

#define SIM_PROBE_N 128  // Must clear every kernel's widest SIMD loop (avx512vnni needs n>=64).

// Child exit code when the probed kernel completed without trapping.
#define SIM_PROBE_CHILD_OK 42

// Child exit code when the probed kernel failed to set up (couldn't allocate memory, etc.)
#define SIM_PROBE_CHILD_SETUP_FAILED 2

// Worst-case time we'll wait for a probe's forked child before giving up on that tier.
// The probe body is one short kernel call, so 2s is generous slack, not a tight bound.
#define SIM_PROBE_TIMEOUT_US (2 * 1000 * 1000)
#define SIM_PROBE_POLL_INTERVAL_US 1000

static sigjmp_buf g_sim_probe_jmpbuf;
static volatile sig_atomic_t g_sim_probe_active;
static struct sigaction g_sim_probe_old_action;

static void sim_probe_sigill_handler(int sig) {
  if (g_sim_probe_active) {
    siglongjmp(g_sim_probe_jmpbuf, 1);
  }
  sigaction(SIGILL, &g_sim_probe_old_action, NULL);
  raise(sig);
}

#if defined(MONGOT_VECSIM_TEST)
// In-process probe for tests only. Never used on the production init path (sim_probe_i8
// below is), and only compiled in test builds so it isn't left unused in production.
static int sim_probe_i8_in_process(i8_fn fn) {
  int8_t probe_a[SIM_PROBE_N] = {0};
  int8_t probe_b[SIM_PROBE_N] = {0};

  struct sigaction probe_action;
  memset(&probe_action, 0, sizeof(probe_action));
  probe_action.sa_handler = sim_probe_sigill_handler;
  sigemptyset(&probe_action.sa_mask);

  if (sigaction(SIGILL, &probe_action, &g_sim_probe_old_action) != 0) {
    return 0;
  }

  g_sim_probe_active = 1;
  int ok;
  if (sigsetjmp(g_sim_probe_jmpbuf, 1) == 0) {
    fn(probe_a, probe_b, SIM_PROBE_N);
    ok = 1;
  } else {
    ok = 0;
  }
  g_sim_probe_active = 0;

  sigaction(SIGILL, &g_sim_probe_old_action, NULL);
  return ok;
}
#endif  // MONGOT_VECSIM_TEST

// waitpid() wrapper that retries on EINTR (an unrelated signal interrupting the syscall)
// instead of treating it as the child's own failure
static pid_t sim_waitpid_retry_eintr(pid_t pid, int* status, int options) {
  pid_t result;
  do {
    result = waitpid(pid, status, options);
  } while (result < 0 && errno == EINTR);
  return result;
}

// Fork a child to run fn once; parent never touches SIGILL. fn's address is valid in
// the child (fork snapshot). Returns 1 if the kernel ran without trapping, 0 otherwise.
static int sim_probe_i8(i8_fn fn) {
  pid_t pid = fork();
  if (pid < 0) {
    fprintf(stderr, "mongot_vecsim: fork() failed during kernel probe: %s\n", strerror(errno));
    return 0;
  }
  if (pid == 0) {
    int8_t probe_a[SIM_PROBE_N] = {0};
    int8_t probe_b[SIM_PROBE_N] = {0};

    struct sigaction probe_action;
    memset(&probe_action, 0, sizeof(probe_action));
    probe_action.sa_handler = sim_probe_sigill_handler;
    sigemptyset(&probe_action.sa_mask);
    if (sigaction(SIGILL, &probe_action, &g_sim_probe_old_action) != 0) {
      _exit(SIM_PROBE_CHILD_SETUP_FAILED);
    }

    g_sim_probe_active = 1;
    int ok;
    if (sigsetjmp(g_sim_probe_jmpbuf, 1) == 0) {
      fn(probe_a, probe_b, SIM_PROBE_N);
      ok = 1;
    } else {
      ok = 0;
    }
    g_sim_probe_active = 0;
    _exit(ok ? SIM_PROBE_CHILD_OK : 1);
  }

  // Bounded wait: the probe body is a single short-lived call to fn followed by _exit(),
  // so it should return almost immediately.
  int status = 0;
  int elapsed_us = 0;
  pid_t waited;
  while ((waited = sim_waitpid_retry_eintr(pid, &status, WNOHANG)) == 0) {
    if (elapsed_us >= SIM_PROBE_TIMEOUT_US) {
      fprintf(stderr, "mongot_vecsim: kernel probe timed out after %dus; killing child\n",
              SIM_PROBE_TIMEOUT_US);
      kill(pid, SIGKILL);
      // Reap the now-dead child so it doesn't linger as a zombie.
      sim_waitpid_retry_eintr(pid, &status, 0);
      return 0;
    }
    usleep(SIM_PROBE_POLL_INTERVAL_US);
    elapsed_us += SIM_PROBE_POLL_INTERVAL_US;
  }
  if (waited < 0) {
    fprintf(stderr, "mongot_vecsim: waitpid() failed during kernel probe: %s\n", strerror(errno));
    return 0;
  }
  if (!WIFEXITED(status)) {
    fprintf(stderr,
            "mongot_vecsim: kernel probe child exited abnormally (%s)\n",
            WIFSIGNALED(status) ? strsignal(WTERMSIG(status)) : "not exited, not signaled");
    return 0;
  }
  if (WEXITSTATUS(status) == SIM_PROBE_CHILD_SETUP_FAILED) {
    fprintf(stderr, "mongot_vecsim: kernel probe child failed to install its SIGILL handler\n");
    return 0;
  }
  if (WEXITSTATUS(status) != SIM_PROBE_CHILD_OK) {
    fprintf(stderr, "mongot_vecsim: kernel probe child caught an instruction trap\n");
    return 0;
  }
  return 1;
}

/// Run the SIGILL-safe execution probe against a single kernel; returns 1 if it ran
/// without trapping.  Exported so tests can confirm a kernel is actually executable on
/// this host before calling it directly: the vecsim_has_* feature bits only report what
/// the CPU claims, and calling an unprobed kernel on a lying host would kill the test
/// process instead of reporting a result.
EXPORT int mongot_vecsim_probe_i8_kernel(int32_t (*fn)(const int8_t*, const int8_t*, size_t)) {
  return sim_probe_i8(fn);
}

static void sim_log_fallback(const char* tier) {
  fprintf(stderr,
          "mongot_vecsim: %s kernel failed its execution probe (CPU reported support, but "
          "the probe did not confirm it works: instruction trap, or a fork/wait error); "
          "falling back\n",
          tier);
}

#if defined(MONGOT_VECSIM_TEST)
// Test-only: a "kernel" that always SIGILLs, for exercising sim_probe_i8's fallback and
// handler-restore behavior without needing genuinely mismatched hardware.
static int32_t sim_test_force_sigill_kernel(const int8_t* a, const int8_t* b, size_t n) {
  (void)a;
  (void)b;
  (void)n;
  raise(SIGILL);
  return 0;  // Unreachable.
}

// Returns 1 if the in-process probe correctly caught the forced fault and reported failure.
EXPORT int mongot_vecsim_test_probe_force_sigill(void) {
  return sim_probe_i8_in_process(sim_test_force_sigill_kernel) == 0;
}

// Returns 1 if the forked probe correctly caught the forced fault and reported failure.
EXPORT int mongot_vecsim_test_probe_force_sigill_forked(void) {
  return sim_probe_i8(sim_test_force_sigill_kernel) == 0;
}
#endif  // MONGOT_VECSIM_TEST

// Exported so tests can invoke dispatch directly and confirm production probing never
// touches the caller's SIGILL handler; the constructor below is what makes this run
// automatically in production.
EXPORT void mongot_vecsim_init(void) {
  // Set before the tier dispatch below, which returns early on the first tier that probes clean.
  // NEON-dotprod and SVE bind real fused kernels below; every other tier (including AVX-512VNNI,
  // deliberately -- see below) leaves both NULL, so the bulk exports use their four-call fallback.
  dot_i8_bulk4_impl = NULL;
  l2_i8_bulk4_impl = NULL;
#if defined(MONGOT_VECSIM_HAVE_AVX512)
  int has_vnni = vecsim_has_avx512vnni();
  if (has_vnni && sim_probe_i8(sim_dot_i8_avx512vnni)) {
    dot_i8_impl = sim_dot_i8_avx512vnni;
    l2_i8_impl  = sim_l2_i8_avx512vnni;
    cos_i8_impl = sim_cos_i8_avx512vnni;
    // CLOUDP-435168: no fused AVX-512VNNI bulk kernels here. A first attempt measured ~3%
    // slower than this four-call fallback on X86_64 M40-S60 (DSI, voyage 129GB Q7 initial sync),
    // the same under-unroll failure the NEON/SVE kernels hit on their first attempt. Re-add
    // retuned to the reference kernel's unroll depth, and only with a DSI result that beats
    // this fallback.
    return;
  }
  if (has_vnni) {
    sim_log_fallback("avx512vnni");
  }
  dot_i8_impl = sim_dot_i8_scalar;
  l2_i8_impl  = sim_l2_i8_scalar;
  cos_i8_impl = sim_cos_i8_scalar;
#elif defined(MONGOT_VECSIM_HAVE_SVE) || defined(MONGOT_VECSIM_HAVE_NEON_DOTPROD)
#if defined(MONGOT_VECSIM_HAVE_SVE)
  if (vecsim_has_sve()) {
    if (sim_probe_i8(sim_dot_i8_sve)) {
      dot_i8_impl = sim_dot_i8_sve;
      l2_i8_impl  = sim_l2_i8_sve;
      cos_i8_impl = sim_cos_i8_sve;
      // CLOUDP-435168: sim_{dot,l2}_i8_bulk4_sve use the same svdot_s32/svdot_u32
      // instructions already probed above, so no separate probe is needed for them.
      dot_i8_bulk4_impl = sim_dot_i8_bulk4_sve;
      l2_i8_bulk4_impl  = sim_l2_i8_bulk4_sve;
      return;
    }
    sim_log_fallback("sve");
  }
#endif
#if defined(MONGOT_VECSIM_HAVE_NEON_DOTPROD)
  if (vecsim_has_dotprod()) {
    if (sim_probe_i8(sim_dot_i8_dotprod)) {
      dot_i8_impl = sim_dot_i8_dotprod;
      l2_i8_impl  = sim_l2_i8_dotprod;
      cos_i8_impl = sim_cos_i8_dotprod;
      // CLOUDP-435168: sim_{dot,l2}_i8_bulk4_dotprod use the same vdotq_s32/vdotq_u32
      // instructions already probed above, so no separate probe is needed for them.
      dot_i8_bulk4_impl = sim_dot_i8_bulk4_dotprod;
      l2_i8_bulk4_impl  = sim_l2_i8_bulk4_dotprod;
      return;
    }
    sim_log_fallback("dotprod");
  }
#endif
  dot_i8_impl = sim_dot_i8_scalar;
  l2_i8_impl  = sim_l2_i8_scalar;
  cos_i8_impl = sim_cos_i8_scalar;
#else
  dot_i8_impl = sim_dot_i8_scalar;
  l2_i8_impl  = sim_l2_i8_scalar;
  cos_i8_impl = sim_cos_i8_scalar;
#endif
}

__attribute__((constructor))
static void sim_init(void) {
  mongot_vecsim_init();
}

// --- Exported functions ---

/// Compute the dot product of two int8 vectors of length n.
EXPORT int32_t mongot_vecsim_dot_i8(const int8_t* a, const int8_t* b, size_t n) {
  return dot_i8_impl(a, b, n);
}

/// Compute the squared L2 distance of two int8 vectors of length n.
EXPORT int32_t mongot_vecsim_l2_i8(const int8_t* a, const int8_t* b, size_t n) {
  return l2_i8_impl(a, b, n);
}

/// Compute the cosine similarity of two int8 vectors of length n.
EXPORT float mongot_vecsim_cos_i8(const int8_t* a, const int8_t* b, size_t n) {
  return cos_i8_impl(a, b, n);
}

// --- Fused 4-wide int8 forms ---
//
// One query scored against VECSIM_BULK_FANOUT data vectors per call: a base pointer plus byte
// offsets instead of four separate vector pointers, so a batch costs one FFM downcall and no
// per-node MemorySegment slicing on the Java side. Unlike the float32 bulk ABI there is no unit
// asymmetry here: q_off, d0..d3 and n are all byte counts.
//
// Returns the raw integer dot product / squared distance; Q7's score correction (constMultiplier
// and the per-vector correction constants) is applied by the caller in Java, so there are no float
// parameters and no per-similarity-function variants.
//
// `out` must not alias `base`. The d* offsets need not be distinct, so no v* pointer may be
// marked `restrict`.

/// Fan-out of the bulk int8 kernels. Mirrored by BULK_FANOUT in Similarity.java -- changing one
/// without the other overruns `out`.
#define VECSIM_BULK_FANOUT 4

#define VECSIM_I8_AT(base, off) ((const int8_t*)((const unsigned char*)(base) + (off)))

/// Dot product of one query against VECSIM_BULK_FANOUT data vectors; writes that many int32s to out.
EXPORT void mongot_vecsim_dot_i8_bulk4(const void* q_base, size_t q_off, const void* base,
                                       int32_t* out, size_t d0, size_t d1, size_t d2, size_t d3,
                                       size_t n) {
  const int8_t* q = VECSIM_I8_AT(q_base, q_off);
  const int8_t* v0 = VECSIM_I8_AT(base, d0);
  const int8_t* v1 = VECSIM_I8_AT(base, d1);
  const int8_t* v2 = VECSIM_I8_AT(base, d2);
  const int8_t* v3 = VECSIM_I8_AT(base, d3);
  if (dot_i8_bulk4_impl != NULL) {
    dot_i8_bulk4_impl(q, v0, v1, v2, v3, out, n);
    return;
  }
  out[0] = dot_i8_impl(q, v0, n);
  out[1] = dot_i8_impl(q, v1, n);
  out[2] = dot_i8_impl(q, v2, n);
  out[3] = dot_i8_impl(q, v3, n);
}

/// Squared L2 distance of one query against VECSIM_BULK_FANOUT data vectors.
EXPORT void mongot_vecsim_l2_i8_bulk4(const void* q_base, size_t q_off, const void* base,
                                      int32_t* out, size_t d0, size_t d1, size_t d2, size_t d3,
                                      size_t n) {
  const int8_t* q = VECSIM_I8_AT(q_base, q_off);
  const int8_t* v0 = VECSIM_I8_AT(base, d0);
  const int8_t* v1 = VECSIM_I8_AT(base, d1);
  const int8_t* v2 = VECSIM_I8_AT(base, d2);
  const int8_t* v3 = VECSIM_I8_AT(base, d3);
  if (l2_i8_bulk4_impl != NULL) {
    l2_i8_bulk4_impl(q, v0, v1, v2, v3, out, n);
    return;
  }
  out[0] = l2_i8_impl(q, v0, n);
  out[1] = l2_i8_impl(q, v1, n);
  out[2] = l2_i8_impl(q, v2, n);
  out[3] = l2_i8_impl(q, v3, n);
}

/// Return true if all i8 similarity functions are using an accelerated (non-scalar) implementation.
EXPORT bool mongot_vecsim_i8_accelerated() {
  return dot_i8_impl != sim_dot_i8_scalar &&
         l2_i8_impl  != sim_l2_i8_scalar  &&
         cos_i8_impl != sim_cos_i8_scalar;
}
