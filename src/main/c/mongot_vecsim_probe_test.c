// Verifies the SIGILL-safe kernel-execution probe in mongot_vecsim.c:
//  1. mongot_vecsim_init() probes kernels in forked children, so the parent's SIGILL
//     handler (standing in for a JVM handler below) is never replaced during init.
//  2. mongot_vecsim_test_probe_force_sigill() exercises the in-process probe path
//     (test-only): forced SIGILL is caught, handler is restored, process does not leak
//     the temporary handler.
//  3. mongot_vecsim_test_probe_force_sigill_forked() exercises the same forced fault
//     through the production forked probe (sim_probe_i8), confirming the fallback path
//     is reachable and never touches the caller's SIGILL handler.
//
// This is the "must have" test from the SIGILL-safe validation plan: it doesn't require
// genuinely mismatched hardware (which isn't reproducible in CI), only the forced-fault
// hook compiled in under MONGOT_VECSIM_TEST.

#include <signal.h>
#include <stdio.h>
#include <string.h>

extern void mongot_vecsim_init(void);
extern int mongot_vecsim_test_probe_force_sigill(void);
extern int mongot_vecsim_test_probe_force_sigill_forked(void);

static void sentinel_handler(int sig) {
  (void)sig;
}

static int check(int cond, const char* what) {
  fprintf(stderr, "%s: %s\n", cond ? "PASS" : "FAIL", what);
  return cond;
}

int main(void) {
  int all_passed = 1;

  // Stands in for "whatever handler the host process (e.g. a JVM) had installed
  // before loading this library" -- the thing that must not get clobbered.
  struct sigaction sentinel_action;
  memset(&sentinel_action, 0, sizeof(sentinel_action));
  sentinel_action.sa_handler = sentinel_handler;
  sigemptyset(&sentinel_action.sa_mask);
  if (sigaction(SIGILL, &sentinel_action, NULL) != 0) {
    fprintf(stderr, "FAIL: could not install sentinel handler\n");
    return 1;
  }

  mongot_vecsim_init();

  struct sigaction after_init;
  sigaction(SIGILL, NULL, &after_init);
  all_passed &= check(after_init.sa_handler == sentinel_handler,
                       "mongot_vecsim_init() does not replace the caller's SIGILL handler");

  all_passed &= check(mongot_vecsim_test_probe_force_sigill(),
                       "a forced SIGILL during a probe is caught and reported as a failed probe");

  struct sigaction after_forced_probe;
  sigaction(SIGILL, NULL, &after_forced_probe);
  all_passed &= check(after_forced_probe.sa_handler == sentinel_handler,
                       "the forced-SIGILL probe also restores the caller's previous SIGILL handler");

  all_passed &= check(mongot_vecsim_test_probe_force_sigill_forked(),
                       "a forced SIGILL during a forked probe is caught and reported as a failed probe");

  struct sigaction after_forked_probe;
  sigaction(SIGILL, NULL, &after_forked_probe);
  all_passed &= check(after_forked_probe.sa_handler == sentinel_handler,
                       "the forked-SIGILL probe leaves the caller's SIGILL handler untouched");

  return all_passed ? 0 : 1;
}
