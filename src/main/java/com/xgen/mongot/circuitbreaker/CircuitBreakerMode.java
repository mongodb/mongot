package com.xgen.mongot.circuitbreaker;

/**
 * How far the circuit breaker is allowed to go for one query.
 *
 * <p>Two values. Shadow mode is not a flag of its own: it is simply what runs when enforcement is
 * off, so measurement is always on and a kill switch cannot accidentally turn it off with the
 * enforcement it is meant to justify.
 */
public enum CircuitBreakerMode {
  /** Everything runs except refusal and cancellation. What would have happened is counted. */
  SHADOW,

  /** Reservations past the limit are refused, and the largest query cancels itself. */
  ENFORCING;

  /** Whether the breaker may refuse and cancel. */
  public boolean isEnforcing() {
    return this == ENFORCING;
  }

  /** Resolves the mode from the enforcement flag: off means shadow. */
  public static CircuitBreakerMode of(boolean enforcementEnabled) {
    return enforcementEnabled ? ENFORCING : SHADOW;
  }
}
