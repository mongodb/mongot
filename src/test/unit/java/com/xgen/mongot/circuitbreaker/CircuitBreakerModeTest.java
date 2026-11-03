package com.xgen.mongot.circuitbreaker;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class CircuitBreakerModeTest {

  @Test
  public void of_whenEnforcementIsOff_runsInShadowMode() {
    CircuitBreakerMode mode = CircuitBreakerMode.of(false);

    assertThat(mode).isEqualTo(CircuitBreakerMode.SHADOW);
  }

  @Test
  public void of_whenEnforcementIsOn_enforces() {
    CircuitBreakerMode mode = CircuitBreakerMode.of(true);

    assertThat(mode).isEqualTo(CircuitBreakerMode.ENFORCING);
  }

  @Test
  public void isEnforcing_onlyForEnforcing() {
    assertThat(CircuitBreakerMode.SHADOW.isEnforcing()).isFalse();
    assertThat(CircuitBreakerMode.ENFORCING.isEnforcing()).isTrue();
  }
}
