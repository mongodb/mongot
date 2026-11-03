package com.xgen.mongot.circuitbreaker;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;

public class QueryFootprintTest {

  private static final long MIN_QUERY_BYTES = 32L * 1024 * 1024;

  @Test
  public void isSmallAfter_trueOnlyWhileUnderTheMinimum() {
    QueryFootprint footprint = new QueryFootprint(1);
    footprint.reserve(MIN_QUERY_BYTES - 2);

    assertThat(footprint.isSmallAfter(0, MIN_QUERY_BYTES)).isTrue();
    assertThat(footprint.isSmallAfter(1, MIN_QUERY_BYTES)).isTrue();
    assertThat(footprint.isSmallAfter(2, MIN_QUERY_BYTES)).isFalse();
  }
}
