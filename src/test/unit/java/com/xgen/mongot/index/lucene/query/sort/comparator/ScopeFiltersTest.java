package com.xgen.mongot.index.lucene.query.sort.comparator;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.lucene.search.Pruning;
import org.junit.Test;

public class ScopeFiltersTest {

  @Test
  public void requirePruningDisabled_pruningNone_doesNotThrow() {
    ScopeFilters.requirePruningDisabled(Pruning.NONE);
  }

  @Test
  public void requirePruningDisabled_pruningEnabled_throws() {
    // A scoped sort reads block-join reduced doc-values that serve advanceExact only; reaching a
    // pruning-enabled path must fail loudly rather than crash as an UnsupportedOperationException
    // deep in collection.
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> ScopeFilters.requirePruningDisabled(Pruning.GREATER_THAN));

    assertThat(thrown).hasMessageThat().contains("pruning disabled");
  }
}
