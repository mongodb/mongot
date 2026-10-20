package com.xgen.mongot.index.lucene.query.sort.comparator;

import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.join.BitSetProducer;

/**
 * The block-join filters that scope an embeddedDoc-scoped sort to a parent's matching children:
 * {@code parents} marks the root doc of each block, {@code children} marks the child docs the scope
 * operator selected.
 *
 * <p>Bundling the two producers enforces the "both or neither" invariant. A scoped sort always has
 * <em>both</em> filters, so callers hold a single {@code ScopeFilters} (or an {@code
 * Optional<ScopeFilters>} where scoped is optional) rather than two independent {@code
 * Optional<BitSetProducer>}s that could drift out of sync into a silently half-scoped sort. See
 * CLOUDP-441844.
 */
public record ScopeFilters(BitSetProducer parents, BitSetProducer children) {

  /**
   * Fails loudly if a scoped sort is about to run with pruning enabled. The block-join reduced
   * doc-values a scoped sort injects ({@link BlockJoinReducedNumericDocValues}, {@link
   * BlockJoinReducedSortedDocValues}) support {@code advanceExact} only; pruning would drive them
   * as a competitive iterator via {@code nextDoc}/{@code advance}, which they cannot serve. The
   * scoped engines always disable pruning, so this only trips if that invariant is ever broken by a
   * future change -- and then it trips early and clearly rather than as a bare {@code
   * UnsupportedOperationException} deep inside collection. See CLOUDP-441845.
   */
  public static void requirePruningDisabled(Pruning pruning) {
    if (pruning != Pruning.NONE) {
      throw new IllegalStateException(
          "embeddedDoc-scoped sort requires pruning disabled: its block-join reduced doc-values "
              + "support advanceExact only, but pruning ("
              + pruning
              + ") drives them as a competitive iterator");
    }
  }
}
