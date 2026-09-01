package com.xgen.testing.mongot.metrics;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

/**
 * Asserts on every counter with a given name whatever its tags, or narrows to a subset of them.
 *
 * <p>{@link #containingTags(Tags)} returns a {@link MultiCounterDefinedTags}, which has no
 * narrowing method of its own. A second narrowing is therefore a compile error rather than a silent
 * override of the first one.
 */
public interface MultiCounterEmptyTags extends MultiCounterDefinedTags {

  /** Narrows to the counters carrying at least {@code tags}, on top of the factory's own tags. */
  MultiCounterDefinedTags containingTags(Tag... tags);

  /** Narrows to the counters carrying at least {@code tags}, on top of the factory's own tags. */
  MultiCounterDefinedTags containingTags(Tags tags);
}
