package com.xgen.testing.mongot.metrics;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

/**
 * Asserts on the untagged counter with a given name, or narrows to a tagged one.
 *
 * <p>{@link #withTags(Tags)} returns a {@link SingleCounterDefinedTags}, which has no narrowing
 * method of its own. A second narrowing is therefore a compile error rather than a silent override
 * of the first one.
 */
public interface SingleCounterEmptyTags extends SingleCounterDefinedTags {

  /** Narrows to the counter carrying exactly {@code tags}, on top of the factory's own tags. */
  SingleCounterDefinedTags withTags(Tag... tags);

  /** Narrows to the counter carrying exactly {@code tags}, on top of the factory's own tags. */
  SingleCounterDefinedTags withTags(Tags tags);
}
