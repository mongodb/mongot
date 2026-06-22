package com.xgen.mongot.index.lucene.similarity;

import java.util.Map;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;

/**
 * A {@link Similarity} wrapper that delegates to field-specific similarities when available, and
 * falls back to a default similarity otherwise.
 *
 * <p>This class is useful when different fields in the index require different scoring models, such
 * as using {@code BooleanSimilarity} for one field and {@code BM25Similarity} for another.
 *
 * <p>The field-to-similarity mapping is provided at construction time and is immutable.
 */
public class SimilarityWrapper extends PerFieldSimilarityWrapper {
  private final Map<String, Similarity> fieldSimilarities;
  private final Similarity defaultSimilarity;

  public SimilarityWrapper(
      Map<String, Similarity> fieldSimilarities, Similarity defaultSimilarity) {
    this.fieldSimilarities = fieldSimilarities;
    this.defaultSimilarity = defaultSimilarity;
  }

  @Override
  public final Similarity get(String name) {
    return this.fieldSimilarities.getOrDefault(name, this.defaultSimilarity);
  }
}
