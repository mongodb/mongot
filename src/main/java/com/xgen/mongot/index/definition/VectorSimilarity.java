package com.xgen.mongot.index.definition;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.util.Enums;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import org.apache.lucene.index.VectorSimilarityFunction;

public enum VectorSimilarity {
  EUCLIDEAN(VectorSimilarityFunction.EUCLIDEAN),
  DOT_PRODUCT(VectorSimilarityFunction.DOT_PRODUCT),
  COSINE(VectorSimilarityFunction.COSINE);

  // camelCase name → enum, using the same derivation as the index-BSON parser
  // (enumField(...).asCamelCase()), so both spellings agree by construction.
  private static final ImmutableMap<String, VectorSimilarity> NAME_TO_SIMILARITY;

  static {
    NAME_TO_SIMILARITY =
        Arrays.stream(VectorSimilarity.values())
            .collect(
                ImmutableMap.toImmutableMap(
                    similarity -> Enums.convertNameTo(CaseFormat.LOWER_CAMEL, similarity),
                    Function.identity()));
  }

  private final VectorSimilarityFunction luceneSimilarityFunction;

  VectorSimilarity(VectorSimilarityFunction luceneSimilarityFunction) {
    this.luceneSimilarityFunction = luceneSimilarityFunction;
  }

  public VectorSimilarityFunction getLuceneSimilarityFunction() {
    return this.luceneSimilarityFunction;
  }

  /** The camelCase index-definition BSON spelling (for example {@code dotProduct}). */
  public String getName() {
    return Enums.convertNameTo(CaseFormat.LOWER_CAMEL, this);
  }

  /** Resolves the camelCase similarity name (for example {@code dotProduct}), empty if unknown. */
  public static Optional<VectorSimilarity> fromName(String name) {
    return Optional.ofNullable(NAME_TO_SIMILARITY.get(name));
  }
}
