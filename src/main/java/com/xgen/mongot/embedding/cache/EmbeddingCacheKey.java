package com.xgen.mongot.embedding.cache;

import com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;

/**
 * Identity of a cached embedding vector in an {@link EmbeddingRetryCache}.
 *
 * <p>Keys carry the SHA-256 hash of the source text rather than the text itself so that cache
 * entries do not retain large source strings. Two lookups share a vector only when every dimension
 * of the key (model, dimensions, quantization, input type, text hash) matches.
 */
public record EmbeddingCacheKey(
    String modelName,
    int numDimensions,
    VectorAutoEmbedQuantization quantization,
    InputType inputType,
    String textHash) {

  /**
   * The embedding provider's input type. Providers generate different vectors for the same text
   * depending on whether it is embedded as a document or as a query, so entries can never be shared
   * across input types. Indexing uses {@link #DOCUMENT}; {@link #QUERY} is reserved for a potential
   * future query-path integration.
   */
  public enum InputType {
    DOCUMENT,
    QUERY
  }

  /** Creates a key for the indexing (document) path, hashing the source text. */
  public static EmbeddingCacheKey forDocument(
      String modelName, int numDimensions, VectorAutoEmbedQuantization quantization, String text) {
    return new EmbeddingCacheKey(
        modelName,
        numDimensions,
        quantization,
        InputType.DOCUMENT,
        AutoEmbeddingDocumentUtils.computeTextHash(text));
  }
}
