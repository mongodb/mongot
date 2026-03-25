package com.xgen.mongot.index;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.concurrent.atomic.AtomicLong;

public final class IndexTypeData {

  private IndexTypeData() {}

  public static final String INDEX_TYPE_TAG_NAME = "indexType";

  /**
   * IndexTypeTag to provide more granular workload types besides index type in
   * IndexDefinition.Type, we use this enum type to provide workload isolation and metrics
   * reporting.
   */
  public enum IndexTypeTag {
    TAG_SEARCH("search"),
    TAG_SEARCH_AUTO_EMBEDDING("search_auto_embedding"),
    TAG_VECTOR_SEARCH("vector_search"),
    // TODO(CLOUDP-390796): Clean up this Tag, should just use replicationType tag once type:text is
    // deprecated.
    // Auto-embedding vector search workload
    TAG_VECTOR_SEARCH_AUTO_EMBEDDING("vector_search_auto_embedding");

    IndexTypeTag(String tagValue) {
      this.tagValue = tagValue;
    }

    public final String tagValue;
  }

  public static IndexTypeTag getIndexTypeTag(IndexDefinition indexDefinition) {
    return switch (indexDefinition.getType()) {
      case SEARCH ->
          indexDefinition.isAutoEmbeddingIndex()
              ? IndexTypeTag.TAG_SEARCH_AUTO_EMBEDDING
              : IndexTypeTag.TAG_SEARCH;
      case VECTOR_SEARCH ->
          indexDefinition.isAutoEmbeddingIndex()
              ? IndexTypeTag.TAG_VECTOR_SEARCH_AUTO_EMBEDDING
              : IndexTypeTag.TAG_VECTOR_SEARCH;
    };
  }

  public static AtomicLong getNumGauge(
      MetricsFactory metricsFactory, String gaugeName, IndexTypeTag indexTypeTag, Tag extraTag) {
    return metricsFactory.numGauge(
        gaugeName, Tags.of(INDEX_TYPE_TAG_NAME, indexTypeTag.tagValue).and(extraTag));
  }
}
