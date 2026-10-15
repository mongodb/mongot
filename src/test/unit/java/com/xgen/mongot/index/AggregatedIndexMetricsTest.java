package com.xgen.mongot.index;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.testing.mongot.index.IndexMetricsBuilder;
import com.xgen.testing.mongot.index.IndexMetricsBuilder.IndexingMetricsBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import java.util.List;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Test;

public class AggregatedIndexMetricsTest {

  private static final SearchIndexDefinition DEFINITION =
      SearchIndexDefinitionBuilder.builder()
          .indexId(new ObjectId("507f191e810c19729de860ea"))
          .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
          .name("index")
          .database("database")
          .lastObservedCollectionName("collection")
          .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
          .build();

  private static IndexMetrics withColdStorageBytes(long bytes) {
    return IndexMetricsBuilder.builder()
        .indexDefinition(DEFINITION)
        .indexingMetrics(IndexingMetricsBuilder.builder().coldStorageSizeBytes(bytes).build())
        .build();
  }

  @Test
  public void createFromOrderedMetrics_sumsColdStorageBytesAcrossGenerations() {
    var aggregated =
        AggregatedIndexMetrics.createFromOrderedMetrics(
            List.of(withColdStorageBytes(7), withColdStorageBytes(3)));

    assertThat(aggregated.coldStorageSizeBytes()).isEqualTo(10);
  }

  @Test
  public void createFromOrderedMetrics_emptyList_coldStorageBytesIsZero() {
    assertThat(AggregatedIndexMetrics.createFromOrderedMetrics(List.of()).coldStorageSizeBytes())
        .isEqualTo(0);
  }
}
