package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.index.DocCounts;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater.SourceCollectionDocCounts;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.CollectionStatsResponse;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.types.ObjectId;
import org.junit.Test;

public class SourceCollectionCountUpdaterTest {

  private static final MongoNamespace NAMESPACE = new MongoNamespace("database", "collection");

  @Test
  public void update_collectionIndex_setsCollectionAndIndexedCountsTogether() {
    IndexGeneration generation = generation(definitionBuilder().build());
    InitializedIndex index = SearchIndex.mockInitializedIndex(generation);
    IndexMetricsUpdater metricsUpdater = index.getMetricsUpdater();
    doReturn(new DocCounts(1_300, 1_300, 1_300, 1_200)).when(metricsUpdater).getDocCounts();
    CollectionStatsMongoClient statsClient = statsClientReturning(1_234);
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    try (SourceCollectionCountUpdater updater =
        newUpdater(statsClient, catalog(generation), initializedCatalog(index), meterRegistry)) {
      updater.update();
    }

    assertThat(sourceCollectionDocCounts(index))
        .hasValue(new SourceCollectionDocCounts(1_234, 1_200));
  }

  @Test
  public void update_twoIndexesOnSameCollection_fetchesCountOnce() {
    IndexGeneration firstGeneration = generation(definitionBuilder().build());
    IndexGeneration secondGeneration = generation(definitionBuilder().build());
    InitializedIndex first = SearchIndex.mockInitializedIndex(firstGeneration);
    InitializedIndex second = SearchIndex.mockInitializedIndex(secondGeneration);
    CollectionStatsMongoClient statsClient = statsClientReturning(1_234);
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    try (SourceCollectionCountUpdater updater =
        newUpdater(
            statsClient,
            catalog(firstGeneration, secondGeneration),
            initializedCatalog(first, second),
            meterRegistry)) {
      updater.update();
    }

    verify(statsClient, times(1)).getStats(NAMESPACE);
    assertThat(sourceCollectionDocCounts(first).map(SourceCollectionDocCounts::collectionDocs))
        .hasValue(1_234L);
    assertThat(sourceCollectionDocCounts(second).map(SourceCollectionDocCounts::collectionDocs))
        .hasValue(1_234L);
  }

  @Test
  public void update_collStatsFails_clearsCountAndIncrementsErrorCounter() {
    IndexGeneration generation = generation(definitionBuilder().build());
    InitializedIndex index = SearchIndex.mockInitializedIndex(generation);
    index
        .getMetricsUpdater()
        .getIndexingMetricsUpdater()
        .setSourceCollectionDocCounts(Optional.of(new SourceCollectionDocCounts(1_234, 1_234)));
    CollectionStatsMongoClient statsClient = mock(CollectionStatsMongoClient.class);
    when(statsClient.getStats(any())).thenThrow(new RuntimeException("collStats failed"));
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    try (SourceCollectionCountUpdater updater =
        newUpdater(statsClient, catalog(generation), initializedCatalog(index), meterRegistry)) {
      updater.update();
    }

    assertThat(sourceCollectionDocCounts(index)).isEmpty();
    assertThat(
            meterRegistry.counter(SourceCollectionCountUpdater.ERROR_COUNTER_METRIC_NAME).count())
        .isEqualTo(1.0);
  }

  @Test
  public void update_indexCountFails_clearsCountsAndIncrementsErrorCounter() {
    IndexGeneration generation = generation(definitionBuilder().build());
    InitializedIndex index = SearchIndex.mockInitializedIndex(generation);
    IndexMetricsUpdater metricsUpdater = index.getMetricsUpdater();
    metricsUpdater
        .getIndexingMetricsUpdater()
        .setSourceCollectionDocCounts(Optional.of(new SourceCollectionDocCounts(1_234, 1_234)));
    doThrow(new IllegalStateException("reader failed")).when(metricsUpdater).getDocCounts();
    CollectionStatsMongoClient statsClient = statsClientReturning(1_234);
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    try (SourceCollectionCountUpdater updater =
        newUpdater(statsClient, catalog(generation), initializedCatalog(index), meterRegistry)) {
      updater.update();
    }

    assertThat(sourceCollectionDocCounts(index)).isEmpty();
    assertThat(
            meterRegistry.counter(SourceCollectionCountUpdater.ERROR_COUNTER_METRIC_NAME).count())
        .isEqualTo(1.0);
  }

  @Test
  public void update_oneIndexFails_updatesOtherIndexesAndIncrementsErrorCounter() {
    IndexGeneration failingGeneration = generation(definitionBuilder().build());
    IndexGeneration healthyGeneration = generation(definitionBuilder().build());
    InitializedIndex failing = SearchIndex.mockInitializedIndex(failingGeneration);
    InitializedIndex healthy = SearchIndex.mockInitializedIndex(healthyGeneration);
    var failingMetricsUpdater = failing.getMetricsUpdater().getIndexingMetricsUpdater();
    doThrow(new IllegalStateException("metrics unavailable"))
        .when(failingMetricsUpdater)
        .setSourceCollectionDocCounts(any());
    CollectionStatsMongoClient statsClient = statsClientReturning(1_234);
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    try (SourceCollectionCountUpdater updater =
        newUpdater(
            statsClient,
            catalog(failingGeneration, healthyGeneration),
            initializedCatalog(failing, healthy),
            meterRegistry)) {
      updater.update();
    }

    assertThat(sourceCollectionDocCounts(healthy).map(SourceCollectionDocCounts::collectionDocs))
        .hasValue(1_234L);
    assertThat(
            meterRegistry.counter(SourceCollectionCountUpdater.ERROR_COUNTER_METRIC_NAME).count())
        .isEqualTo(1.0);
  }

  @Test
  public void update_viewWithMatch_clearsCountWithoutFetching() {
    IndexGeneration generation =
        generation(
            definitionBuilder()
                .view(ViewDefinition.existing("view", List.of(matchStage())))
                .build());
    InitializedIndex index = SearchIndex.mockInitializedIndex(generation);
    index
        .getMetricsUpdater()
        .getIndexingMetricsUpdater()
        .setSourceCollectionDocCounts(Optional.of(new SourceCollectionDocCounts(1_234, 1_234)));
    CollectionStatsMongoClient statsClient = statsClientReturning(1_234);
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    try (SourceCollectionCountUpdater updater =
        newUpdater(statsClient, catalog(generation), initializedCatalog(index), meterRegistry)) {
      updater.update();
    }

    verify(statsClient, never()).getStats(any());
    assertThat(sourceCollectionDocCounts(index)).isEmpty();
  }

  @Test
  public void create_killSwitchOff_schedulesUpdates() {
    NamedScheduledExecutorService executor = mock(NamedScheduledExecutorService.class);

    SourceCollectionCountUpdater.create(
        catalog(),
        initializedCatalog(),
        statsClientReturning(1_234),
        executor,
        SourceCollectionCountUpdater.DEFAULT_UPDATE_INTERVAL,
        /* disabled= */ false,
        new SimpleMeterRegistry());

    verify(executor).scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());
  }

  @Test
  public void create_killSwitchOn_doesNotSchedule() {
    NamedScheduledExecutorService executor = mock(NamedScheduledExecutorService.class);

    SourceCollectionCountUpdater.create(
        catalog(),
        initializedCatalog(),
        statsClientReturning(1_234),
        executor,
        SourceCollectionCountUpdater.DEFAULT_UPDATE_INTERVAL,
        /* disabled= */ true,
        new SimpleMeterRegistry());

    verify(executor, never()).scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());
  }

  @Test
  public void isCountComparable_collectionIndex_returnsTrue() {
    var definition = definitionBuilder().build();

    assertThat(SourceCollectionCountUpdater.isCountComparable(definition)).isTrue();
  }

  @Test
  public void isCountComparable_viewWithoutMatch_returnsTrue() {
    var definition =
        definitionBuilder()
            .view(ViewDefinition.existing("view", List.of(addFieldsStage())))
            .build();

    assertThat(SourceCollectionCountUpdater.isCountComparable(definition)).isTrue();
  }

  @Test
  public void isCountComparable_viewWithMatch_returnsFalse() {
    var definition =
        definitionBuilder().view(ViewDefinition.existing("view", List.of(matchStage()))).build();

    assertThat(SourceCollectionCountUpdater.isCountComparable(definition)).isFalse();
  }

  @Test
  public void isCountComparable_missingView_returnsFalse() {
    var definition = definitionBuilder().view(ViewDefinition.missing("view")).build();

    assertThat(SourceCollectionCountUpdater.isCountComparable(definition)).isFalse();
  }

  @Test
  public void isCountComparable_autoEmbeddingIndex_returnsFalse() {
    var definition = VectorIndexDefinitionBuilder.builder().withTextField("text").build();

    assertThat(SourceCollectionCountUpdater.isCountComparable(definition)).isFalse();
  }

  private static SearchIndexDefinitionBuilder definitionBuilder() {
    return SearchIndexDefinitionBuilder.builder()
        .indexId(new ObjectId())
        .name("default")
        .database(NAMESPACE.getDatabaseName())
        .lastObservedCollectionName(NAMESPACE.getCollectionName())
        .collectionUuid(UUID.randomUUID())
        .dynamicMapping();
  }

  private static IndexGeneration generation(SearchIndexDefinition definition) {
    return com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration(definition);
  }

  private static BsonDocument matchStage() {
    return new BsonDocument("$match", new BsonDocument("a", new BsonInt32(1)));
  }

  private static BsonDocument addFieldsStage() {
    return new BsonDocument("$addFields", new BsonDocument("b", new BsonInt32(1)));
  }

  private static Optional<SourceCollectionDocCounts> sourceCollectionDocCounts(
      InitializedIndex index) {
    return index.getMetricsUpdater().getIndexingMetricsUpdater().getSourceCollectionDocCounts();
  }

  private static CollectionStatsMongoClient statsClientReturning(long count) {
    CollectionStatsMongoClient statsClient = mock(CollectionStatsMongoClient.class);
    when(statsClient.getStats(NAMESPACE)).thenReturn(new CollectionStatsResponse(count, 100));
    return statsClient;
  }

  private static IndexCatalog catalog(IndexGeneration... generations) {
    IndexCatalog catalog = mock(IndexCatalog.class);
    when(catalog.getIndexes()).thenReturn(List.of(generations));
    return catalog;
  }

  private static InitializedIndexCatalog initializedCatalog(InitializedIndex... indexes) {
    InitializedIndexCatalog catalog = mock(InitializedIndexCatalog.class);
    for (InitializedIndex index : indexes) {
      when(catalog.getIndex(index.getGenerationId())).thenReturn(Optional.of(index));
    }
    return catalog;
  }

  private static SourceCollectionCountUpdater newUpdater(
      CollectionStatsMongoClient statsClient,
      IndexCatalog indexCatalog,
      InitializedIndexCatalog initializedIndexCatalog,
      SimpleMeterRegistry meterRegistry) {
    return new SourceCollectionCountUpdater(
        indexCatalog,
        initializedIndexCatalog,
        statsClient,
        Executors.singleThreadScheduledExecutor("sourceCollectionCountUpdaterTest", meterRegistry),
        meterRegistry);
  }
}
