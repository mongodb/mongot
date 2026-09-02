package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.replication.mongodb.common.ChangeStreamModeSelector.ChangeStreamMode;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockDefinitionBuilder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Condition;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.CollectionStatsResponse;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class HeuristicChangeStreamModeSelectorTest {

  private static final SearchIndexDefinition STATIC_MAPPING_DEFINITION =
      mockDefinitionBuilder()
          .mappings(
              DocumentFieldDefinitionBuilder.builder()
                  .dynamic(false)
                  .field(
                      "data",
                      FieldDefinitionBuilder.builder()
                          .string(StringFieldDefinitionBuilder.builder().build())
                          .build())
                  .build())
          .build();

  private static final SearchIndexDefinition DYNAMIC_MAPPING_DEFINITION =
      mockDefinitionBuilder().dynamicMapping().build();

  @Test
  public void testNotRegisteredGenerationReturnsDefaultSelection() {

    var statsClient = Mockito.mock(CollectionStatsMongoClient.class);
    var samplingClient = Mockito.mock(CollectionSamplingMongoClient.class);

    // nothing registered
    var modeSelector =
        new HeuristicChangeStreamModeSelector(
            statsClient,
            samplingClient,
            getScheduler(),
            Optional.empty(),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            50,
            TimeUnit.MILLISECONDS);

    Assert.assertEquals(ChangeStreamMode.getDefault(), modeSelector.getMode(getGenerationId()));
  }

  @Test
  public void testCollectionIsNotReadyForSamplingReturnsDefaultSelection() {

    var statsClient = Mockito.mock(CollectionStatsMongoClient.class);
    var samplingClient = Mockito.mock(CollectionSamplingMongoClient.class);

    // stats report empty collection
    Mockito.when(statsClient.getStats(Mockito.any())).thenReturn(new CollectionStatsResponse(0, 0));

    var modeSelector =
        new HeuristicChangeStreamModeSelector(
            statsClient,
            samplingClient,
            getScheduler(),
            Optional.empty(),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            50,
            TimeUnit.MILLISECONDS);

    var generationId = getGenerationId();
    modeSelector.register(
        generationId, STATIC_MAPPING_DEFINITION, new MongoNamespace("test", "test"));

    Condition.await()
        .atMost(Duration.ZERO)
        .withInitialDelay(Duration.ofSeconds(1))
        .until(() -> modeSelector.getMode(generationId) == ChangeStreamMode.getDefault());
  }

  @Test
  public void testMongoDbClientErrorReturnsDefaultSelection() {

    var statsClient = Mockito.mock(CollectionStatsMongoClient.class);
    var samplingClient = Mockito.mock(CollectionSamplingMongoClient.class);

    // stats report empty collection
    Mockito.when(statsClient.getStats(Mockito.any())).thenThrow(new IllegalStateException("test"));

    var modeSelector =
        new HeuristicChangeStreamModeSelector(
            statsClient,
            samplingClient,
            getScheduler(),
            Optional.empty(),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            50,
            TimeUnit.MILLISECONDS);

    var generationId = getGenerationId();
    modeSelector.register(
        generationId, STATIC_MAPPING_DEFINITION, new MongoNamespace("test", "test"));

    Condition.await()
        .atMost(Duration.ZERO)
        .withInitialDelay(Duration.ofSeconds(1))
        .until(() -> modeSelector.getMode(generationId) == ChangeStreamMode.getDefault());
  }

  @Test
  public void testAllFieldsModeIsSelectedWhenDocumentIsTheSameAfterProject() {

    var avgfullDocumentSize = 1_000;
    var avgDocumentSizeAfterProject = 1_000;

    var statsClient = Mockito.mock(CollectionStatsMongoClient.class);
    var samplingClient = Mockito.mock(CollectionSamplingMongoClient.class);

    // stats report large collection of avg doc size of 1000B
    Mockito.when(statsClient.getStats(Mockito.any()))
        .thenReturn(new CollectionStatsResponse(100_000, avgfullDocumentSize));

    // sampling query returns documents after project of the same size
    Mockito.when(
            samplingClient.getSamples(
                Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any(), Mockito.any()))
        .thenReturn(getSampleDocuments(avgDocumentSizeAfterProject));

    var modeSelector =
        new HeuristicChangeStreamModeSelector(
            statsClient,
            samplingClient,
            getScheduler(),
            Optional.empty(),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            50,
            TimeUnit.MILLISECONDS);

    var generationId = getGenerationId();
    modeSelector.register(
        generationId, STATIC_MAPPING_DEFINITION, new MongoNamespace("test", "test"));

    Condition.await()
        .atMost(Duration.ZERO)
        .withInitialDelay(Duration.ofSeconds(1))
        .until(() -> modeSelector.getMode(generationId) == ChangeStreamMode.ALL_FIELDS);
  }

  @Test
  public void testIndexedFieldsModeIsSelectedWhenDocumentAfterProjectIsMuchSmaller() {

    var avgfullDocumentSize = 50_000;
    var avgDocumentSizeAfterProject = 1_000;

    var statsClient = Mockito.mock(CollectionStatsMongoClient.class);
    var samplingClient = Mockito.mock(CollectionSamplingMongoClient.class);

    // stats report large collection of avg doc size ~ 50Kb
    Mockito.when(statsClient.getStats(Mockito.any()))
        .thenReturn(new CollectionStatsResponse(100_000, avgfullDocumentSize));

    // sampling query returns documents after project which are 50 times smaller
    Mockito.when(
            samplingClient.getSamples(
                Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any(), Mockito.any()))
        .thenReturn(getSampleDocuments(avgDocumentSizeAfterProject));

    var modeSelector =
        new HeuristicChangeStreamModeSelector(
            statsClient,
            samplingClient,
            getScheduler(),
            Optional.empty(),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            50,
            TimeUnit.MILLISECONDS);

    var generationId = getGenerationId();
    modeSelector.register(
        generationId, STATIC_MAPPING_DEFINITION, new MongoNamespace("test", "test"));

    Condition.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> modeSelector.getMode(generationId) == ChangeStreamMode.INDEXED_FIELDS);
  }

  @Test
  public void testEmptyProjectionReturnsAllFields() {

    var modeSelector =
        new HeuristicChangeStreamModeSelector(
            Mockito.mock(CollectionStatsMongoClient.class),
            Mockito.mock(CollectionSamplingMongoClient.class),
            getScheduler(),
            Optional.empty(),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            50,
            TimeUnit.MILLISECONDS);

    var generationId = getGenerationId();
    modeSelector.register(
        generationId, DYNAMIC_MAPPING_DEFINITION, new MongoNamespace("test", "test"));

    Condition.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> modeSelector.getMode(generationId) == ChangeStreamMode.ALL_FIELDS);
  }

  @Test
  public void testSelectorRepeatsAttemptsForNonExistentNamespace() {

    var statsClient = Mockito.mock(CollectionStatsMongoClient.class);
    var samplingClient = Mockito.mock(CollectionSamplingMongoClient.class);

    // empty results for both queries in case of non-existent namespace
    Mockito.when(statsClient.getStats(Mockito.any())).thenReturn(new CollectionStatsResponse(0, 0));
    Mockito.when(
            samplingClient.getSamples(
                Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any(), Mockito.any()))
        .thenReturn(List.of());

    var modeSelector =
        new HeuristicChangeStreamModeSelector(
            statsClient,
            samplingClient,
            getScheduler(),
            Optional.empty(),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            5,
            TimeUnit.MILLISECONDS);

    modeSelector.register(
        getGenerationId(), STATIC_MAPPING_DEFINITION, new MongoNamespace("test", "test"));

    Mockito.verify(statsClient, timeout(5_000).atLeast(10)).getStats(any());
  }

  @Test
  public void testSelectsDefaultModeWhenStatsShowPopulatedCollectionButSampleBatchIsEmpty() {

    var statsClient = Mockito.mock(CollectionStatsMongoClient.class);
    var samplingClient = Mockito.mock(CollectionSamplingMongoClient.class);

    // populated collection in stats query
    Mockito.when(statsClient.getStats(Mockito.any()))
        .thenReturn(new CollectionStatsResponse(100_000, 10_000));

    // empty sample batch (can be caused by dropping the collection in between calls)
    Mockito.when(
            samplingClient.getSamples(
                Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any(), Mockito.any()))
        .thenReturn(List.of());

    var modeSelector =
        new HeuristicChangeStreamModeSelector(
            statsClient,
            samplingClient,
            getScheduler(),
            Optional.empty(),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            5,
            TimeUnit.MILLISECONDS);

    var generationId = getGenerationId();
    modeSelector.register(
        generationId, DYNAMIC_MAPPING_DEFINITION, new MongoNamespace("test", "test"));

    Condition.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> modeSelector.getMode(generationId) == ChangeStreamMode.getDefault());
  }

  @Test
  public void testShutdown() throws InterruptedException {

    var statsClient = Mockito.mock(CollectionStatsMongoClient.class);
    var latch = new CountDownLatch(1);

    // stats returning 0 would result more attempts for the same generation in the next cycles
    Mockito.when(statsClient.getStats(Mockito.any()))
        .thenAnswer(
            invocation -> {
              latch.countDown();
              TimeUnit.SECONDS.sleep(1); // first task is fast
              return new CollectionStatsResponse(0, 0);
            })
        .thenAnswer(
            invocation -> {
              TimeUnit.SECONDS.sleep(60); // second task is slow (should not be executed)
              return new CollectionStatsResponse(0, 0);
            });

    var scheduler = getScheduler();

    var modeSelector =
        new HeuristicChangeStreamModeSelector(
            statsClient,
            Mockito.mock(CollectionSamplingMongoClient.class),
            scheduler,
            Optional.empty(),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            10,
            TimeUnit.MILLISECONDS);

    modeSelector.register(
        getGenerationId(), STATIC_MAPPING_DEFINITION, new MongoNamespace("test", "test"));

    // make sure we caught a moment when background selection has started
    Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

    // initiate shutdown
    modeSelector.shutdown();

    // check that scheduler is terminated and the new 60 sec task was not accepted
    Condition.await().atMost(Duration.ofSeconds(5)).until(scheduler::isTerminated);
    verify(statsClient, times(1)).getStats(any());
  }

  @Test
  public void testShutdownAbortsBetweenIndexesMidCycle() {

    var statsClient = Mockito.mock(CollectionStatsMongoClient.class);
    var firstQueryBeforeAndAfter = new Phaser(2);

    Mockito.when(statsClient.getStats(Mockito.any()))
        .thenAnswer(
            invocation -> {
              firstQueryBeforeAndAfter.arriveAndAwaitAdvance();
              // shutdown() is called here
              firstQueryBeforeAndAfter.arriveAndAwaitAdvance();
              return new CollectionStatsResponse(0, 0);
            })
        .thenAnswer(
            invocation -> {
              TimeUnit.SECONDS.sleep(60); // 2nd index is slow, shouldn't happen
              return new CollectionStatsResponse(0, 0);
            });

    var scheduler = spy(getScheduler());

    var modeSelector =
        new HeuristicChangeStreamModeSelector(
            statsClient,
            Mockito.mock(CollectionSamplingMongoClient.class),
            scheduler,
            Optional.empty(),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            10,
            TimeUnit.MILLISECONDS);

    // unlike the previous test, we have two indexes here, so both will be queried on the first
    // cycle
    modeSelector.register(
        getGenerationId(), STATIC_MAPPING_DEFINITION, new MongoNamespace("test", "test"));
    modeSelector.register(
        getGenerationId(), STATIC_MAPPING_DEFINITION, new MongoNamespace("test", "test2"));

    firstQueryBeforeAndAfter.arriveAndAwaitAdvance();

    // The following block makes sure that the first index is allowed to finish in the middle of
    // modeSelector::shutdown call. We accomplish that by having the underlying executor arrive to
    // the Phaser when its shutdown.
    Answer<Void> releaseQueryBeforeShutdown =
        invocationOnMock -> {
          firstQueryBeforeAndAfter.arriveAndAwaitAdvance();
          invocationOnMock.callRealMethod();
          return null;
        };
    doAnswer(releaseQueryBeforeShutdown).when(scheduler).shutdown();

    // Shut down during the first index query, has to be done on a separate thread otherwise we will
    // have to wait until its shutdown.
    CompletableFuture.runAsync(modeSelector::shutdown);

    // check that scheduler is terminated and the new 60 sec task was not accepted
    Condition.await().atMost(Duration.ofSeconds(5)).until(scheduler::isTerminated);

    // only one collection should have been queried
    verify(statsClient, times(1)).getStats(any());
  }

  @Test
  public void testFailToRegisterTheSameGenerationTwice() {
    var modeSelector =
        new HeuristicChangeStreamModeSelector(
            Mockito.mock(CollectionStatsMongoClient.class),
            Mockito.mock(CollectionSamplingMongoClient.class),
            getScheduler(),
            Optional.empty(),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            10,
            TimeUnit.MILLISECONDS);

    var generationId = getGenerationId();

    // first registration
    modeSelector.register(
        generationId, STATIC_MAPPING_DEFINITION, new MongoNamespace("test", "test"));

    // second registration
    Assert.assertThrows(
        AssertionError.class,
        () ->
            modeSelector.register(
                generationId, STATIC_MAPPING_DEFINITION, new MongoNamespace("test", "test")));
  }

  @Test
  public void testFailToRemoveTheSameGenerationTwice() {
    var modeSelector =
        new HeuristicChangeStreamModeSelector(
            Mockito.mock(CollectionStatsMongoClient.class),
            Mockito.mock(CollectionSamplingMongoClient.class),
            getScheduler(),
            Optional.empty(),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            10,
            TimeUnit.MILLISECONDS);

    var generationId = getGenerationId();

    modeSelector.register(
        generationId, STATIC_MAPPING_DEFINITION, new MongoNamespace("test", "test"));

    // first remove
    modeSelector.remove(generationId);

    // second remove
    Assert.assertThrows(AssertionError.class, () -> modeSelector.remove(generationId));
  }

  @Test
  public void testOverrideValueIsSelectedWhenProjectionIsNotEmpty() {

    var statsClient = Mockito.mock(CollectionStatsMongoClient.class);
    var samplingClient = Mockito.mock(CollectionSamplingMongoClient.class);

    // stats are not expected to be requested in presence of override
    Mockito.when(statsClient.getStats(Mockito.any())).thenThrow(new IllegalStateException("test"));

    var modeSelector =
        new HeuristicChangeStreamModeSelector(
            statsClient,
            samplingClient,
            getScheduler(),
            Optional.of(ChangeStreamMode.INDEXED_FIELDS),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            50,
            TimeUnit.MILLISECONDS);

    var generationId = getGenerationId();
    modeSelector.register(
        generationId, STATIC_MAPPING_DEFINITION, new MongoNamespace("test", "test"));

    Condition.await()
        .atMost(Duration.ZERO)
        .withInitialDelay(Duration.ofSeconds(1))
        .until(() -> modeSelector.getMode(generationId) == ChangeStreamMode.INDEXED_FIELDS);

    Mockito.verify(statsClient, after(100).never()).getStats(Mockito.any());
  }

  @Test
  public void testOverrideValueIsNotSelectedWhenProjectionIsEmpty() {

    var statsClient = Mockito.mock(CollectionStatsMongoClient.class);
    var samplingClient = Mockito.mock(CollectionSamplingMongoClient.class);

    // stats are not expected to be requested in presence of override
    Mockito.when(statsClient.getStats(Mockito.any())).thenThrow(new IllegalStateException("test"));

    var modeSelector =
        new HeuristicChangeStreamModeSelector(
            statsClient,
            samplingClient,
            getScheduler(),
            Optional.of(ChangeStreamMode.INDEXED_FIELDS),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            50,
            TimeUnit.MILLISECONDS);

    var generationId = getGenerationId();
    modeSelector.register(
        generationId, DYNAMIC_MAPPING_DEFINITION, new MongoNamespace("test", "test"));

    Condition.await()
        .atMost(Duration.ZERO)
        .withInitialDelay(Duration.ofSeconds(1))
        .until(() -> modeSelector.getMode(generationId) == ChangeStreamMode.getDefault());

    Mockito.verify(statsClient, after(100).never()).getStats(Mockito.any());
  }

  @Test
  public void testViewWithMatchStageSkipsSamplingAndReturnsAllFields() {

    var statsClient = Mockito.mock(CollectionStatsMongoClient.class);
    var samplingClient = Mockito.mock(CollectionSamplingMongoClient.class);

    // stats report large collection — would normally trigger sampling
    Mockito.when(statsClient.getStats(Mockito.any()))
        .thenReturn(new CollectionStatsResponse(100_000, 50_000));

    SearchIndexDefinition definitionWithMatchView =
        mockDefinitionBuilder()
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "data",
                        FieldDefinitionBuilder.builder()
                            .string(StringFieldDefinitionBuilder.builder().build())
                            .build())
                    .build())
            .view(
                ViewDefinition.existing(
                    "myView",
                    List.of(
                        BsonDocument.parse("{\"$match\": {\"$expr\": {\"$gt\": [\"$x\", 0]}}}"))))
            .build();

    var modeSelector =
        new HeuristicChangeStreamModeSelector(
            statsClient,
            samplingClient,
            getScheduler(),
            Optional.empty(),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            50,
            TimeUnit.MILLISECONDS);

    var generationId = getGenerationId();
    modeSelector.register(
        generationId, definitionWithMatchView, new MongoNamespace("test", "test"));

    // Block for several scheduler cycles (interval is 50ms) to confirm neither stats nor sampling
    // were invoked. Using after() ensures the scheduler has had time to run; if the $match
    // short-circuit is broken, getStats/getSamples would be called and the test would fail.
    Mockito.verify(statsClient, Mockito.after(500).never()).getStats(Mockito.any());
    Mockito.verify(samplingClient, Mockito.never())
        .getSamples(any(), any(), Mockito.anyInt(), any(), any());
    Assert.assertEquals(ChangeStreamMode.ALL_FIELDS, modeSelector.getMode(generationId));
  }

  @Test
  public void testViewWithAddFieldsOnlyStillSamples() {

    var statsClient = Mockito.mock(CollectionStatsMongoClient.class);
    var samplingClient = Mockito.mock(CollectionSamplingMongoClient.class);

    // stats report large collection of avg doc size ~ 50Kb — ratio will trigger INDEXED_FIELDS
    Mockito.when(statsClient.getStats(Mockito.any()))
        .thenReturn(new CollectionStatsResponse(100_000, 50_000));

    // sampling returns small documents after projection
    Mockito.when(
            samplingClient.getSamples(
                Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any(), Mockito.any()))
        .thenReturn(getSampleDocuments(1_000));

    SearchIndexDefinition definitionWithAddFieldsView =
        mockDefinitionBuilder()
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "data",
                        FieldDefinitionBuilder.builder()
                            .string(StringFieldDefinitionBuilder.builder().build())
                            .build())
                    .build())
            .view(
                ViewDefinition.existing(
                    "myView", List.of(BsonDocument.parse("{\"$addFields\": {\"computed\": 1}}"))))
            .build();

    var modeSelector =
        new HeuristicChangeStreamModeSelector(
            statsClient,
            samplingClient,
            getScheduler(),
            Optional.empty(),
            new MetricsFactory("factory", new SimpleMeterRegistry()),
            50,
            TimeUnit.MILLISECONDS);

    var generationId = getGenerationId();
    modeSelector.register(
        generationId, definitionWithAddFieldsView, new MongoNamespace("test", "test"));

    Condition.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> modeSelector.getMode(generationId) == ChangeStreamMode.INDEXED_FIELDS);

    // sampling must be called for a view without $match
    Mockito.verify(samplingClient, Mockito.atLeastOnce())
        .getSamples(any(), any(), Mockito.anyInt(), any(), any());
  }

  private List<RawBsonDocument> getSampleDocuments(int sizeInBytes) {
    return IntStream.range(0, 10)
        .mapToObj(
            value ->
                new BsonDocument(
                    "key", new BsonString(RandomStringUtils.randomAlphabetic(sizeInBytes))))
        .map(BsonUtils::documentToRaw)
        .collect(Collectors.toList());
  }

  private GenerationId getGenerationId() {
    return new GenerationId(new ObjectId(), Generation.CURRENT);
  }

  private NamedScheduledExecutorService getScheduler() {
    return Executors.singleThreadScheduledExecutor(
        "change-stream-mode-selector", new SimpleMeterRegistry());
  }
}
