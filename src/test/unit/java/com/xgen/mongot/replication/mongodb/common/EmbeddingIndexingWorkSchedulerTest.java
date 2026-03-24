package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO;
import static com.xgen.testing.mongot.mock.replication.mongodb.common.DocumentIndexer.mockDocumentRequiresAutoEmbedding;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderNonTransientException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.embedding.providers.EmbeddingServiceRegistry;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.testing.mongot.embedding.providers.FakeEmbeddingClientFactory;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Test;

public class EmbeddingIndexingWorkSchedulerTest {

  private static final IndexCommitUserData COMMIT_USER_DATA =
      getCommitUserData(new MongoNamespace("db", "collection"), 0);

  private static final IndexMetricsUpdater.IndexingMetricsUpdater IGNORE_METRICS =
      SearchIndex.mockIndexingMetricsUpdater(IndexDefinition.Type.SEARCH);

  private static final EmbeddingServiceConfig.EmbeddingConfig VOYAGE_3_CONFIG =
      new EmbeddingServiceConfig.EmbeddingConfig(
          Optional.empty(),
          new EmbeddingServiceConfig.VoyageModelConfig(
              Optional.of(1024),
              Optional.of(EmbeddingServiceConfig.TruncationOption.NONE),
              Optional.of(100),
              Optional.of(120_000)),
          new EmbeddingServiceConfig.ErrorHandlingConfig(3, 100L, 200L, 0.1),
          new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
              "token123", "2024-10-15T22:32:20.925Z"),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          true,
          Optional.empty(),
          false,
          Optional.empty());

  private static final EmbeddingServiceConfig TEST_EMBEDDING_CONFIG_V3_LARGE =
      new EmbeddingServiceConfig(
          EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
          "voyage-3-large",
          EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
          VOYAGE_3_CONFIG);
  private static final EmbeddingServiceConfig TEST_EMBEDDING_CONFIG_V3_LITE =
      new EmbeddingServiceConfig(
          EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
          "voyage-3-lite",
          EmbeddingServiceConfig.DEFAULT_RPS_PER_PROVIDER,
          VOYAGE_3_CONFIG);

  @Test
  public void testSingleDocumentRequiresAutoEmbedding()
      throws ExecutionException,
      InterruptedException,
      TimeoutException,
      FieldExceededLimitsException,
      IOException {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    EmbeddingIndexingWorkScheduler scheduler =
        scheduler(
            Suppliers.ofInstance(
                new EmbeddingServiceManager(
                    List.of(TEST_EMBEDDING_CONFIG_V3_LARGE, TEST_EMBEDDING_CONFIG_V3_LITE),
                    new FakeEmbeddingClientFactory(),
                    Executors.singleThreadScheduledExecutor("indexing", meterRegistry),
                    meterRegistry,
                    Optional.empty())));

    ObjectId indexId = new ObjectId();
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withTextField(indexId + ".a")
            .withTextField(indexId + ".b")
            .build();
    DocumentIndexer indexer = mockDocumentRequiresAutoEmbedding(vectorIndexDefinition);
    RawBsonDocument rawBsonDoc =
        BsonUtils.documentToRaw(new BsonDocument(indexId.toString(), createBasicBson()));
    DocumentEvent event =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(rawBsonDoc), indexId), rawBsonDoc);
    List<DocumentEvent> batch = new ArrayList<>(List.of(event));
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);
    indexingFuture.get(5, TimeUnit.SECONDS);
    DocumentEvent expected =
        AutoEmbeddingDocumentUtils.buildAutoEmbeddingDocumentEvent(
            event, vectorIndexDefinition.getMappings(), expectedAutoEmbeddingsByPath(indexId));
    verify(indexer, times(1)).indexDocumentEvent(expected);
  }

  @Test
  public void testAutoEmbeddingNonTransientException() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    EmbeddingIndexingWorkScheduler scheduler =
        scheduler(
            Suppliers.ofInstance(
                new EmbeddingServiceManager(
                    List.of(TEST_EMBEDDING_CONFIG_V3_LARGE, TEST_EMBEDDING_CONFIG_V3_LITE),
                    new FakeEmbeddingClientFactory(
                        meterRegistry,
                        ImmutableSet.of(),
                        ImmutableSet.of(),
                        ImmutableSet.of("aString")),
                    Executors.singleThreadScheduledExecutor("indexing", meterRegistry),
                    meterRegistry,
                    Optional.empty())));

    ObjectId indexId = new ObjectId();
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withTextField(indexId + ".a")
            .withTextField(indexId + ".b")
            .build();
    DocumentIndexer indexer = mockDocumentRequiresAutoEmbedding(vectorIndexDefinition);
    RawBsonDocument rawBsonDoc =
        BsonUtils.documentToRaw(new BsonDocument(indexId.toString(), createBasicBson()));
    DocumentEvent event =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(rawBsonDoc), indexId), rawBsonDoc);
    List<DocumentEvent> batch = new ArrayList<>(List.of(event));
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);
    var e = assertThrows(ExecutionException.class, indexingFuture::get);
    Throwable cause = e.getCause();
    assertThat(cause).isInstanceOf(SteadyStateException.class);
    assertThat(cause.getCause()).isInstanceOf(EmbeddingProviderNonTransientException.class);
  }

  @Test
  public void testAutoEmbeddingTransientException() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    EmbeddingIndexingWorkScheduler scheduler =
        scheduler(
            Suppliers.ofInstance(
                new EmbeddingServiceManager(
                    List.of(TEST_EMBEDDING_CONFIG_V3_LARGE),
                    new FakeEmbeddingClientFactory(
                        meterRegistry,
                        ImmutableSet.of(),
                        ImmutableSet.of("aString"),
                        ImmutableSet.of()),
                    Executors.singleThreadScheduledExecutor("indexing", meterRegistry),
                    meterRegistry,
                    Optional.empty())));

    ObjectId indexId = new ObjectId();
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withTextField(indexId + ".a")
            .withTextField(indexId + ".b")
            .build();
    DocumentIndexer indexer = mockDocumentRequiresAutoEmbedding(vectorIndexDefinition);
    RawBsonDocument rawBsonDoc =
        BsonUtils.documentToRaw(new BsonDocument(indexId.toString(), createBasicBson()));
    DocumentEvent event =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(rawBsonDoc), indexId), rawBsonDoc);
    List<DocumentEvent> batch = new ArrayList<>(List.of(event));
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    var e = assertThrows(ExecutionException.class, indexingFuture::get);
    Throwable cause = e.getCause();
    assertThat(cause).isInstanceOf(SteadyStateException.class);
    assertThat(cause.getCause()).isInstanceOf(EmbeddingProviderTransientException.class);
  }

  @Test
  public void testBuildingIndexRequiresSupportedModel() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    EmbeddingIndexingWorkScheduler scheduler =
        scheduler(
            Suppliers.ofInstance(
                new EmbeddingServiceManager(
                    List.of(TEST_EMBEDDING_CONFIG_V3_LARGE, TEST_EMBEDDING_CONFIG_V3_LITE),
                    new FakeEmbeddingClientFactory(),
                    Executors.singleThreadScheduledExecutor("indexing", meterRegistry),
                    meterRegistry,
                    Optional.empty())));

    ObjectId indexId = new ObjectId();
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            // Unsupported model should fail replication
            .withTextField(indexId + ".a", "voyage-3-test")
            .withTextField(indexId + ".b", "voyage-3-large")
            .build();
    DocumentIndexer indexer = mockDocumentRequiresAutoEmbedding(vectorIndexDefinition);
    RawBsonDocument rawBsonDoc =
        BsonUtils.documentToRaw(new BsonDocument(indexId.toString(), createBasicBson()));
    DocumentEvent event =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(rawBsonDoc), indexId), rawBsonDoc);
    List<DocumentEvent> batch = new ArrayList<>(List.of(event));
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            new GenerationId(new ObjectId(), Generation.CURRENT),
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);
    Throwable ex = assertThrows(ExecutionException.class, indexingFuture::get);
    assertTrue(
        ex.getCause()
            .getMessage()
            .contains(
                "CanonicalModel: voyage-3-test not registered yet, supported models are: "
                    + "[voyage-3-large, voyage-3-lite]"));
  }

  @Test
  public void testSingleDocumentRequiresAutoEmbeddingForMaterializedView()
      throws ExecutionException,
      InterruptedException,
      TimeoutException,
      FieldExceededLimitsException,
      IOException {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    ObjectId indexId = new ObjectId();
    var generationId = new GenerationId(indexId, Generation.CURRENT);

    EmbeddingIndexingWorkScheduler scheduler =
        schedulerForMaterializedViewIndex(
            Suppliers.ofInstance(
                new EmbeddingServiceManager(
                    List.of(TEST_EMBEDDING_CONFIG_V3_LARGE, TEST_EMBEDDING_CONFIG_V3_LITE),
                    new FakeEmbeddingClientFactory(),
                    Executors.singleThreadScheduledExecutor("indexing", meterRegistry),
                    meterRegistry,
                    Optional.empty())),
            generationId);

    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField(indexId + ".a")
            .withAutoEmbedField(indexId + ".b")
            .build();
    DocumentIndexer indexer = mockDocumentRequiresAutoEmbedding(vectorIndexDefinition);
    RawBsonDocument rawBsonDoc =
        BsonUtils.documentToRaw(new BsonDocument(indexId.toString(), createBasicBson()));
    DocumentEvent event =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(rawBsonDoc), indexId), rawBsonDoc);
    List<DocumentEvent> batch = new ArrayList<>(List.of(event));
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            generationId,
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);
    indexingFuture.get(5, TimeUnit.SECONDS);
    DocumentEvent expected =
        AutoEmbeddingDocumentUtils.buildMaterializedViewDocumentEvent(
            event,
            vectorIndexDefinition,
            expectedAutoEmbeddingsPerField(vectorIndexDefinition.getMappings()),
            VERSION_ZERO);
    verify(indexer, times(1)).indexDocumentEvent(expected);
  }

  @Test
  public void testAutoEmbeddingMaterializedViewTransientException() throws IOException {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    ObjectId indexId = new ObjectId();
    var generationId = new GenerationId(indexId, Generation.CURRENT);
    EmbeddingIndexingWorkScheduler scheduler =
        schedulerForMaterializedViewIndex(
            Suppliers.ofInstance(
                new EmbeddingServiceManager(
                    List.of(TEST_EMBEDDING_CONFIG_V3_LARGE),
                    new FakeEmbeddingClientFactory(
                        meterRegistry, ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of()),
                    Executors.singleThreadScheduledExecutor("indexing", meterRegistry),
                    meterRegistry,
                    Optional.empty())),
            generationId);

    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField(indexId + ".a")
            .withAutoEmbedField(indexId + ".b")
            .build();
    DocumentIndexer indexer = mockDocumentRequiresAutoEmbedding(vectorIndexDefinition);
    doThrow(new MaterializedViewTransientException("mocked error")).when(indexer).commit();
    RawBsonDocument rawBsonDoc =
        BsonUtils.documentToRaw(new BsonDocument(indexId.toString(), createBasicBson()));
    DocumentEvent event =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(rawBsonDoc), indexId), rawBsonDoc);
    List<DocumentEvent> batch = new ArrayList<>(List.of(event));
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            generationId,
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    var e = assertThrows(ExecutionException.class, indexingFuture::get);
    Throwable cause = e.getCause();
    assertThat(cause).isInstanceOf(SteadyStateException.class);
    assertThat(cause.getCause()).isInstanceOf(MaterializedViewTransientException.class);
  }

  @Test
  public void testAutoEmbeddingNonTransientException_missingMatViewMetadata() {
    // Setup: Use a materialized view scheduler but with a generationId that has no metadata
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    ObjectId indexId = new ObjectId();
    var generationId = new GenerationId(indexId, Generation.CURRENT);

    // Create scheduler for materialized view but DON'T add metadata for the generationId
    SimpleMeterRegistry schedulerRegistry = new SimpleMeterRegistry();
    NamedExecutorService executor = Executors.fixedSizeThreadPool("indexing", 2, schedulerRegistry);
    var matViewCollectionMetadataCatalog = new MaterializedViewCollectionMetadataCatalog();
    // Intentionally NOT adding metadata: matViewCollectionMetadataCatalog.addMetadata(...)
    EmbeddingIndexingWorkScheduler scheduler =
        EmbeddingIndexingWorkScheduler.createForMaterializedViewIndex(
            executor,
            Suppliers.ofInstance(
                new EmbeddingServiceManager(
                    List.of(TEST_EMBEDDING_CONFIG_V3_LARGE),
                    new FakeEmbeddingClientFactory(),
                    Executors.singleThreadScheduledExecutor("indexing", meterRegistry),
                    meterRegistry,
                    Optional.empty())),
            matViewCollectionMetadataCatalog);

    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder().withAutoEmbedField(indexId + ".a").build();
    DocumentIndexer indexer = mockDocumentRequiresAutoEmbedding(vectorIndexDefinition);
    RawBsonDocument rawBsonDoc =
        BsonUtils.documentToRaw(new BsonDocument(indexId.toString(), createBasicBson()));
    DocumentEvent event =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(rawBsonDoc), indexId), rawBsonDoc);
    List<DocumentEvent> batch = new ArrayList<>(List.of(event));
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            generationId,
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);

    var e = assertThrows(ExecutionException.class, indexingFuture::get);
    Throwable cause = e.getCause();
    assertThat(cause).isInstanceOf(SteadyStateException.class);
    assertThat(cause.getCause()).isInstanceOf(EmbeddingProviderNonTransientException.class);
    assertThat(cause.getCause().getMessage())
        .contains("Unable to process materialized view index batch because mat view metadata");
  }

  @Test
  public void testSingleDocumentWithReusableEmbeddings()
      throws ExecutionException,
      InterruptedException,
      TimeoutException,
      FieldExceededLimitsException,
      IOException {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    ObjectId indexId = new ObjectId();
    var generationId = new GenerationId(indexId, Generation.CURRENT);
    var embeddingServiceManager =
        spy(
            new EmbeddingServiceManager(
                List.of(TEST_EMBEDDING_CONFIG_V3_LARGE, TEST_EMBEDDING_CONFIG_V3_LITE),
                new FakeEmbeddingClientFactory(),
                Executors.singleThreadScheduledExecutor("indexing", meterRegistry),
                meterRegistry,
                Optional.empty()));
    EmbeddingIndexingWorkScheduler scheduler =
        schedulerForMaterializedViewIndex(
            Suppliers.ofInstance(embeddingServiceManager), generationId);

    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField(indexId + ".a")
            .withAutoEmbedField(indexId + ".b")
            .build();
    DocumentIndexer indexer = mockDocumentRequiresAutoEmbedding(vectorIndexDefinition);
    RawBsonDocument rawBsonDoc =
        BsonUtils.documentToRaw(new BsonDocument(indexId.toString(), createBasicBson()));
    DocumentEvent event =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(rawBsonDoc), indexId), rawBsonDoc);

    // Add existing embeddings for one field
    var existingEmbeddings =
        ImmutableMap.of(
            FieldPath.parse(indexId + ".a"),
            ImmutableMap.of("aString", expectedAutoEmbeddings().get("aString")));
    DocumentEvent eventWithEmbeddings =
        DocumentEvent.createFromDocumentEventAndVectors(event, existingEmbeddings);
    List<DocumentEvent> batch = new ArrayList<>(List.of(eventWithEmbeddings));
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            generationId,
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);
    indexingFuture.get(5, TimeUnit.SECONDS);
    DocumentEvent expected =
        AutoEmbeddingDocumentUtils.buildMaterializedViewDocumentEvent(
            event,
            vectorIndexDefinition,
            expectedAutoEmbeddingsPerField(vectorIndexDefinition.getMappings()),
            VERSION_ZERO);
    verify(indexer, times(1)).indexDocumentEvent(expected);
    // only one field should have been embedded.
    verify(embeddingServiceManager, times(1))
        .embedAsync(argThat(strings -> strings.size() == 1), any(), any(), any());
  }

  @Test
  public void testFilterOnlyUpdateSkipsEmbeddingTransformation()
      throws ExecutionException,
          InterruptedException,
          TimeoutException,
          FieldExceededLimitsException,
          IOException {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    ObjectId indexId = new ObjectId();
    var generationId = new GenerationId(indexId, Generation.CURRENT);
    var embeddingServiceManager =
        spy(
            new EmbeddingServiceManager(
                List.of(TEST_EMBEDDING_CONFIG_V3_LARGE, TEST_EMBEDDING_CONFIG_V3_LITE),
                new FakeEmbeddingClientFactory(),
                Executors.singleThreadScheduledExecutor("indexing", meterRegistry),
                meterRegistry,
                Optional.empty()));
    EmbeddingIndexingWorkScheduler scheduler =
        schedulerForMaterializedViewIndex(
            Suppliers.ofInstance(embeddingServiceManager), generationId);

    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField(indexId + ".a")
            .withFilterPath(indexId + ".filter")
            .build();
    DocumentIndexer indexer = mockDocumentRequiresAutoEmbedding(vectorIndexDefinition);

    // Create a document with both auto-embed and filter fields
    // The document structure is: {indexId: {_id: "anId", a: "textToEmbed", filter: "filterValue"}}
    BsonDocument innerDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("a", new BsonString("textToEmbed"))
            .append("filter", new BsonString("filterValue"));
    BsonDocument bsonDoc = new BsonDocument(indexId.toString(), innerDoc);
    RawBsonDocument rawBsonDoc = BsonUtils.documentToRaw(bsonDoc);

    // Create a filter-only update event (simulating when only filter field changed)
    // The filterFieldUpdates contains the $set document for partial update
    BsonDocument filterFieldUpdates =
        new BsonDocument(indexId + ".filter", new BsonString("newFilterValue"));
    DocumentEvent filterOnlyEvent =
        DocumentEvent.createFilterOnlyUpdate(
            DocumentMetadata.fromMetadataNamespace(Optional.of(rawBsonDoc), indexId),
            rawBsonDoc,
            filterFieldUpdates);

    List<DocumentEvent> batch = new ArrayList<>(List.of(filterOnlyEvent));
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            generationId,
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);
    indexingFuture.get(5, TimeUnit.SECONDS);

    // Verify the event was passed through unchanged (still has filterFieldUpdates)
    // This confirms that buildMaterializedViewDocumentEvent was skipped
    verify(indexer, times(1))
        .indexDocumentEvent(argThat(event -> event.getFilterFieldUpdates().isPresent()));
  }

  @Test
  public void testMultipleFieldsWithDifferentModels()
      throws ExecutionException,
          InterruptedException,
          TimeoutException,
          FieldExceededLimitsException,
          IOException {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();

    ObjectId indexId = new ObjectId();
    var generationId = new GenerationId(indexId, Generation.CURRENT);
    var embeddingServiceManager =
        spy(
            new EmbeddingServiceManager(
                List.of(TEST_EMBEDDING_CONFIG_V3_LARGE, TEST_EMBEDDING_CONFIG_V3_LITE),
                new FakeEmbeddingClientFactory(),
                Executors.singleThreadScheduledExecutor("indexing", meterRegistry),
                meterRegistry,
                Optional.empty()));
    EmbeddingIndexingWorkScheduler scheduler =
        schedulerForMaterializedViewIndex(
            Suppliers.ofInstance(embeddingServiceManager), generationId);

    // Create index with two fields using DIFFERENT models
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField(indexId + ".a", "voyage-3-large")
            .withAutoEmbedField(indexId + ".b", "voyage-3-lite")
            .build();
    DocumentIndexer indexer = mockDocumentRequiresAutoEmbedding(vectorIndexDefinition);

    // Create document with both fields
    BsonDocument innerDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("a", new BsonString("textForLargeModel"))
            .append("b", new BsonString("textForLiteModel"));
    BsonDocument bsonDoc = new BsonDocument(indexId.toString(), innerDoc);
    RawBsonDocument rawBsonDoc = BsonUtils.documentToRaw(bsonDoc);

    DocumentEvent event =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(rawBsonDoc), indexId), rawBsonDoc);
    List<DocumentEvent> batch = new ArrayList<>(List.of(event));
    CompletableFuture<Void> indexingFuture =
        scheduler.schedule(
            batch,
            SchedulerQueue.Priority.STEADY_STATE_CHANGE_STREAM,
            indexer,
            generationId,
            Optional.of(new ObjectId()),
            Optional.of(COMMIT_USER_DATA),
            IGNORE_METRICS);
    indexingFuture.get(5, TimeUnit.SECONDS);

    // Verify embedAsync was called twice - once for each model
    verify(embeddingServiceManager, times(2)).embedAsync(any(), any(), any(), any());

    // Verify each model received the correct text
    verify(embeddingServiceManager)
        .embedAsync(
            argThat(strings -> strings.size() == 1 && strings.contains("textForLargeModel")),
            argThat(config -> config.name().equals("voyage-3-large")),
            any(),
            any());
    verify(embeddingServiceManager)
        .embedAsync(
            argThat(strings -> strings.size() == 1 && strings.contains("textForLiteModel")),
            argThat(config -> config.name().equals("voyage-3-lite")),
            any(),
            any());

    // Verify the document was indexed
    verify(indexer, times(1)).indexDocumentEvent(any());
  }

  @Test
  public void testGetTextValueBundles_GroupsByModel() throws IOException {
    // Initialize the embedding service manager to register models in the catalog
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    new EmbeddingServiceManager(
        List.of(TEST_EMBEDDING_CONFIG_V3_LARGE, TEST_EMBEDDING_CONFIG_V3_LITE),
        new FakeEmbeddingClientFactory(),
        Executors.singleThreadScheduledExecutor("indexing", meterRegistry),
        meterRegistry,
        Optional.empty());

    // Create field mapping with two fields using different models
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition vectorIndexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField(indexId + ".a", "voyage-3-large")
            .withAutoEmbedField(indexId + ".b", "voyage-3-lite")
            .build();
    VectorIndexFieldMapping fieldMapping = vectorIndexDefinition.getMappings();

    // Get model configs from the catalog (now registered)
    EmbeddingModelConfig largeModelConfig =
        EmbeddingModelCatalog.getModelConfig("voyage-3-large");
    EmbeddingModelConfig liteModelConfig = EmbeddingModelCatalog.getModelConfig("voyage-3-lite");

    ImmutableMap<FieldPath, EmbeddingModelConfig> modelConfigPerPath =
        ImmutableMap.of(
            FieldPath.parse(indexId + ".a"), largeModelConfig,
            FieldPath.parse(indexId + ".b"), liteModelConfig);

    // Create a document with both fields
    BsonDocument innerDoc =
        new BsonDocument()
            .append("_id", new BsonString("anId"))
            .append("a", new BsonString("textA"))
            .append("b", new BsonString("textB"));
    BsonDocument bsonDoc = new BsonDocument(indexId.toString(), innerDoc);
    RawBsonDocument rawBsonDoc = BsonUtils.documentToRaw(bsonDoc);
    DocumentEvent event =
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(rawBsonDoc), indexId), rawBsonDoc);

    // Call getTextValueBundles
    var bundles =
        EmbeddingIndexingWorkScheduler.getTextValueBundles(
            List.of(event), fieldMapping, modelConfigPerPath, 100);

    // Should have one bundle with the document
    assertThat(bundles).hasSize(1);
    var bundle = bundles.get(0);

    // The bundle should have the document
    assertThat(bundle.getLeft()).hasSize(1);

    // The texts should be grouped by model
    var textsPerModel = bundle.getRight();
    assertThat(textsPerModel).hasSize(2);

    // Verify each model has the correct text
    assertThat(textsPerModel.get(largeModelConfig)).containsExactly("textA");
    assertThat(textsPerModel.get(liteModelConfig)).containsExactly("textB");
  }

  @After
  public void clearStaticRegistries() {
    EmbeddingServiceRegistry.clearRegistry();
  }

  private EmbeddingIndexingWorkScheduler scheduler(Supplier<EmbeddingServiceManager> supplier) {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    NamedExecutorService executor = Executors.fixedSizeThreadPool("indexing", 2, meterRegistry);
    return EmbeddingIndexingWorkScheduler.create(executor, supplier);
  }

  private EmbeddingIndexingWorkScheduler schedulerForMaterializedViewIndex(
      Supplier<EmbeddingServiceManager> supplier, GenerationId generationId) {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    NamedExecutorService executor = Executors.fixedSizeThreadPool("indexing", 2, meterRegistry);
    var matViewCollectionMetadataCatalog = new MaterializedViewCollectionMetadataCatalog();
    matViewCollectionMetadataCatalog.addMetadata(
        generationId,
        new MaterializedViewCollectionMetadata(
            VERSION_ZERO, UUID.randomUUID(), generationId.indexId.toHexString()));
    return EmbeddingIndexingWorkScheduler.createForMaterializedViewIndex(
        executor, supplier, matViewCollectionMetadataCatalog);
  }

  private static IndexCommitUserData getCommitUserData(MongoNamespace namespace, int token) {
    return IndexCommitUserData.createChangeStreamResume(
        ChangeStreamResumeInfo.create(namespace, new BsonDocument("token", new BsonInt32(token))),
        IndexFormatVersion.CURRENT);
  }

  private BsonDocument createBasicBson() {
    BsonDocument bsonDoc = new BsonDocument();
    bsonDoc
        .append("_id", new BsonString("anId"))
        .append("a", new BsonString("aString"))
        .append("b", new BsonString("bString"))
        .append("c", new BsonString("cString"));
    return bsonDoc;
  }

  private ImmutableMap<FieldPath, ImmutableMap<String, Vector>> expectedAutoEmbeddingsByPath(
      ObjectId indexId) {
    var autoEmbeddings = expectedAutoEmbeddings();
    ImmutableMap.Builder<FieldPath, ImmutableMap<String, Vector>> builder = ImmutableMap.builder();
    return builder
        .put(
            FieldPath.parse(indexId + ".a"),
            ImmutableMap.of("aString", autoEmbeddings.get("aString")))
        .put(
            FieldPath.parse(indexId + ".b"),
            ImmutableMap.of("bString", autoEmbeddings.get("bString")))
        .build();
  }

  /**
   * Creates per-field embeddings map for materialized view tests.
   * Each field in the mapping gets the same flat embeddings map.
   */
  private ImmutableMap<FieldPath, ImmutableMap<String, Vector>> expectedAutoEmbeddingsPerField(
      VectorIndexFieldMapping mappings) {
    var autoEmbeddings = expectedAutoEmbeddings();
    ImmutableMap.Builder<FieldPath, ImmutableMap<String, Vector>> builder = ImmutableMap.builder();
    for (FieldPath fieldPath : mappings.fieldMap().keySet()) {
      builder.put(fieldPath, autoEmbeddings);
    }
    return builder.build();
  }

  private ImmutableMap<String, Vector> expectedAutoEmbeddings() {
    ImmutableMap.Builder<String, Vector> builder = ImmutableMap.builder();
    return builder
        .put(
            "aString",
            Vector.fromBytes(
                new byte[]{
                    -83, -56, 64, 59, -50, 50, 122, 21, -77, -83, -41, -104, -57, 36, -17, -121,
                    -40, 57, -100, -45, 10, 94, -72, -48, -63, -22, 8, -27, -5, 2, -110, 104
                }))
        .put(
            "bString",
            Vector.fromBytes(
                new byte[]{
                    12, 104, 54, 121, -82, 90, 3, -18, 32, 16, 20, -92, -40, 55, 75, -28, -26, -93,
                    72, 76, -67, 26, 82, 6, -54, -57, -106, 57, -60, 20, -4, -118
                }))
        .build();
  }
}
