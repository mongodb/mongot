package com.xgen.mongot.server.command.search;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static com.xgen.testing.mongot.mock.cursor.MongotCursorBatches.mockInitialMongotCursorBatchForVectorSearch;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_GENERATION_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.lucene.LuceneVectorIndex;
import com.xgen.mongot.index.lucene.LuceneVectorIndexReader;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.mongodb.MongoDbServerInfo;
import com.xgen.testing.mongot.index.IndexMetricsUpdaterBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorSearchResultBatch;
import com.xgen.testing.mongot.server.command.search.definition.request.VectorSearchCommandDefinitionBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonArray;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Tests for VectorSearchCommand metrics recording with INDEX_SIZE_QUANTIZATION_METRICS feature
 * flag.
 */
@RunWith(MockitoJUnitRunner.class)
public class VectorSearchCommandIndexSizeMetricsTest {
  private static final String DATABASE_NAME = "testDb";
  private static final String COLLECTION_NAME = "testCollection";
  private static final String NAMESPACE = String.format("%s.%s", DATABASE_NAME, COLLECTION_NAME);
  private static final UUID COLLECTION_UUID = UUID.randomUUID();
  private static final String INDEX_NAME = "default";
  private static final int NUM_CANDIDATES = 5000;
  private static final int LIMIT = 1000;
  private static final Vector QUERY_VECTOR = Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE);
  private static final FieldPath PATH = FieldPath.parse("testPath");

  private static final Supplier<EmbeddingServiceManager> MOCK_EMBEDDING_SERVICE =
      mockEmbeddingServiceManager();

  @Test
  public void testMetricsRecordedWhenFeatureFlagEnabled() throws Exception {
    // Create feature flags with INDEX_SIZE_QUANTIZATION_METRICS enabled
    FeatureFlags featureFlagsEnabled =
        FeatureFlags.withDefaults().enable(Feature.INDEX_SIZE_QUANTIZATION_METRICS).build();

    var mocks = new Mocks();
    var metricsFactory = createSimpleMetricsFactory();
    var metrics = new VectorSearchCommand.Metrics(metricsFactory);
    var command =
        buildVectorSearchCommandWithFeatureFlags(
            mocks, metrics, featureFlagsEnabled, VectorQuantization.SCALAR);

    var result = command.run();

    // Verify the command succeeded
    var expected =
        mockInitialMongotCursorBatchForVectorSearch(0, new VectorSearchResultBatch(4), NAMESPACE)
            .toBson();
    Assert.assertEquals(expected, result);

    // Verify that the metric was recorded with the correct tags
    var timer = metricsFactory.get("vectorSearchCommandTotalLatencyByIndexSize").timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1);

    // Verify tags are present (note: timer also includes a timeUnit tag)
    var tags = timer.getId().getTags();
    assertThat(tags.stream().anyMatch(tag -> tag.getKey().equals("indexSizeCategory"))).isTrue();
    assertThat(tags.stream().anyMatch(tag -> tag.getKey().equals("quantizationType"))).isTrue();

    // Verify quantization type is correct
    assertThat(
            tags.stream()
                .filter(tag -> tag.getKey().equals("quantizationType"))
                .findFirst()
                .get()
                .getValue())
        .isEqualTo("scalar_quantized");
  }

  @Test
  public void testMetricsNotRecordedWhenFeatureFlagDisabled() throws Exception {
    // Create feature flags with INDEX_SIZE_QUANTIZATION_METRICS disabled (default)
    FeatureFlags featureFlagsDisabled = FeatureFlags.getDefault();

    var mocks = new Mocks();
    var metricsFactory = createSimpleMetricsFactory();
    var metrics = new VectorSearchCommand.Metrics(metricsFactory);
    var command =
        buildVectorSearchCommandWithFeatureFlags(
            mocks, metrics, featureFlagsDisabled, VectorQuantization.NONE);

    var result = command.run();

    // Verify the command succeeded
    var expected =
        mockInitialMongotCursorBatchForVectorSearch(0, new VectorSearchResultBatch(4), NAMESPACE)
            .toBson();
    Assert.assertEquals(expected, result);

    // Verify that the metric was NOT recorded (timer should not exist)
    var timer = mocks.meterRegistry.find("vectorSearchCommandTotalLatencyByIndexSize").timer();
    assertThat(timer).isNull();
  }

  @Test
  public void testMetricsRecordedWithDifferentQuantizationTypes() throws Exception {
    // Test with BINARY quantization
    FeatureFlags featureFlagsEnabled =
        FeatureFlags.withDefaults().enable(Feature.INDEX_SIZE_QUANTIZATION_METRICS).build();

    var mocks = new Mocks();
    var metricsFactory = createSimpleMetricsFactory();
    var metrics = new VectorSearchCommand.Metrics(metricsFactory);
    var command =
        buildVectorSearchCommandWithFeatureFlags(
            mocks, metrics, featureFlagsEnabled, VectorQuantization.BINARY);

    var result = command.run();

    // Verify the command succeeded
    var expected =
        mockInitialMongotCursorBatchForVectorSearch(0, new VectorSearchResultBatch(4), NAMESPACE)
            .toBson();
    Assert.assertEquals(expected, result);

    // Verify that the metric was recorded with binary quantization
    var timer = metricsFactory.get("vectorSearchCommandTotalLatencyByIndexSize").timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1);

    var tags = timer.getId().getTags();
    assertThat(
            tags.stream()
                .filter(tag -> tag.getKey().equals("quantizationType"))
                .findFirst()
                .get()
                .getValue())
        .isEqualTo("binary_quantized");
  }

  @Test
  public void testMetricsRecordedWithUnquantizedIndex() throws Exception {
    // Test with NONE (unquantized) quantization
    FeatureFlags featureFlagsEnabled =
        FeatureFlags.withDefaults().enable(Feature.INDEX_SIZE_QUANTIZATION_METRICS).build();

    var mocks = new Mocks();
    var metricsFactory = createSimpleMetricsFactory();
    var metrics = new VectorSearchCommand.Metrics(metricsFactory);
    var command =
        buildVectorSearchCommandWithFeatureFlags(
            mocks, metrics, featureFlagsEnabled, VectorQuantization.NONE);

    var result = command.run();

    // Verify the command succeeded
    var expected =
        mockInitialMongotCursorBatchForVectorSearch(0, new VectorSearchResultBatch(4), NAMESPACE)
            .toBson();
    Assert.assertEquals(expected, result);

    // Verify that the metric was recorded with unquantized
    var timer = metricsFactory.get("vectorSearchCommandTotalLatencyByIndexSize").timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1);

    var tags = timer.getId().getTags();
    assertThat(
            tags.stream()
                .filter(tag -> tag.getKey().equals("quantizationType"))
                .findFirst()
                .get()
                .getValue())
        .isEqualTo("unquantized");
  }

  private static VectorSearchCommand buildVectorSearchCommandWithFeatureFlags(
      Mocks mocks,
      VectorSearchCommand.Metrics metrics,
      FeatureFlags featureFlags,
      VectorQuantization quantization)
      throws Exception {
    // Update the mock to return the specified quantization type
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withVectorField(
                PATH.toString(),
                3,
                VectorSimilarity.DOT_PRODUCT,
                quantization,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm())
            .build();
    lenient().when(mocks.indexGeneration.getDefinition()).thenReturn(indexDef);
    lenient().when(mocks.initializedIndex.getDefinition()).thenReturn(indexDef);

    SearchCommandsRegister.BootstrapperMetadata metadata =
        new SearchCommandsRegister.BootstrapperMetadata(
            "testVersion", "localhost", () -> MongoDbServerInfo.EMPTY, featureFlags);

    VectorSearchQuery vectorSearchQuery =
        VectorQueryBuilder.builder()
            .index(INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(LIMIT)
                    .numCandidates(NUM_CANDIDATES)
                    .queryVector(QUERY_VECTOR)
                    .path(PATH)
                    .build())
            .build();

    return new VectorSearchCommand(
        VectorSearchCommandDefinitionBuilder.builder()
            .db(DATABASE_NAME)
            .collectionName(COLLECTION_NAME)
            .collectionUuid(COLLECTION_UUID)
            .vectorSearchQuery(vectorSearchQuery)
            .build(),
        mocks.catalog,
        mocks.initializedIndexCatalog,
        metadata,
        MOCK_EMBEDDING_SERVICE,
        metrics);
  }

  private static Supplier<EmbeddingServiceManager> mockEmbeddingServiceManager() {
    EmbeddingServiceManager manager = mock(EmbeddingServiceManager.class);
    return Suppliers.ofInstance(manager);
  }

  private static class Mocks {
    private final IndexCatalog catalog;
    private final InitializedIndexCatalog initializedIndexCatalog;
    private final IndexMetricsUpdater metricsUpdater;
    private final IndexGeneration indexGeneration;
    final InitializedVectorIndex initializedIndex;
    final LuceneVectorIndexReader reader;
    final BsonArray bsonResults;
    final SimpleMeterRegistry meterRegistry;

    public Mocks()
        throws ReaderClosedException,
            IOException,
            InvalidQueryException,
            IndexUnavailableException {
      var index = Mockito.mock(LuceneVectorIndex.class);

      this.initializedIndex = Mockito.mock(InitializedVectorIndex.class);
      lenient()
          .when(this.initializedIndex.getDefinition())
          .thenReturn(
              VectorIndexDefinitionBuilder.builder()
                  .withDotProductVectorField(PATH.toString(), 3)
                  .build());
      lenient()
          .when(this.initializedIndex.getGenerationId())
          .thenReturn(MOCK_INDEX_GENERATION_ID);
      lenient().when(index.isCompatibleWith(any(VectorIndexDefinition.class))).thenReturn(true);
      lenient().when(index.asVectorIndex()).thenCallRealMethod();
      lenient().when(this.initializedIndex.asVectorIndex()).thenCallRealMethod();

      this.catalog = Mockito.mock(IndexCatalog.class);
      this.initializedIndexCatalog = Mockito.mock(InitializedIndexCatalog.class);
      this.indexGeneration = mock(IndexGeneration.class);

      when(this.indexGeneration.getGenerationId())
          .thenReturn(MOCK_INDEX_DEFINITION_GENERATION.getGenerationId());
      when(this.indexGeneration.getDefinition())
          .thenReturn(
              VectorIndexDefinitionBuilder.builder()
                  .withDotProductVectorField(PATH.toString(), 3)
                  .build());
      lenient().when(this.indexGeneration.getIndex()).thenReturn(index);

      when(this.catalog.getIndex(DATABASE_NAME, COLLECTION_UUID, Optional.empty(), INDEX_NAME))
          .thenReturn(Optional.of(this.indexGeneration));

      when(this.initializedIndexCatalog.getIndex(MOCK_INDEX_GENERATION_ID))
          .thenReturn(Optional.of(this.initializedIndex));

      this.reader = Mockito.mock(LuceneVectorIndexReader.class);
      when(this.initializedIndex.getReader()).thenReturn(this.reader);

      this.bsonResults = new VectorSearchResultBatch(4).getBsonResults();
      when(this.reader.query(any())).thenReturn(this.bsonResults);

      this.metricsUpdater =
          IndexMetricsUpdaterBuilder.builder()
              .metricsFactory(SearchIndex.mockMetricsFactory())
              .indexMetricsSupplier(Mockito.mock(IndexMetricValuesSupplier.class))
              .build();
      when(this.initializedIndex.getMetricsUpdater()).thenReturn(this.metricsUpdater);

      // Mock getIndexSize() to return 100 MB
      lenient()
          .when(this.initializedIndex.getIndexSize())
          .thenReturn(1024L * 1024L * 100L); // 100 MB

      this.meterRegistry = new SimpleMeterRegistry();
    }
  }

  private static MetricsFactory createSimpleMetricsFactory() {
    return new MetricsFactory("testNamespace", new SimpleMeterRegistry());
  }
}

