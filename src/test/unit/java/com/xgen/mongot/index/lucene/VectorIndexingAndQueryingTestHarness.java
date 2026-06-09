package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_COLLECTION_UUID;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_DATABASE_NAME;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_ID;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_NAME;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_VECTOR_INDEX_DEFINITION_GENERATION;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.IndexWriter;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryFactory;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryHelper;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.util.AtomicDirectoryRemover;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.BsonVectorParser;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.LuceneConfigBuilder;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.ExactVectorCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.mock.index.IndexGeneration;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.VectorUtil;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.junit.Assert;

public class VectorIndexingAndQueryingTestHarness implements AutoCloseable {
  protected static final String FIELD_PATH = "vector.path";
  public static final VectorIndexDefinition MOCK_VECTOR_INDEX_DEFINITION_FOR_FLOAT_BYTE_VECTORS =
      buildVectorIndexDefinition(VectorSimilarity.COSINE, 3);
  public static final VectorIndexDefinition MOCK_VECTOR_INDEX_DEFINITION_FOR_BIT_VECTORS =
      buildVectorIndexDefinition(VectorSimilarity.EUCLIDEAN, 8);

  private final Random random = new Random(0);
  private LuceneVectorIndex index;
  private IndexWriter indexWriter;
  private VectorIndexReader indexReader;
  private Optional<NamedExecutorService> concurrentSearchExecutor = Optional.empty();
  private int numCandidates = 100;
  private int limit = 10;
  private double targetRecall = 0.9;

  static Vector fromNativeFloats(float[] floats) {
    return Vector.fromFloats(floats, FloatVector.OriginalType.NATIVE);
  }

  protected static VectorIndexDefinition buildVectorIndexDefinition(
      VectorSimilarity vectorSimilarity, int dimensions) {
    return buildVectorIndexDefinition(
        vectorSimilarity,
        dimensions,
        IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue(),
        VectorQuantization.NONE);
  }

  protected static VectorIndexDefinition buildVectorIndexDefinition(
      VectorSimilarity vectorSimilarity, int dimensions, int numPartitions) {
    return buildVectorIndexDefinition(
        vectorSimilarity, dimensions, numPartitions, VectorQuantization.NONE);
  }

  protected static VectorIndexDefinition buildVectorIndexDefinition(
      VectorSimilarity vectorSimilarity,
      int dimensions,
      int numPartitions,
      VectorQuantization quantization) {
    return VectorIndexDefinitionBuilder.builder()
        .indexId(MOCK_INDEX_ID)
        .name(MOCK_INDEX_NAME)
        .database(MOCK_INDEX_DATABASE_NAME)
        .lastObservedCollectionName(MOCK_INDEX_LAST_OBSERVED_COLLECTION_NAME)
        .collectionUuid(MOCK_INDEX_COLLECTION_UUID)
        .withFilterPath("filter.path")
        .withVectorFieldDefaultOptions(FIELD_PATH, dimensions, vectorSimilarity, quantization)
        .numPartitions(numPartitions)
        .build();
  }

  public VectorIndexReader getReader() {
    return this.indexReader;
  }

  public Optional<NamedExecutorService> getConcurrentSearchExecutor() {
    return this.concurrentSearchExecutor;
  }

  /** Must call setUp before using this method. */
  public void runTest(
      List<BsonDocument> docsToIndex, Vector queryVector, List<Integer> expectedDocIds)
      throws Exception {
    runTest(docsToIndex, queryVector, expectedDocIds, false);
  }

  public void runTest(
      List<BsonDocument> docsToIndex,
      Vector queryVector,
      List<Integer> expectedDocIds,
      boolean exact)
      throws Exception {
    try {
      for (BsonDocument bsonDocument : docsToIndex) {
        addDocument(bsonDocument);
      }
      var queryResultBsonArray = runVectorSearchQuery(queryVector, exact);
      validateQueryResults(queryResultBsonArray, expectedDocIds);
    } finally {
      close();
    }
  }

  public void runTestWithRecallCheck(
      List<BsonDocument> docsToIndex,
      List<? extends Vector> queryVectors,
      boolean exact,
      VectorSimilarity vectorSimilarity)
      throws Exception {
    for (BsonDocument bsonDocument : docsToIndex) {
      addDocument(bsonDocument);
    }
    @Var double totalRecall = 0;
    for (Vector queryVector : queryVectors) {
      var queryResultBsonArray = runVectorSearchQuery(queryVector, exact);
      var expectedDocIds = calculateExpectedDocIds(docsToIndex, queryVector, vectorSimilarity);
      totalRecall +=
          calculateRecall(
              queryResultBsonArray.stream().map(e -> getDocId(e.asDocument())).toList(),
              expectedDocIds);
    }
    var averageRecall = totalRecall / queryVectors.size();
    assertThat(averageRecall).isAtLeast(this.targetRecall);
  }

  /** Must call setUp before using this method. */
  public void runMultiQueryTest(
      List<BsonDocument> docsToIndex, Map<Vector, List<Integer>> queriesAndExpectedDocIds)
      throws Exception {
    runMultiQueryTest(docsToIndex, queriesAndExpectedDocIds, false);
  }

  public void runMultiQueryTest(
      List<BsonDocument> docsToIndex,
      Map<Vector, List<Integer>> queriesAndExpectedDocIds,
      boolean exact)
      throws Exception {
    try {
      for (BsonDocument bsonDocument : docsToIndex) {
        addDocument(bsonDocument);
      }
      queriesAndExpectedDocIds.forEach(
          (queryVector, expectedDocIds) -> {
            try {
              var queryResultBsonArray = runVectorSearchQuery(queryVector, exact);
              validateQueryResults(queryResultBsonArray, expectedDocIds);
            } catch (IOException | ReaderClosedException | InvalidQueryException e) {
              Assert.fail(e.getMessage());
            }
          });
    } finally {
      close();
    }
  }

  /** Set up resources for test. Must be called before runTest or runMultiQueryTest. */
  public void setUp(VectorIndexDefinition vectorIndexDefinition) throws Exception {
    setUp(vectorIndexDefinition, false);
  }

  /**
   * Set up resources for test with optional concurrent search over index partitions.
   *
   * @param vectorIndexDefinition the vector index definition
   * @param enableConcurrentPartitionSearch whether to enable concurrent search across partitions
   */
  public void setUp(
      VectorIndexDefinition vectorIndexDefinition, boolean enableConcurrentPartitionSearch)
      throws Exception {
    VectorIndexDefinitionGeneration vectorIndexDefinitionGeneration =
        IndexGeneration.mockDefinitionGeneration(vectorIndexDefinition);
    var folder = TestUtils.getTempFolder();
    var path = folder.getRoot().toPath();
    var luceneConfig = LuceneConfigBuilder.builder().dataPath(path).build();
    var indexPath = path.resolve("indexGeneration");
    var metadataPath = path.resolve("indexMapping/indexGeneration");
    var numPartitions = vectorIndexDefinitionGeneration.getIndexDefinition().getNumPartitions();
    var directoryFactory =
        new IndexDirectoryFactory(
            indexPath, metadataPath, luceneConfig, numPartitions, Optional.empty());
    var metricsFactory = VectorIndex.mockMetricsFactory();

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    if (enableConcurrentPartitionSearch) {
      this.concurrentSearchExecutor =
          Optional.of(
              spy(
                  Executors.fixedSizeThreadScheduledExecutor(
                      "testConcurrentSearchExecutor", 2, meterRegistry)));
    } else {
      this.concurrentSearchExecutor = Optional.empty();
    }
    var mergeScheduler = new InstrumentedConcurrentMergeScheduler(meterRegistry);
    // We have to manually call the setMaxMergesAndThreads() here.
    mergeScheduler.setMaxMergesAndThreads(10, 10);
    this.index =
        LuceneVectorIndex.createDiskBacked(
            indexPath,
            metadataPath,
            luceneConfig,
            FeatureFlags.getDefault(),
            mergeScheduler,
            new TieredMergePolicy(),
            new QueryCacheProvider.DefaultQueryCacheProvider(),
            mock(NamedScheduledExecutorService.class),
            this.concurrentSearchExecutor,
            Optional.empty(),
            vectorIndexDefinition,
            vectorIndexDefinitionGeneration.generation().indexFormatVersion,
            new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
            metricsFactory);
    FeatureFlags featureFlags =
        enableConcurrentPartitionSearch
            ? FeatureFlags.withDefaults()
                .enable(Feature.CONCURRENT_INDEX_PARTITION_SEARCH)
                .build()
            : FeatureFlags.getDefault();
    InitializedVectorIndex initializedIndex =
        InitializedLuceneVectorIndex.create(
            this.index,
            vectorIndexDefinitionGeneration.getGenerationId(),
            directoryFactory,
            mock(IndexDirectoryHelper.class),
            Optional.empty(),
            featureFlags,
            DynamicFeatureFlagRegistry.empty(),
            () -> false);
    this.indexWriter = initializedIndex.getWriter();
    this.indexReader = initializedIndex.getReader();
  }

  @Override
  public void close() throws IOException {
    this.index.close();
    this.concurrentSearchExecutor.ifPresent(NamedExecutorService::shutdown);
  }

  /**
   * Adds a document to the index. Can be used by tests that need to manually control document
   * indexing separate from querying.
   */
  public void addDocument(BsonDocument bsonDocument)
      throws IOException, FieldExceededLimitsException {
    ObjectId indexId = new ObjectId();
    bsonDocument.append(indexId.toString(), new BsonDocument("_id", bsonDocument.get("_id")));
    RawBsonDocument document = BsonUtils.documentToRaw(bsonDocument);
    this.indexWriter.updateIndex(
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document));
  }

  public BsonArray runVectorSearchQuery(Vector queryVector, boolean exact)
      throws IOException, ReaderClosedException, InvalidQueryException {
    // Ensure that all the documents updates will be visible in the tests.
    this.indexReader.refresh();
    this.index.setStatus(IndexStatus.steady());
    var vectorQuery = getQuery(queryVector, exact);
    return this.indexReader.query(
        new MaterializedVectorSearchQuery(vectorQuery, vectorQuery.criteria().queryVector().get()));
  }

  private VectorSearchQuery getQuery(Vector queryVector, boolean exact) {
    VectorSearchCriteria criteria =
        exact
            ? ExactVectorCriteriaBuilder.builder()
                .queryVector(queryVector)
                .path(FieldPath.newRoot(FIELD_PATH))
                .limit(this.limit)
                .build()
            : ApproximateVectorQueryCriteriaBuilder.builder()
                .queryVector(queryVector)
                .path(FieldPath.newRoot(FIELD_PATH))
                .numCandidates(this.numCandidates)
                .limit(this.limit)
                .build();
    return VectorQueryBuilder.builder()
        .index(MOCK_VECTOR_INDEX_DEFINITION_GENERATION.getIndexDefinition().getName())
        .criteria(criteria)
        .build();
  }

  public int getDocId(BsonDocument doc) {
    return doc.get("_id").asInt32().getValue();
  }

  private double getDocScore(BsonDocument doc) {
    return doc.get("$vectorSearchScore").asDouble().getValue();
  }

  private void validateQueryResults(BsonArray actualResults, List<Integer> expectedDocIds) {
    Assert.assertEquals(expectedDocIds.size(), actualResults.size());
    List<Integer> actualResultDocIds =
        actualResults.stream().map(e -> getDocId(e.asDocument())).toList();
    Assert.assertArrayEquals(expectedDocIds.toArray(), actualResultDocIds.toArray());

    List<Double> actualResultScores =
        actualResults.stream().map(e -> getDocScore(e.asDocument())).toList();
    assertThat(actualResultScores).isInOrder(Comparator.reverseOrder());
  }

  public <T> List<BsonDocument> createVectorDocs(
      T[] vectors, int startDocId, Function<T, Vector> vectorCreator) {
    return IntStream.range(0, vectors.length)
        .mapToObj(
            i -> {
              int docId = startDocId + i;
              T vector = vectors[i];
              return BsonDocument.parse(String.format("{_id: %d, data: 'some text'}", docId))
                  .append(FIELD_PATH, BsonVectorParser.encode(vectorCreator.apply(vector)));
            })
        .toList();
  }

  public float[][] generateNormalizedRandomFloatVectors(int numVectors, int vectorDimensions) {
    float[][] vectors = new float[numVectors][vectorDimensions];
    for (int i = 0; i < numVectors; i++) {
      @Var float sum = 0;
      for (int j = 0; j < vectorDimensions; j++) {
        vectors[i][j] = this.random.nextFloat() * 2 - 1; // Scale to -1 to 1
        sum += vectors[i][j] * vectors[i][j];
      }
      var sqrtSum = Math.sqrt(sum);
      for (int j = 0; j < vectorDimensions; j++) {
        vectors[i][j] /= (float) sqrtSum;
      }
    }
    return vectors;
  }

  public byte[][] generateRandomByteVectors(int numVectors, int vectorDimensions) {
    byte[][] vectors = new byte[numVectors][vectorDimensions];

    for (int i = 0; i < numVectors; i++) {
      this.random.nextBytes(vectors[i]);
    }
    return vectors;
  }

  public List<Integer> calculateExpectedDocIds(
      List<BsonDocument> docs, Vector queryVector, VectorSimilarity vectorSimilarity) {
    BiFunction<Vector, Vector, Double> euclidean = this::euclideanSimilarity;
    BiFunction<Vector, Vector, Double> dotProduct = this::dotProductSimilarity;
    BiFunction<Vector, Vector, Double> cosine = this::cosineSimilarity;
    var similarityFunction =
        switch (vectorSimilarity) {
          case EUCLIDEAN -> euclidean;
          case DOT_PRODUCT -> dotProduct;
          case COSINE -> cosine;
        };
    List<ScoreAndId> scoreAndIds = new ArrayList<>();
    for (BsonDocument doc : docs) {
      Vector docVector = BsonVectorParser.parse(doc.get(FIELD_PATH).asBinary());
      double score = similarityFunction.apply(docVector, queryVector);
      scoreAndIds.add(new ScoreAndId(getDocId(doc), score));
    }
    scoreAndIds.sort(Comparator.comparingDouble(ScoreAndId::score));
    return scoreAndIds.stream().limit(this.limit).map(ScoreAndId::id).toList();
  }

  private double calculateRecall(List<Integer> expectedDocIds, List<Integer> actualDocIds) {
    Assert.assertEquals(expectedDocIds.size(), actualDocIds.size());
    Set<Integer> expectedDocIdSet = new HashSet<>(expectedDocIds);
    Set<Integer> actualDocIdSet = new HashSet<>(actualDocIds);

    @Var int truePositives = 0;
    for (int expectedId : expectedDocIdSet) {
      if (actualDocIdSet.contains(expectedId)) {
        truePositives++;
      }
    }
    int falseNegatives = expectedDocIdSet.size() - truePositives;
    return (double) truePositives / (truePositives + falseNegatives);
  }

  public void setNumCandidatesAndLimit(int numCandidates, int limit) {
    this.numCandidates = numCandidates;
    this.limit = limit;
  }

  public void setTargetRecall(double targetRecall) {
    this.targetRecall = targetRecall;
  }

  private double euclideanSimilarity(Vector vector1, Vector vector2) {
    return switch (vector1.getVectorType()) {
      case FLOAT ->
          euclideanSimilarityFloatVector(
              vector1.asFloatVector().getFloatVector(), vector2.asFloatVector().getFloatVector());
      case BYTE ->
          euclideanSimilarityByteVector(
              vector1.asByteVector().getByteVector(), vector2.asByteVector().getByteVector());
      case BIT ->
          euclideanSimilarityBitVector(
              vector1.asBitVector().getBitVector(), vector2.asBitVector().getBitVector());
    };
  }

  private double euclideanSimilarityFloatVector(float[] vector1, float[] vector2) {
    @Var double score = 0.0;
    for (int i = 0; i < vector1.length; i++) {
      double difference = vector1[i] - vector2[i];
      score += difference * difference;
    }
    return Math.sqrt(score);
  }

  private double euclideanSimilarityByteVector(byte[] vector1, byte[] vector2) {
    @Var long score = 0L;
    for (int i = 0; i < vector1.length; i++) {
      long difference = (long) vector1[i] - vector2[i];
      score += difference * difference;
    }
    return Math.sqrt(score);
  }

  private double euclideanSimilarityBitVector(byte[] vector1, byte[] vector2) {
    int xorBitCount = VectorUtil.xorBitCount(vector1, vector2);
    int dimensions = vector1.length / Byte.SIZE;
    var score = (dimensions - xorBitCount) / (double) dimensions;
    return 1.0 / (1.0 + score);
  }

  private double dotProductSimilarity(Vector vector1, Vector vector2) {
    return switch (vector1.getVectorType()) {
      case FLOAT ->
          dotProductSimilarityFloatVector(
              vector1.asFloatVector().getFloatVector(), vector2.asFloatVector().getFloatVector());
      case BYTE ->
          dotProductSimilarityByteVector(
              vector1.asByteVector().getByteVector(), vector2.asByteVector().getByteVector());
      case BIT -> throw new RuntimeException("dot product similarity not supported for bit vector");
    };
  }

  private double dotProductSimilarityFloatVector(float[] vector1, float[] vector2) {
    @Var double dotProduct = 0.0;
    for (int i = 0; i < vector1.length; i++) {
      dotProduct += vector1[i] * vector2[i];
    }
    return 1 / (1 + dotProduct);
  }

  private double dotProductSimilarityByteVector(byte[] vector1, byte[] vector2) {
    @Var int dotProduct = 0;
    for (int i = 0; i < vector1.length; i++) {
      dotProduct += vector1[i] * vector2[i];
    }
    double scalingFactor = (vector1.length * (1 << 15));
    double score = 0.5 + dotProduct / scalingFactor;
    return 1 / (1.0 + score);
  }

  private double cosineSimilarity(Vector vector1, Vector vector2) {
    return switch (vector1.getVectorType()) {
      case FLOAT ->
          // For normalized float vectors, cosine and dotProduct similarities are the same
          dotProductSimilarityFloatVector(
              vector1.asFloatVector().getFloatVector(), vector2.asFloatVector().getFloatVector());
      case BYTE ->
          cosineSimilarityByteVector(
              vector1.asByteVector().getByteVector(), vector2.asByteVector().getByteVector());
      case BIT -> throw new RuntimeException("dot product similarity not supported for bit vector");
    };
  }

  private double cosineSimilarityByteVector(byte[] vector1, byte[] vector2) {
    @Var double dotProduct = 0.0;
    @Var double normA = 0.0;
    @Var double normB = 0.0;

    for (int i = 0; i < vector1.length; i++) {
      dotProduct += vector1[i] * vector2[i];
      normA += vector1[i] * vector1[i];
      normB += vector2[i] * vector2[i];
    }

    if (normA == 0 || normB == 0) {
      return 0; // Avoid division by zero
    }

    var score = dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    return 1 / (1 + score);
  }

  private record ScoreAndId(int id, double score) {}
}
