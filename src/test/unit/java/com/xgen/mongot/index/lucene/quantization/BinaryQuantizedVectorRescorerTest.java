package com.xgen.mongot.index.lucene.quantization;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.LuceneIndexSearcherReference;
import com.xgen.mongot.index.lucene.LuceneVectorSearchManager;
import com.xgen.mongot.index.lucene.extension.KnnByteVectorField;
import com.xgen.mongot.index.lucene.extension.KnnFloatVectorField;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnByteQuery;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnFloatQuery;
import com.xgen.mongot.index.lucene.query.util.WrappedToParentBlockJoinQuery;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class BinaryQuantizedVectorRescorerTest {

  private static final IndexMetricsUpdater.QueryingMetricsUpdater metrics =
      new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());

  private final IndexWriter indexWriter;
  private final String vectorFieldName;
  public final Optional<NamedExecutorService> executor;

  public final boolean sparse;
  private final VectorSimilarityFunction similarity;
  private final Vector.VectorType type;

  record TestSpec(
      boolean sparse,
      VectorSimilarityFunction sim,
      Optional<NamedExecutorService> executor,
      Vector.VectorType type) {}

  @Parameters(name = "{0}")
  public static Collection<TestSpec> data() {
    List<TestSpec> params = new ArrayList<>();

    for (VectorSimilarityFunction sim : List.of(DOT_PRODUCT, COSINE, EUCLIDEAN)) {
      for (boolean sparse : new boolean[] {true, false}) {
        for (var executor :
            List.of(
                Optional.<NamedExecutorService>empty(),
                Optional.of(
                    Executors.namedExecutor(
                        "test",
                        java.util.concurrent.Executors.newFixedThreadPool(2),
                        new SimpleMeterRegistry())))) {
          for (Vector.VectorType type : List.of(Vector.VectorType.FLOAT, Vector.VectorType.BYTE)) {
            params.add(new TestSpec(sparse, sim, executor, type));
          }
        }
      }
    }
    return params;
  }

  public BinaryQuantizedVectorRescorerTest(TestSpec params) throws IOException {
    var fieldPath = FieldPath.newRoot("field");
    this.sparse = params.sparse;
    this.executor = params.executor;
    this.similarity = params.sim;
    this.type = params.type;

    this.indexWriter =
        new IndexWriter(
            new ByteBuffersDirectory(),
            new IndexWriterConfig()
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setCodec(new Mongot01042BinaryQuantizedTestCodec()));

    this.vectorFieldName =
        (params.type == Vector.VectorType.FLOAT
                ? FieldName.TypeField.KNN_F32_Q1
                : FieldName.TypeField.KNN_BYTE_Q1)
            .getLuceneFieldName(fieldPath, Optional.empty());
  }

  @Parameterized.AfterParam
  public static void cleanup(TestSpec spec) {
    spec.executor.ifPresent(ExecutorService::close);
  }

  @Test
  public void dequantizedRescoring_euclidean_returnsResultsInOrder() throws IOException {
    Assume.assumeTrue(this.similarity == EUCLIDEAN);
    // insert documents into Lucene
    indexDocumentsAndCommit(
        toVectors(
            this.type,
            new float[] {10, -10},
            new float[] {20, 20},
            new float[] {10, -5},
            new float[] {30, 30},
            new float[] {3, -3},
            new float[] {50, 50}),
        EUCLIDEAN);
    createEmptySegment(2, this.type);

    // create search manager
    LuceneVectorSearchManager manager =
        createSearchManager(toVector(new float[] {1, -1}, this.type), 10, 100);

    var searcherManager =
        LuceneSearcherManager.create(
            this.indexWriter,
            new LuceneSearcherFactory(
                VectorIndex.MOCK_VECTOR_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH)),
            VectorIndex.mockMetricsFactory(),
            () -> false);
    var searcherReference =
        LuceneIndexSearcherReference.create(
            searcherManager,
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH),
            FeatureFlags.getDefault());
    var firstPassTopDocs = manager.initialSearch(searcherReference, 10).topDocs;
    var approximateRescorer =
        new BinaryQuantizedVectorRescorer.ApproximateRescorer(
            BinaryQuantizedRescoreQuery.from(manager.getLuceneQuery()));

    TopDocs fullResult =
        approximateRescorer.rescore(searcherReference.getIndexSearcher(), firstPassTopDocs, 10);
    Assert.assertEquals(6, fullResult.scoreDocs.length);
    // check that docs 0 and 2 are scored higher than 1 and 3 (but can't guarantee the exact order)
    assertThat(Stream.of(Arrays.copyOf(fullResult.scoreDocs, 3)).mapToInt(value -> value.doc))
        .containsExactly(0, 2, 4);
    assertThat(
            Stream.of(Arrays.copyOfRange(fullResult.scoreDocs, 3, 6)).mapToInt(value -> value.doc))
        .containsExactly(1, 3, 5);

    TopDocs subsetResult =
        approximateRescorer.rescore(searcherReference.getIndexSearcher(), firstPassTopDocs, 2);
    Assert.assertEquals(2, subsetResult.scoreDocs.length);
    assertThat(Stream.of(subsetResult.scoreDocs).mapToInt(value -> value.doc))
        .containsExactly(0, 2);
    assertScoresValid(subsetResult);
  }

  @Test
  public void dequantizedRescoring_allSimilarities_returnsResultsInOrder() throws IOException {
    // Temporarily skip DOT_PRODUCT due to incorrectly clamping of intermediate result.
    Assume.assumeFalse(this.similarity == DOT_PRODUCT);

    // insert vectors of equal norm since they are rank equivalent.
    indexDocumentsAndCommit(
        toVectors(
            this.type,
            new float[] {-70, -70, -10},
            new float[] {-70, -70, 10},
            new float[] {70, 70, 10}),
        this.similarity);
    createEmptySegment(3, this.type);

    // create search manager
    LuceneVectorSearchManager manager =
        createSearchManager(toVector(new float[] {96, 28, 1}, this.type), 10, 100);
    var searcherManager =
        LuceneSearcherManager.create(
            this.indexWriter,
            new LuceneSearcherFactory(
                VectorIndex.MOCK_VECTOR_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH)),
            VectorIndex.mockMetricsFactory(),
            () -> false);
    var searcherReference =
        LuceneIndexSearcherReference.create(
            searcherManager,
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH),
            FeatureFlags.getDefault());
    var firstPassTopDocs = manager.initialSearch(searcherReference, 10).topDocs;
    var approximateRescorer =
        new BinaryQuantizedVectorRescorer.ApproximateRescorer(
            BinaryQuantizedRescoreQuery.from(manager.getLuceneQuery()));

    TopDocs fullResult =
        approximateRescorer.rescore(searcherReference.getIndexSearcher(), firstPassTopDocs, 10);
    TestUtils.assertHasDocIds(fullResult.scoreDocs, 2, 1, 0);
  }

  @Test
  public void fullFidelityRescoring_euclidean_returnsResultsInOrder() throws IOException {
    Assume.assumeTrue(this.similarity == EUCLIDEAN);
    // insert documents into Lucene
    indexDocumentsAndCommit(
        toVectors(
            this.type,
            new float[] {1, 1},
            new float[] {2, 2},
            new float[] {10, 10},
            new float[] {11, 11}),
        EUCLIDEAN);
    createEmptySegment(2, this.type);

    // create search manager
    LuceneVectorSearchManager manager =
        createSearchManager(toVector(new float[] {9, 9}, this.type), 10, 100);

    var searcherManager =
        LuceneSearcherManager.create(
            this.indexWriter,
            new LuceneSearcherFactory(
                VectorIndex.MOCK_VECTOR_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH)),
            VectorIndex.mockMetricsFactory(),
            () -> false);

    var searcherReference =
        LuceneIndexSearcherReference.create(
            searcherManager,
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH),
            FeatureFlags.getDefault());
    var firstPassTopDocs = manager.initialSearch(searcherReference, 10).topDocs;
    var rescorer =
        new BinaryQuantizedVectorRescorer.FullFidelityRescorer(
            BinaryQuantizedRescoreQuery.from(manager.getLuceneQuery()), this.executor);

    TopDocs fullResult =
        rescorer.rescore(searcherReference.getIndexSearcher(), firstPassTopDocs, 10);
    assertThat(Stream.of(fullResult.scoreDocs).mapToInt(value -> value.doc))
        .containsExactly(2, 3, 1, 0)
        .inOrder();

    TopDocs subsetResult =
        rescorer.rescore(searcherReference.getIndexSearcher(), firstPassTopDocs, 2);
    assertThat(Stream.of(subsetResult.scoreDocs).mapToInt(value -> value.doc))
        .containsExactly(1, 0)
        .inOrder();
    assertScoresValid(subsetResult);
  }

  @Test
  public void basicTwoStageRescoring_euclidean_returnsResultsInOrder() throws IOException {
    Assume.assumeTrue(this.similarity == EUCLIDEAN);

    // insert documents into Lucene
    indexDocumentsAndCommit(
        toVectors(
            this.type,
            new float[] {10, -10},
            new float[] {20, 20},
            new float[] {10, -5},
            new float[] {50, 50}),
        EUCLIDEAN);
    createEmptySegment(2, this.type);

    // create search manager
    LuceneVectorSearchManager manager =
        createSearchManager(toVector(new float[] {1, -1}, this.type), 10, 100);

    var searcherManager =
        LuceneSearcherManager.create(
            this.indexWriter,
            new LuceneSearcherFactory(
                VectorIndex.MOCK_VECTOR_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH)),
            VectorIndex.mockMetricsFactory(),
            () -> false);

    var searcherReference =
        LuceneIndexSearcherReference.create(
            searcherManager,
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH),
            FeatureFlags.getDefault());
    var firstPass = manager.initialSearch(searcherReference, 10).topDocs;

    var rescorer = new BinaryQuantizedVectorRescorer(this.executor);
    TopDocs fullResult =
        rescorer.rescore(
            searcherReference.getIndexSearcher(),
            new TopDocs(firstPass.totalHits, firstPass.scoreDocs.clone()),
            createVectorSearchQuery(10, 20),
            BinaryQuantizedRescoreQuery.from(manager.getLuceneQuery()));

    assertThat(Stream.of(fullResult.scoreDocs).mapToInt(value -> value.doc))
        .containsExactly(2, 0, 1, 3)
        .inOrder();
    assertScoresValid(fullResult);
  }

  @Test
  public void basicTwoStageRescoringSubset_euclidean_returnsResultsInOrder() throws IOException {
    Assume.assumeTrue(this.similarity == EUCLIDEAN);

    // insert documents into Lucene
    indexDocumentsAndCommit(
        toVectors(
            this.type,
            new float[] {10, -10},
            new float[] {20, 20},
            new float[] {10, -5},
            new float[] {50, 50}),
        EUCLIDEAN);
    createEmptySegment(2, this.type);

    // create search manager
    LuceneVectorSearchManager manager =
        createSearchManager(toVector(new float[] {1, -1}, this.type), 10, 100);

    var searcherManager =
        LuceneSearcherManager.create(
            this.indexWriter,
            new LuceneSearcherFactory(
                VectorIndex.MOCK_VECTOR_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH)),
            VectorIndex.mockMetricsFactory(),
            () -> false);

    var searcherReference =
        LuceneIndexSearcherReference.create(
            searcherManager,
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH),
            FeatureFlags.getDefault());
    var firstPass = manager.initialSearch(searcherReference, 10).topDocs;

    var rescorer = new BinaryQuantizedVectorRescorer(this.executor);
    TopDocs subsetResult =
        rescorer.rescore(
            searcherReference.getIndexSearcher(),
            new TopDocs(firstPass.totalHits, firstPass.scoreDocs.clone()),
            createVectorSearchQuery(2, 20),
            BinaryQuantizedRescoreQuery.from(manager.getLuceneQuery()));

    assertThat(Stream.of(subsetResult.scoreDocs).mapToInt(value -> value.doc))
        .containsExactly(2, 0)
        .inOrder();
    assertScoresValid(subsetResult);
  }

  @Test
  public void multiSegmentTwoStageRescoring_euclidean_returnsResultsInOrder() throws IOException {
    Assume.assumeFalse(this.sparse);
    Assume.assumeTrue(this.similarity == EUCLIDEAN);

    // insert documents in multiple batches, effectively producing multiple segments
    indexDocumentsAndCommit(
        toVectors(this.type, new float[] {10, -10}, new float[] {20, 20}), EUCLIDEAN);
    indexDocumentsAndCommit(
        toVectors(this.type, new float[] {10, -5}, new float[] {30, 30}), EUCLIDEAN);
    indexDocumentsAndCommit(
        toVectors(this.type, new float[] {3, -3}, new float[] {50, 50}), EUCLIDEAN);
    createEmptySegment(2, this.type);

    // create search manager
    LuceneVectorSearchManager manager =
        createSearchManager(toVector(new float[] {1, -1}, this.type), 10, 100);

    var searcherManager =
        LuceneSearcherManager.create(
            this.indexWriter,
            new LuceneSearcherFactory(
                VectorIndex.MOCK_VECTOR_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH)),
            VectorIndex.mockMetricsFactory(),
            () -> false);

    var searcherReference =
        LuceneIndexSearcherReference.create(
            searcherManager,
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH),
            FeatureFlags.getDefault());
    var firstPassTopDocs = manager.initialSearch(searcherReference, 10).topDocs;

    var rescorer = new BinaryQuantizedVectorRescorer(this.executor);
    TopDocs result =
        rescorer.rescore(
            searcherReference.getIndexSearcher(),
            firstPassTopDocs,
            createVectorSearchQuery(10, 20),
            BinaryQuantizedRescoreQuery.from(manager.getLuceneQuery()));

    assertThat(Stream.of(result.scoreDocs).mapToInt(value -> value.doc))
        .containsExactly(4, 2, 0, 1, 3, 5)
        .inOrder();
    assertScoresValid(result);
  }

  @Test
  public void twoStageRescoring_allSimilarities_isIdempotent() throws IOException {
    // insert random documents into Lucene, and query with a random vector, of this.type
    List<Vector> vectors;
    Vector queryVector;
    if (this.type == Vector.VectorType.FLOAT) {
      Supplier<Float> generator = () -> (float) (ThreadLocalRandom.current().nextDouble() - 0.5);
      vectors =
          Stream.<Vector>generate(
                  () ->
                      Vector.fromFloats(
                          new float[] {
                            generator.get(), generator.get(), generator.get(), generator.get()
                          },
                          NATIVE))
              .limit(100)
              .toList();
      queryVector =
          Vector.fromFloats(
              new float[] {generator.get(), generator.get(), generator.get(), generator.get()},
              NATIVE);
    } else {
      Supplier<Byte> generator = () -> (byte) (ThreadLocalRandom.current().nextInt(256) - 128);
      vectors =
          Stream.<Vector>generate(
                  () ->
                      Vector.fromBytes(
                          new byte[] {
                            generator.get(), generator.get(), generator.get(), generator.get()
                          }))
              .limit(100)
              .toList();
      queryVector =
          Vector.fromBytes(
              new byte[] {generator.get(), generator.get(), generator.get(), generator.get()});
    }
    indexDocumentsAndCommit(vectors, this.similarity);
    createEmptySegment(4, this.type);

    // create search manager
    LuceneVectorSearchManager manager = createSearchManager(queryVector, 50, 100);

    var searcherManager =
        LuceneSearcherManager.create(
            this.indexWriter,
            new LuceneSearcherFactory(
                VectorIndex.MOCK_VECTOR_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH)),
            VectorIndex.mockMetricsFactory(),
            () -> false);

    var searcherReference =
        LuceneIndexSearcherReference.create(
            searcherManager,
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH),
            FeatureFlags.getDefault());
    var firstPassTopDocs = manager.initialSearch(searcherReference, 10).topDocs;

    var rescorer = new BinaryQuantizedVectorRescorer(this.executor);
    TopDocs firstRescore =
        rescorer.rescore(
            searcherReference.getIndexSearcher(),
            new TopDocs(firstPassTopDocs.totalHits, firstPassTopDocs.scoreDocs.clone()),
            createVectorSearchQuery(10, 20),
            BinaryQuantizedRescoreQuery.from(manager.getLuceneQuery()));
    TopDocs secondRescore =
        rescorer.rescore(
            searcherReference.getIndexSearcher(),
            new TopDocs(firstPassTopDocs.totalHits, firstPassTopDocs.scoreDocs.clone()),
            createVectorSearchQuery(10, 20),
            BinaryQuantizedRescoreQuery.from(manager.getLuceneQuery()));

    assertScoresValid(firstRescore);
    Assert.assertEquals(
        Stream.of(firstRescore.scoreDocs).map(doc -> doc.doc).toList(),
        Stream.of(secondRescore.scoreDocs).map(doc -> doc.doc).toList());
  }

  // ---- nested rescoring tests ----

  @Test
  public void nestedApproximateRescoring_euclidean_reordersParentsByMaxChildScore()
      throws IOException {
    Assume.assumeTrue(this.similarity == EUCLIDEAN);
    Assume.assumeFalse(this.sparse);

    // Parent A: children [1,-1] (sign pattern +,- same as query) and [1,1] (sign pattern +,+)
    // Parent B: child [-1,-1] (sign pattern -,- opposite to query)
    // Binary quantization encodes sign only, so [1,-1] and [3,-3] share the same code (+,-).
    // Using different sign patterns ensures the dequantized rescorer produces distinct scores:
    //   [+,-] vs query [+,-]: distance 0, score = 1.0
    //   [-,-] vs query [+,-]: both dimensions differ, much lower score
    this.indexWriter.addDocuments(
        nestedBlock(toVectorArray(this.type, new float[] {1, -1}, new float[] {1, 1})));
    this.indexWriter.addDocuments(nestedBlock(toVectorArray(this.type, new float[] {-1, -1})));
    this.indexWriter.commit();

    QueryBitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term(NESTED_PARENT_MARKER_FIELD, "T")));
    Query knnQuery = knnQueryResolver(new float[] {1, -1}, 10);
    WrappedToParentBlockJoinQuery blockJoinQuery =
        new WrappedToParentBlockJoinQuery(knnQuery, parentFilter, ScoreMode.Max);

    IndexSearcher searcher = buildSearcher();
    TopDocs parentTopDocs = searcher.search(blockJoinQuery, 10);
    assertThat(parentTopDocs.scoreDocs).hasLength(2);

    BinaryQuantizedVectorRescorer.ApproximateRescorer rescorer =
        new BinaryQuantizedVectorRescorer.ApproximateRescorer(
            BinaryQuantizedRescoreQuery.from(knnQuery), Optional.of(parentFilter));
    TopDocs rescored = rescorer.rescore(searcher, parentTopDocs, 10);

    assertThat(rescored.scoreDocs).hasLength(2);
    // Parent A ranks first: its [1,-1] child has euclidean score = 1.0 (zero distance)
    // Parent B's [-1,-1] child scores significantly lower
    assertThat(rescored.scoreDocs[0].score).isGreaterThan(rescored.scoreDocs[1].score);
    assertThat(rescored.scoreDocs[0].score).isGreaterThan(0.5f);
    assertScoresValid(rescored);
  }

  @Test
  public void nestedFullFidelityRescoring_euclidean_reordersParentsByMaxChildScore()
      throws IOException {
    Assume.assumeTrue(this.similarity == EUCLIDEAN);
    Assume.assumeFalse(this.sparse);

    // Same layout as the approximate test; full-fidelity scores should give the same ordering.
    this.indexWriter.addDocuments(
        nestedBlock(toVectorArray(this.type, new float[] {1, -1}, new float[] {50, 50})));
    this.indexWriter.addDocuments(nestedBlock(toVectorArray(this.type, new float[] {3, -3})));
    this.indexWriter.commit();

    QueryBitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term(NESTED_PARENT_MARKER_FIELD, "T")));
    Query knnQuery = knnQueryResolver(new float[] {1, -1}, 10);
    WrappedToParentBlockJoinQuery blockJoinQuery =
        new WrappedToParentBlockJoinQuery(knnQuery, parentFilter, ScoreMode.Max);

    IndexSearcher searcher = buildSearcher();
    TopDocs parentTopDocs = searcher.search(blockJoinQuery, 10);
    assertThat(parentTopDocs.scoreDocs).hasLength(2);

    BinaryQuantizedVectorRescorer.FullFidelityRescorer rescorer =
        new BinaryQuantizedVectorRescorer.FullFidelityRescorer(
            BinaryQuantizedRescoreQuery.from(knnQuery), this.executor, Optional.of(parentFilter));
    TopDocs rescored = rescorer.rescore(searcher, parentTopDocs, 10);

    assertThat(rescored.scoreDocs).hasLength(2);
    assertThat(rescored.scoreDocs[0].score).isGreaterThan(rescored.scoreDocs[1].score);
    // Full-fidelity [1,-1] vs [1,-1]: euclidean score = 1.0 exactly
    assertThat(rescored.scoreDocs[0].score).isWithin(1e-4f).of(1.0f);
    assertScoresValid(rescored);
  }

  @Test
  public void nestedTwoStageRescoring_euclidean_returnsResultsInOrder() throws IOException {
    Assume.assumeTrue(this.similarity == EUCLIDEAN);
    Assume.assumeFalse(this.sparse);

    this.indexWriter.addDocuments(
        nestedBlock(toVectorArray(this.type, new float[] {1, -1}, new float[] {50, 50})));
    this.indexWriter.addDocuments(nestedBlock(toVectorArray(this.type, new float[] {3, -3})));
    this.indexWriter.commit();

    QueryBitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term(NESTED_PARENT_MARKER_FIELD, "T")));
    Query knnQuery = knnQueryResolver(new float[] {1, -1}, 10);
    WrappedToParentBlockJoinQuery blockJoinQuery =
        new WrappedToParentBlockJoinQuery(knnQuery, parentFilter, ScoreMode.Max);

    IndexSearcher searcher = buildSearcher();
    TopDocs parentTopDocs = searcher.search(blockJoinQuery, 10);
    assertThat(parentTopDocs.scoreDocs).hasLength(2);

    BinaryQuantizedVectorRescorer rescorer = new BinaryQuantizedVectorRescorer(this.executor);
    ApproximateVectorSearchCriteria criteria = createVectorSearchQuery(2, 10);
    TopDocs rescored =
        rescorer.rescore(
            searcher,
            parentTopDocs,
            criteria,
            BinaryQuantizedRescoreQuery.from(knnQuery),
            Optional.of(parentFilter));

    assertThat(rescored.scoreDocs).hasLength(2);
    assertThat(rescored.scoreDocs[0].score).isGreaterThan(rescored.scoreDocs[1].score);
    assertThat(rescored.scoreDocs[0].score).isWithin(1e-4f).of(1.0f);
    assertScoresValid(rescored);
  }

  @Test
  public void nestedRescoring_parentWithNoChildren_getsZeroScore() throws IOException {
    Assume.assumeTrue(this.similarity == EUCLIDEAN);
    Assume.assumeFalse(this.sparse);

    // Doc 0: child with vector; Doc 1: parent1 (well-formed block).
    this.indexWriter.addDocuments(nestedBlock(toVectorArray(this.type, new float[] {1, -1})));
    // Doc 2: bare parent with no preceding child — simulates a malformed or split block.
    // The parentBitSet will mark both doc 1 and doc 2 as parents, so for doc 2:
    //   prevParent = 1, firstChild = 2, localParentDoc = 2 → firstChild >= localParentDoc.
    Document bareParent = new Document();
    bareParent.add(new StringField(NESTED_PARENT_MARKER_FIELD, "T", Field.Store.NO));
    this.indexWriter.addDocument(bareParent);
    this.indexWriter.commit();

    QueryBitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term(NESTED_PARENT_MARKER_FIELD, "T")));
    Query knnQuery = knnQueryResolver(new float[] {1, -1}, 10);

    IndexSearcher searcher = buildSearcher();

    // Manually inject both parents with a stale ANN score of 0.8 — this is what the rescorer
    // receives before it replaces scores. The bare parent (doc 2) must end up with 0, not 0.8.
    float annScore = 0.8f;
    TopDocs injectedTopDocs =
        new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {new ScoreDoc(1, annScore), new ScoreDoc(2, annScore)});

    BinaryQuantizedVectorRescorer.FullFidelityRescorer rescorer =
        new BinaryQuantizedVectorRescorer.FullFidelityRescorer(
            BinaryQuantizedRescoreQuery.from(knnQuery), this.executor, Optional.of(parentFilter));
    TopDocs rescored = rescorer.rescore(searcher, injectedTopDocs, 10);

    assertThat(rescored.scoreDocs).hasLength(2);

    // Results are sorted descending by score, so parent1 (positive rescored score) comes first.
    float parent1Score =
        Arrays.stream(rescored.scoreDocs).filter(sd -> sd.doc == 1).findFirst().get().score;
    float bareParentScore =
        Arrays.stream(rescored.scoreDocs).filter(sd -> sd.doc == 2).findFirst().get().score;

    // parent1 has a real child vector — rescored score must be positive.
    assertThat(parent1Score).isGreaterThan(0f);
    // bare parent has no children — must get 0, not the stale ANN score of 0.8.
    assertThat(bareParentScore).isEqualTo(0f);
    assertThat(bareParentScore).isLessThan(annScore);
  }

  // ---- nested rescoring with child filter tests ----

  @Test
  public void nestedApproximateRescoring_withChildFilter_excludesNonMatchingChildren()
      throws IOException {
    Assume.assumeTrue(this.similarity == EUCLIDEAN);
    Assume.assumeFalse(this.sparse);

    // Parent A: childA1 matches filter (BQ sign -,+ → worst similarity to query +,-);
    //           childA2 does NOT match filter (BQ sign +,- → perfect similarity to query).
    // Parent B: childB1 matches filter (BQ sign +,+ → medium similarity to query +,-).
    // childA2's high score must not bleed into Parent A's rescored result.
    String childMatchField = "childMatch";
    TermQuery childFilter = new TermQuery(new Term(childMatchField, "T"));

    Document childA1 = new Document();
    childA1.add(vectorField(new float[] {-1, 1}));
    childA1.add(new StringField(childMatchField, "T", Field.Store.NO));
    Document childA2 = new Document();
    childA2.add(vectorField(new float[] {1, -1}));
    Document parentA = new Document();
    parentA.add(new StringField(NESTED_PARENT_MARKER_FIELD, "T", Field.Store.NO));

    Document childB1 = new Document();
    childB1.add(vectorField(new float[] {1, 1}));
    childB1.add(new StringField(childMatchField, "T", Field.Store.NO));
    Document parentB = new Document();
    parentB.add(new StringField(NESTED_PARENT_MARKER_FIELD, "T", Field.Store.NO));

    this.indexWriter.addDocuments(List.of(childA1, childA2, parentA));
    this.indexWriter.addDocuments(List.of(childB1, parentB));
    this.indexWriter.commit();

    QueryBitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term(NESTED_PARENT_MARKER_FIELD, "T")));
    Query knnQuery = knnQueryResolver(new float[] {1, -1}, 10, childFilter);
    WrappedToParentBlockJoinQuery blockJoinQuery =
        new WrappedToParentBlockJoinQuery(knnQuery, parentFilter, ScoreMode.Max);

    IndexSearcher searcher = buildSearcher();
    TopDocs parentTopDocs = searcher.search(blockJoinQuery, 10);
    assertThat(parentTopDocs.scoreDocs).hasLength(2);

    BinaryQuantizedVectorRescorer.ApproximateRescorer rescorer =
        new BinaryQuantizedVectorRescorer.ApproximateRescorer(
            BinaryQuantizedRescoreQuery.from(knnQuery), Optional.of(parentFilter));
    TopDocs rescored = rescorer.rescore(searcher, parentTopDocs, 10);

    assertThat(rescored.scoreDocs).hasLength(2);
    assertThat(rescored.scoreDocs[0].score).isGreaterThan(rescored.scoreDocs[1].score);
    assertScoresValid(rescored);
  }

  @Test
  public void nestedFullFidelityRescoring_withChildFilter_excludesNonMatchingChildren()
      throws IOException {
    Assume.assumeTrue(this.similarity == EUCLIDEAN);
    Assume.assumeFalse(this.sparse);

    // Same layout as the approximate test; full-fidelity scores are exact.
    String childMatchField = "childMatch";
    TermQuery childFilter = new TermQuery(new Term(childMatchField, "T"));

    Document childA1 = new Document();
    childA1.add(vectorField(new float[] {-1, 1}));
    childA1.add(new StringField(childMatchField, "T", Field.Store.NO));
    Document childA2 = new Document();
    childA2.add(vectorField(new float[] {1, -1}));
    Document parentA = new Document();
    parentA.add(new StringField(NESTED_PARENT_MARKER_FIELD, "T", Field.Store.NO));

    Document childB1 = new Document();
    childB1.add(vectorField(new float[] {1, 1}));
    childB1.add(new StringField(childMatchField, "T", Field.Store.NO));
    Document parentB = new Document();
    parentB.add(new StringField(NESTED_PARENT_MARKER_FIELD, "T", Field.Store.NO));

    this.indexWriter.addDocuments(List.of(childA1, childA2, parentA));
    this.indexWriter.addDocuments(List.of(childB1, parentB));
    this.indexWriter.commit();

    QueryBitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term(NESTED_PARENT_MARKER_FIELD, "T")));
    Query knnQuery = knnQueryResolver(new float[] {1, -1}, 10, childFilter);
    WrappedToParentBlockJoinQuery blockJoinQuery =
        new WrappedToParentBlockJoinQuery(knnQuery, parentFilter, ScoreMode.Max);

    IndexSearcher searcher = buildSearcher();
    TopDocs parentTopDocs = searcher.search(blockJoinQuery, 10);
    assertThat(parentTopDocs.scoreDocs).hasLength(2);

    BinaryQuantizedVectorRescorer.FullFidelityRescorer rescorer =
        new BinaryQuantizedVectorRescorer.FullFidelityRescorer(
            BinaryQuantizedRescoreQuery.from(knnQuery), this.executor, Optional.of(parentFilter));
    TopDocs rescored = rescorer.rescore(searcher, parentTopDocs, 10);

    assertThat(rescored.scoreDocs).hasLength(2);
    assertThat(rescored.scoreDocs[0].score).isGreaterThan(rescored.scoreDocs[1].score);
    // Parent B's score = 1/(1 + ||[1,-1]-[1,1]||²) = 1/5 = 0.2
    assertThat(rescored.scoreDocs[0].score).isWithin(1e-4f).of(0.2f);
    assertScoresValid(rescored);
  }

  @Test
  public void nestedTwoStageRescoring_withChildFilter_excludesNonMatchingChildren()
      throws IOException {
    Assume.assumeTrue(this.similarity == EUCLIDEAN);
    Assume.assumeFalse(this.sparse);

    String childMatchField = "childMatch";
    TermQuery childFilter = new TermQuery(new Term(childMatchField, "T"));

    Document childA1 = new Document();
    childA1.add(vectorField(new float[] {-1, 1}));
    childA1.add(new StringField(childMatchField, "T", Field.Store.NO));
    Document childA2 = new Document();
    childA2.add(vectorField(new float[] {1, -1}));
    Document parentA = new Document();
    parentA.add(new StringField(NESTED_PARENT_MARKER_FIELD, "T", Field.Store.NO));

    Document childB1 = new Document();
    childB1.add(vectorField(new float[] {1, 1}));
    childB1.add(new StringField(childMatchField, "T", Field.Store.NO));
    Document parentB = new Document();
    parentB.add(new StringField(NESTED_PARENT_MARKER_FIELD, "T", Field.Store.NO));

    this.indexWriter.addDocuments(List.of(childA1, childA2, parentA));
    this.indexWriter.addDocuments(List.of(childB1, parentB));
    this.indexWriter.commit();

    QueryBitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term(NESTED_PARENT_MARKER_FIELD, "T")));
    Query knnQuery = knnQueryResolver(new float[] {1, -1}, 10, childFilter);
    WrappedToParentBlockJoinQuery blockJoinQuery =
        new WrappedToParentBlockJoinQuery(knnQuery, parentFilter, ScoreMode.Max);

    IndexSearcher searcher = buildSearcher();
    TopDocs parentTopDocs = searcher.search(blockJoinQuery, 10);
    assertThat(parentTopDocs.scoreDocs).hasLength(2);

    BinaryQuantizedVectorRescorer rescorer = new BinaryQuantizedVectorRescorer(this.executor);
    ApproximateVectorSearchCriteria criteria = createVectorSearchQuery(2, 10);
    TopDocs rescored =
        rescorer.rescore(
            searcher,
            parentTopDocs,
            criteria,
            BinaryQuantizedRescoreQuery.from(knnQuery),
            Optional.of(parentFilter));

    assertThat(rescored.scoreDocs).hasLength(2);
    assertThat(rescored.scoreDocs[0].score).isGreaterThan(rescored.scoreDocs[1].score);
    assertScoresValid(rescored);
  }

  // ---- byte-only boundary test ----

  @Test
  public void fullFidelityByteRescoring_int8Boundaries_returnsResultsInOrder() throws IOException {
    Assume.assumeTrue(this.type == Vector.VectorType.BYTE);
    Assume.assumeTrue(this.similarity == EUCLIDEAN);
    indexDocumentsAndCommit(
        List.of(
                new byte[] {-128, 100},
                new byte[] {80, -128},
                new byte[] {20, 30},
                new byte[] {127, 127})
            .stream()
            .<Vector>map(Vector::fromBytes)
            .toList(),
        EUCLIDEAN);
    createEmptySegment(2, this.type);

    LuceneVectorSearchManager manager =
        createSearchManager(Vector.fromBytes(new byte[] {9, 9}), 10, 100);

    var searcherManager =
        LuceneSearcherManager.create(
            this.indexWriter,
            new LuceneSearcherFactory(
                VectorIndex.MOCK_VECTOR_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH)),
            VectorIndex.mockMetricsFactory(),
            () -> false);

    var searcherReference =
        LuceneIndexSearcherReference.create(
            searcherManager,
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH),
            FeatureFlags.getDefault());
    var firstPassTopDocs = manager.initialSearch(searcherReference, 10).topDocs;
    var rescorer =
        new BinaryQuantizedVectorRescorer.FullFidelityRescorer(
            BinaryQuantizedRescoreQuery.from(manager.getLuceneQuery()), this.executor);

    TopDocs fullResult =
        rescorer.rescore(searcherReference.getIndexSearcher(), firstPassTopDocs, 10);
    assertThat(Stream.of(fullResult.scoreDocs).mapToInt(value -> value.doc))
        .containsExactly(2, 1, 0, 3)
        .inOrder();

    TopDocs subsetResult =
        rescorer.rescore(searcherReference.getIndexSearcher(), firstPassTopDocs, 2);
    assertThat(Stream.of(subsetResult.scoreDocs).mapToInt(value -> value.doc))
        .containsExactly(2, 3)
        .inOrder();
    assertScoresValid(subsetResult);
  }

  private static final String NESTED_PARENT_MARKER_FIELD = "$meta/embeddedRoot";

  /** Creates a Lucene document block: child docs (with vectors) followed by one parent doc. */
  private List<Document> nestedBlock(Vector... childVectors) {
    List<Document> block =
        Arrays.stream(childVectors)
            .map(
                vector -> {
                  Document child = new Document();
                  child.add(vectorField(vector, this.similarity));
                  return child;
                })
            .collect(Collectors.toList());
    Document parent = new Document();
    parent.add(new StringField(NESTED_PARENT_MARKER_FIELD, "T", Field.Store.NO));
    block.add(parent);
    return block;
  }

  /** Converts a float fixture into a byte-valid {@link Vector} of the given type. */
  private static Vector toVector(float[] raw, Vector.VectorType type) {
    if (type == Vector.VectorType.FLOAT) {
      return Vector.fromFloats(raw, NATIVE);
    }
    byte[] bytes = new byte[raw.length];
    for (int i = 0; i < raw.length; i++) {
      long rounded = Math.round(raw[i]);
      if (rounded < Byte.MIN_VALUE || rounded > Byte.MAX_VALUE) {
        throw new IllegalArgumentException(
            "Vector value is outside the signed byte range: " + raw[i]);
      }
      bytes[i] = (byte) rounded;
    }
    return Vector.fromBytes(bytes);
  }

  private static List<Vector> toVectors(Vector.VectorType type, float[]... raws) {
    return Arrays.stream(raws).map(v -> toVector(v, type)).toList();
  }

  private static Vector[] toVectorArray(Vector.VectorType type, float[]... raws) {
    return Arrays.stream(raws).map(v -> toVector(v, type)).toArray(Vector[]::new);
  }

  /** Builds a {@link KnnFloatVectorField} or {@link KnnByteVectorField} for {@code this.type}. */
  private Field vectorField(float[] raw) {
    return vectorField(toVector(raw, this.type), this.similarity);
  }

  /** Builds a {@link KnnFloatVectorField} or {@link KnnByteVectorField} for {@code vector}. */
  private Field vectorField(Vector vector, VectorSimilarityFunction similarity) {
    return vector.getVectorType() == Vector.VectorType.FLOAT
        ? new KnnFloatVectorField(
            this.vectorFieldName, vector.asFloatVector().getFloatVector(), similarity)
        : new KnnByteVectorField(this.vectorFieldName, vector.getBytes(), similarity);
  }

  /** Builds a {@link MongotKnnFloatQuery} or {@link MongotKnnByteQuery} for {@code this.type}. */
  private Query knnQueryResolver(float[] rawQueryVector, int numCandidates) {
    return knnQueryResolver(toVector(rawQueryVector, this.type), numCandidates);
  }

  /** Builds a {@link MongotKnnFloatQuery} or {@link MongotKnnByteQuery} for {@code queryVector}. */
  private Query knnQueryResolver(Vector queryVector, int numCandidates) {
    if (queryVector.getVectorType() == Vector.VectorType.FLOAT) {
      return new MongotKnnFloatQuery(
          metrics,
          this.vectorFieldName,
          queryVector.asFloatVector().getFloatVector(),
          numCandidates);
    }
    return new MongotKnnByteQuery(
        metrics, this.vectorFieldName, queryVector.getBytes(), numCandidates);
  }

  /** Overload of {@link #knnQueryResolver(float[], int)} that also applies a child filter. */
  private Query knnQueryResolver(float[] rawQueryVector, int numCandidates, Query childFilter) {
    return knnQueryResolver(toVector(rawQueryVector, this.type), numCandidates, childFilter);
  }

  /** Overload of {@link #knnQueryResolver(Vector, int)} that also applies a child filter. */
  private Query knnQueryResolver(Vector queryVector, int numCandidates, Query childFilter) {
    if (queryVector.getVectorType() == Vector.VectorType.FLOAT) {
      return new MongotKnnFloatQuery(
          metrics,
          FeatureFlags.getDefault(),
          this.vectorFieldName,
          queryVector.asFloatVector().getFloatVector(),
          numCandidates,
          childFilter);
    }
    return new MongotKnnByteQuery(
        metrics, this.vectorFieldName, queryVector.getBytes(), numCandidates, childFilter);
  }

  private IndexSearcher buildSearcher() throws IOException {
    var searcherManager =
        LuceneSearcherManager.create(
            this.indexWriter,
            new LuceneSearcherFactory(
                VectorIndex.MOCK_VECTOR_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH)),
            VectorIndex.mockMetricsFactory(),
            () -> false);
    var searcherReference =
        LuceneIndexSearcherReference.create(
            searcherManager,
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH),
            FeatureFlags.getDefault());
    return searcherReference.getIndexSearcher();
  }

  /** Assert all final scores are [0, 1], which is required for vectorSearch. */
  private static void assertScoresValid(TopDocs topDocs) {
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      assertThat(scoreDoc.score).isAtLeast(0f);
      assertThat(scoreDoc.score).isAtMost(1f);
    }
  }

  /** Create a segment with ghost vector values. */
  private void createEmptySegment(int dim, Vector.VectorType type) throws IOException {
    var toBeDeleted = new Document();
    toBeDeleted.add(new TextField("delete", "delete", Field.Store.NO));
    float[] v = new float[dim];
    v[1] = 1f;
    toBeDeleted.add(vectorField(toVector(v, type), this.similarity));

    var docToKeepSegmentLive = new Document();
    docToKeepSegmentLive.add(new StringField("keep", "keep", Field.Store.NO));
    this.indexWriter.addDocument(docToKeepSegmentLive);
    this.indexWriter.addDocument(toBeDeleted);
    this.indexWriter.commit();

    this.indexWriter.deleteDocuments(new Term("delete", "delete"));
    this.indexWriter.commit();
  }

  private void indexDocumentsAndCommit(List<Vector> vectors, VectorSimilarityFunction similarity)
      throws IOException {

    for (Vector vector : vectors) {
      var document = new Document();
      document.add(vectorField(vector, similarity));

      this.indexWriter.addDocument(document);
    }

    // If set, index a document without a vector field to trigger SparseOffHeapVectorValue path
    if (this.sparse) {
      this.indexWriter.addDocument(new Document());
    }

    this.indexWriter.commit();
  }

  private LuceneVectorSearchManager createSearchManager(
      Vector queryVector, int limit, int numCandidates) {

    VectorSearchQuery vectorQuery =
        VectorQueryBuilder.builder()
            .index("myVectorIndex")
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .queryVector(queryVector)
                    .path(FieldPath.newRoot("root"))
                    .limit(limit)
                    .numCandidates(numCandidates)
                    .build())
            .build();
    Query luceneQuery = knnQueryResolver(queryVector, numCandidates);
    return new LuceneVectorSearchManager(
        luceneQuery, vectorQuery.criteria(), Optional.empty(), Optional.empty());
  }

  private static ApproximateVectorSearchCriteria createVectorSearchQuery(
      int limit, int numCandidates) {
    return ApproximateVectorQueryCriteriaBuilder.builder()
        .path(FieldPath.newRoot("field"))
        .queryVector(Vector.fromFloats(new float[] {}, NATIVE))
        .limit(limit)
        .numCandidates(numCandidates)
        .build();
  }
}
