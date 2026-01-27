package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockMetricsFactory;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexTypeData;
import com.xgen.mongot.index.MeteredIndexWriter;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryFactory;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryHelper;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.query.LuceneVectorQueryFactoryDistributor;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.AtomicDirectoryRemover;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.testing.ConcurrencyTestUtils;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.lucene.LuceneConfigBuilder;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilterVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.VectorScorer;
import org.bson.BsonArray;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

@RunWith(Enclosed.class)
public class TestLuceneVectorIndexReader {
  /**
   * Creates a new Leaf reader with the same fields as {@code reader} except any vector fields will
   * appear to have size={@code numVectors} and dim={@code dimension}.
   */
  private static LeafReader createLeafWithFakeVectors(
      LeafReader reader, int numVectors, int dimension) {
    return new FilterLeafReader(reader) {
      @Override
      public CacheHelper getReaderCacheHelper() {
        return this.in.getReaderCacheHelper();
      }

      @Override
      public CacheHelper getCoreCacheHelper() {
        return this.in.getCoreCacheHelper();
      }

      @Override
      public ByteVectorValues getByteVectorValues(String field) {
        return new ByteVectorValues() {
          @Override
          public int dimension() {
            return dimension;
          }

          @Override
          public int size() {
            return numVectors;
          }

          @Override
          public byte[] vectorValue() {
            return new byte[dimension];
          }

          @Override
          public VectorScorer scorer(byte[] query) {
            return null;
          }

          @Override
          public int docID() {
            return 0;
          }

          @Override
          public int nextDoc() {
            return 0;
          }

          @Override
          public int advance(int target) {
            return 0;
          }
        };
      }

      @Override
      public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return new FilterVectorValues(this.in.getFloatVectorValues(field)) {
          @Override
          public VectorScorer scorer(float[] query) throws IOException {
            return super.in.scorer(query);
          }

          @Override
          public int size() {
            return numVectors;
          }

          @Override
          public int dimension() {
            return dimension;
          }
        };
      }
    };
  }

  public static class TestIndexReader {
    private static final VectorSearchQuery UNQUANTIZED_QUERY_DEFINITION =
        VectorQueryBuilder.builder()
            .index(MOCK_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(5)
                    .numCandidates(10)
                    .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                    .path(FieldPath.newRoot("unquantized.vector.path"))
                    .build())
            .build();
    private static final MaterializedVectorSearchQuery UNQUANTIZED_MATERIALIZED_QUERY =
        new MaterializedVectorSearchQuery(
            UNQUANTIZED_QUERY_DEFINITION,
            UNQUANTIZED_QUERY_DEFINITION.criteria().queryVector().get());

    private static final VectorSearchQuery SCALAR_QUANTIZED_QUERY_DEFINITION =
        VectorQueryBuilder.builder()
            .index(MOCK_INDEX_NAME)
            .criteria(
                ApproximateVectorQueryCriteriaBuilder.builder()
                    .limit(5)
                    .numCandidates(10)
                    .queryVector(Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE))
                    .path(FieldPath.newRoot("scalar.vector.path"))
                    .build())
            .build();
    private static final MaterializedVectorSearchQuery SCALAR_QUANTIZED_MATERIALIZED_QUERY =
        new MaterializedVectorSearchQuery(
            SCALAR_QUANTIZED_QUERY_DEFINITION,
            SCALAR_QUANTIZED_QUERY_DEFINITION.criteria().queryVector().get());

    private LuceneVectorIndexReader reader;
    private IndexWriter writer;
    private LuceneVectorQueryFactoryDistributor queryFactory;
    private LuceneSearcherFactory searcherFactory;
    private NamedExecutorService concurrentSearchExecutor;
    private IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater;

    /** Test setup. */
    @Before
    public void setUp() throws Exception {
      var folder = TestUtils.getTempFolder();
      var path = folder.getRoot().toPath();
      var indexPath = path.resolve("indexGeneration");
      var metadataPath = path.resolve("indexMapping/indexGeneration");
      var luceneConfig = LuceneConfigBuilder.builder().dataPath(path).build();
      var directoryFactory =
          new IndexDirectoryFactory(indexPath, metadataPath, luceneConfig, 1, Optional.empty());
      var metricsFactory = mockMetricsFactory();
      this.queryingMetricsUpdater =
          SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.VECTOR_SEARCH);

      SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
      this.concurrentSearchExecutor =
          spy(Executors.fixedSizeThreadScheduledExecutor("test", 1, meterRegistry));

      LuceneVectorIndex index =
          LuceneVectorIndex.createDiskBacked(
              indexPath,
              metadataPath,
              luceneConfig,
              FeatureFlags.getDefault(),
              new InstrumentedConcurrentMergeScheduler(meterRegistry),
              new TieredMergePolicy(),
              new QueryCacheProvider.DefaultQueryCacheProvider(),
              mock(NamedScheduledExecutorService.class),
              Optional.of(this.concurrentSearchExecutor),
              Optional.of(this.concurrentSearchExecutor),
              VectorIndex.MOCK_INDEX_DEFINITION_GENERATION_ALL_QUANTIZATION.getIndexDefinition(),
              MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
              new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
              metricsFactory);
      InitializedLuceneVectorIndex initializedIndex =
          InitializedLuceneVectorIndex.create(
              index,
              MOCK_INDEX_DEFINITION_GENERATION.getGenerationId(),
              directoryFactory,
              mock(IndexDirectoryHelper.class),
              Optional.empty(),
              FeatureFlags.getDefault());
      var context =
          new VectorQueryFactoryContext(
              VectorIndex.MOCK_INDEX_DEFINITION_GENERATION_ALL_QUANTIZATION.getIndexDefinition(),
              IndexFormatVersion.CURRENT,
              FeatureFlags.getDefault(),
              this.queryingMetricsUpdater);
      this.queryFactory = spy(LuceneVectorQueryFactoryDistributor.create(context));

      this.writer =
          ((SingleLuceneIndexWriter)
                  ((MeteredIndexWriter) initializedIndex.getWriter()).getWrapped())
              .getLuceneWriter();

      this.searcherFactory =
          spy(
              new LuceneSearcherFactory(
                  VectorIndex.MOCK_INDEX_DEFINITION_GENERATION_ALL_QUANTIZATION
                      .getIndexDefinition(),
                  false,
                  new QueryCacheProvider.DefaultQueryCacheProvider(),
                  Optional.empty(),
                  VectorIndex.mockQueryMetricsUpdater()));
      LuceneSearcherManager searcherManager =
          new LuceneSearcherManager(
              this.writer, this.searcherFactory, SearchIndex.mockMetricsFactory());

      var indexDefinition =
          VectorIndex.MOCK_INDEX_DEFINITION_GENERATION_ALL_QUANTIZATION.getIndexDefinition();
      this.reader =
          LuceneVectorIndexReader.create(
              searcherManager,
              new VectorQueryFactoryContext(
                  indexDefinition,
                  IndexFormatVersion.CURRENT,
                  FeatureFlags.getDefault(),
                  this.queryingMetricsUpdater),
              indexDefinition,
              this.queryFactory,
              Optional.of(this.concurrentSearchExecutor),
              Optional.of(this.concurrentSearchExecutor));
    }

    @Test
    public void requiredMemory_hugeSegment_doesNotOverflow() throws Exception {
      this.writer.deleteAll();
      int numVectors = 2_000_000_000;
      int dimension = 8192;
      String floats = "$type:knnVector/floats";
      String bytes = "$type:knnByte/bytes";
      String bits = "$type:knnBit/bits";

      // Ensure field info is populated
      this.writer.addDocument(
          List.of(
              new KnnFloatVectorField(
                  floats, new float[] {1, 2, 3}, VectorSimilarityFunction.COSINE),
              new KnnByteVectorField(bytes, new byte[] {1, 2, 3}, VectorSimilarityFunction.COSINE),
              new KnnByteVectorField(bits, new byte[] {1, 2, 3}, VectorSimilarityFunction.COSINE)));
      this.writer.commit();

      DirectoryReader reader = DirectoryReader.open(this.writer);
      LeafReader leafReader =
          createLeafWithFakeVectors(reader.leaves().getFirst().reader(), numVectors, dimension);

      FieldInfo floatFieldInfo = leafReader.getFieldInfos().fieldInfo(floats);
      FieldInfo byteFieldInfo = leafReader.getFieldInfos().fieldInfo(bytes);
      FieldInfo bitFieldInfo = leafReader.getFieldInfos().fieldInfo(bits);

      assertNotNull(floatFieldInfo);
      assertNotNull(byteFieldInfo);
      assertNotNull(bitFieldInfo);
      assertEquals(
          24_000_000_000L,
          LuceneVectorIndexReader.computeRequiredHeapBytes(leafReader, floatFieldInfo, 4));
      assertEquals(
          6_000_000_000L,
          LuceneVectorIndexReader.computeRequiredHeapBytes(leafReader, byteFieldInfo, 1));
      assertEquals(
          750_000_000L,
          LuceneVectorIndexReader.computeRequiredHeapBytes(leafReader, bitFieldInfo, 1 / 8.0));
    }

    @Test
    public void testCanQueryMultipleTimesSimultaneously() throws Exception {
      // Use an invocation of LuceneVectorQueryFactory::createQuery as a signal that we're within
      // the
      // critical section in query().
      ConcurrencyTestUtils.assertCanBeInvokedConcurrently(
          this.queryFactory,
          factory -> factory.createQuery(any(), any()),
          () -> this.reader.query(UNQUANTIZED_MATERIALIZED_QUERY));
    }

    @Test
    public void testCanRunInvalidQueryMultipleTimesSimultaneously() throws Exception {
      var q = getInvalidQuery();
      ConcurrencyTestUtils.assertCanBeInvokedConcurrently(
          this.queryFactory,
          f -> f.createQuery(any(), any()),
          () -> {
            try {
              this.reader.query(q);
            } catch (Exception e) {
              // ignored - don't cause assertCanBeInvokedConcurrently to fail
            }
          });
    }

    @Test
    public void testWhenQueryIsCompleteReaderReferencesAreReleased() throws Exception {
      // this acquires a references to a lucene reader under the hood
      var reader = getInternalReader();
      // one reference for management
      Assert.assertEquals(1, reader.getRefCount());
      query();
      // the post-query ref count the same
      Assert.assertEquals(1, reader.getRefCount());
    }

    @Test
    public void testWhenInvalidQueryIsCompleteReaderReferencesAreReleased() throws Exception {
      // this acquires a references to a lucene reader under the hood
      var reader = getInternalReader();
      // one reference for management
      Assert.assertEquals(1, reader.getRefCount());
      Assert.assertThrows(InvalidQueryException.class, () -> this.reader.query(getInvalidQuery()));
      // the post-query ref count the same
      Assert.assertEquals(1, reader.getRefCount());
    }

    @Test
    public void testCannotCloseWhileQuery() throws Exception {
      // Use an invocation of LuceneQueryFactory::createQuery as a signal that we're within the
      // critical section in query().
      ConcurrencyTestUtils.assertCannotBeInvokedConcurrently(
          this.queryFactory,
          factory -> factory.createQuery(any(), any()),
          () -> this.reader.query(UNQUANTIZED_MATERIALIZED_QUERY),
          this.reader::close);
    }

    @Test
    public void testClosedIndexReaderQueryThrowsException() {
      this.reader.close();
      Assert.assertThrows(ReaderClosedException.class, this::query);
    }

    @Test
    public void testVectorIndexReader() throws Exception {
      var unquantizedMetric = this.queryingMetricsUpdater.getNumCandidatesUnquantized();
      var scalarQuantizedMetric = this.queryingMetricsUpdater.getNumCandidatesScalarQuantized();
      var binaryQuantizedMetric = this.queryingMetricsUpdater.getNumCandidatesBinaryQuantized();
      var limitMetric = this.queryingMetricsUpdater.getLimitPerQuery();

      Assert.assertEquals(0, unquantizedMetric.count());
      Assert.assertEquals(0, scalarQuantizedMetric.count());
      Assert.assertEquals(0, binaryQuantizedMetric.count());
      Assert.assertEquals(0, limitMetric.count());

      this.reader.query(UNQUANTIZED_MATERIALIZED_QUERY);

      Assert.assertEquals(
          IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH.tagValue,
          this.queryingMetricsUpdater
              .getVectorCommandCounter()
              .getId()
              .getTag(IndexTypeData.INDEX_TYPE_TAG_NAME));
      Assert.assertEquals(
          IndexTypeData.IndexTypeTag.TAG_VECTOR_SEARCH.tagValue,
          this.queryingMetricsUpdater
              .getVectorResultLatencyTimer()
              .getId()
              .getTag(IndexTypeData.INDEX_TYPE_TAG_NAME));
      Assert.assertEquals(1, unquantizedMetric.count());
      Assert.assertEquals(10, unquantizedMetric.mean(), 0.01);

      Assert.assertEquals(1, limitMetric.count());
      Assert.assertEquals(5, limitMetric.mean(), 0.01);

      Assert.assertEquals(0, scalarQuantizedMetric.count());
      Assert.assertEquals(0, binaryQuantizedMetric.count());

      this.reader.query(SCALAR_QUANTIZED_MATERIALIZED_QUERY);
      Assert.assertEquals(1, unquantizedMetric.count());
      Assert.assertEquals(10, unquantizedMetric.mean(), 0.01);

      Assert.assertEquals(2, limitMetric.count());
      Assert.assertEquals(5, limitMetric.mean(), 0.01);

      Assert.assertEquals(1, scalarQuantizedMetric.count());
      Assert.assertEquals(10, scalarQuantizedMetric.mean(), 0.01);

      Assert.assertEquals(0, binaryQuantizedMetric.count());
    }

    private BsonArray query() throws IOException, InvalidQueryException, ReaderClosedException {
      return this.reader.query(UNQUANTIZED_MATERIALIZED_QUERY);
    }

    private IndexReader getInternalReader() throws IOException {
      ArgumentCaptor<IndexReader> getReader = ArgumentCaptor.forClass(IndexReader.class);
      verify(this.searcherFactory, times(1)).newSearcher(getReader.capture(), any());
      return getReader.getValue();
    }

    private static MaterializedVectorSearchQuery getInvalidQuery() {
      var vectorQuery =
          VectorQueryBuilder.builder()
              .index(VectorIndex.MOCK_INDEX_NAME)
              .criteria(
                  ApproximateVectorQueryCriteriaBuilder.builder()
                      .limit(1)
                      .numCandidates(2)
                      .queryVector(Vector.fromFloats(new float[0], NATIVE))
                      .path(FieldPath.newRoot("notvector"))
                      .build())
              .build();
      return new MaterializedVectorSearchQuery(
          vectorQuery, vectorQuery.criteria().queryVector().get());
    }
  }

  public static class TestRequiredMemoryMetric {

    private IndexWriter writer;
    private VectorIndexReader indexReader;
    private Path indexPath;

    private String floatFieldName;
    private String byteFieldName;
    private String bitFieldName;
    private String scalarFieldName;
    private String binaryFieldName;

    private InitializedLuceneVectorIndex initializedIndex;

    /** Set up resources for test. */
    @Before
    public void setUp() throws Exception {
      this.scalarFieldName =
          TypeField.KNN_F32_Q7.getLuceneFieldName(
              FieldPath.newRoot("vector.path"), Optional.empty());
      this.binaryFieldName =
          TypeField.KNN_F32_Q1.getLuceneFieldName(
              FieldPath.newRoot("vector.path"), Optional.empty());
      this.floatFieldName =
          FieldName.TypeField.KNN_VECTOR.getLuceneFieldName(
              FieldPath.newRoot("vector.path"), Optional.empty());
      this.byteFieldName =
          FieldName.TypeField.KNN_BYTE.getLuceneFieldName(
              FieldPath.newRoot("vector.path"), Optional.empty());
      this.bitFieldName =
          FieldName.TypeField.KNN_BIT.getLuceneFieldName(
              FieldPath.newRoot("vector.path"), Optional.empty());

      generateReaderWriter(VectorIndex.MOCK_INDEX_DEFINITION_GENERATION);
    }

    @After
    public void tearDown() throws Exception {
      this.writer.close();
      this.indexReader.close();
    }

    @Test
    public void requiredMemory_emptyIndex_match0L() throws Exception {
      this.writer.deleteAll();
      assertEquals(0L, this.indexReader.getRequiredMemoryForVectorData());
    }

    @Test
    public void requiredMemory_noVectorData_match0L() throws Exception {
      this.writer.addDocument(new Document());
      this.writer.commit();
      this.indexReader.refresh();
      long result = this.indexReader.getRequiredMemoryForVectorData();
      assertEquals(0, result);
    }

    @Test
    public void requiredMemory_sparseVectors_skipsEmptyDocs() throws Exception {
      this.writer.addDocument(new Document());
      indexDocumentsAndCommit(
          List.of(new float[] {1, 2, 3}),
          vector ->
              new KnnFloatVectorField(
                  this.floatFieldName, vector, VectorSimilarityFunction.COSINE));
      this.indexReader.refresh();
      long result = this.indexReader.getRequiredMemoryForVectorData();
      assertEquals(12, result);
    }

    @Test
    public void requiredMemory_sparseSegment_skipsEmptySegments() throws Exception {
      this.writer.addDocument(new Document());
      this.writer.commit();
      indexDocumentsAndCommit(
          List.of(new float[] {1, 2, 3}),
          vector ->
              new KnnFloatVectorField(
                  this.floatFieldName, vector, VectorSimilarityFunction.COSINE));
      this.indexReader.refresh();

      long result = this.indexReader.getRequiredMemoryForVectorData();

      assertEquals(12, result);
    }

    @Test
    public void requiredMemory_invalidFieldType_match0L() throws Exception {
      indexDocumentsAndCommit(
          List.of(
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3}),
          (vector) -> new KeywordField(this.floatFieldName, "invalid", KeywordField.Store.NO));
      this.indexReader.refresh();
      long result = this.indexReader.getRequiredMemoryForVectorData();
      assertEquals(0, result);
    }

    @Test
    public void requiredMemoryMetric_testVectors_floatGreaterThanByte() throws Exception {
      generateReaderWriter(VectorIndex.MOCK_INDEX_DEFINITION_GENERATION);

      indexDocumentsAndCommit(
          List.of(
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3}),
          (vector) ->
              new KnnFloatVectorField(
                  this.floatFieldName, vector, VectorSimilarityFunction.COSINE));

      this.indexReader.refresh();
      long floatFileSize = this.indexReader.getRequiredMemoryForVectorData();

      this.writer.deleteAll();

      indexDocumentsAndCommit(
          List.of(
              new byte[] {(byte) 0x11, (byte) 0xC3, (byte) 0xF0},
              new byte[] {(byte) 0x11, (byte) 0xC3, (byte) 0xF0},
              new byte[] {(byte) 0x11, (byte) 0xC3, (byte) 0xF0},
              new byte[] {(byte) 0x11, (byte) 0xC3, (byte) 0xF0}),
          (vector) ->
              new KnnByteVectorField(this.byteFieldName, vector, VectorSimilarityFunction.COSINE));

      this.indexReader.refresh();
      long byteFileSize = this.indexReader.getRequiredMemoryForVectorData();

      Assert.assertEquals(48, floatFileSize);
      Assert.assertEquals(12, byteFileSize);
    }

    @Test
    public void requiredMemoryMetric_testVectors_floatGreaterThanBit() throws Exception {
      generateReaderWriter(VectorIndex.MOCK_INDEX_DEFINITION_GENERATION);

      indexDocumentsAndCommit(
          List.of(
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3}),
          (vector) ->
              new KnnFloatVectorField(
                  this.floatFieldName, vector, VectorSimilarityFunction.COSINE));

      this.indexReader.refresh();
      long floatFileSize = this.indexReader.getRequiredMemoryForVectorData();

      this.writer.deleteAll();

      indexDocumentsAndCommit(
          List.of(
              Vector.fromBits(new byte[] {(byte) 0x11, (byte) 0xC3, (byte) 0xF0}),
              Vector.fromBits(new byte[] {(byte) 0x11, (byte) 0xC3, (byte) 0xF0}),
              Vector.fromBits(new byte[] {(byte) 0x11, (byte) 0xC3, (byte) 0xF0}),
              Vector.fromBits(new byte[] {(byte) 0x11, (byte) 0xC3, (byte) 0xF0})),
          (vector) ->
              new KnnByteVectorField(
                  this.bitFieldName,
                  vector.asBitVector().getBitVector(),
                  VectorSimilarityFunction.COSINE));

      this.indexReader.refresh();
      long bitFileSize = this.indexReader.getRequiredMemoryForVectorData();

      Assert.assertEquals(48, floatFileSize);
      Assert.assertEquals(2, bitFileSize);
    }

    @Test
    public void requiredMemoryMetric_testQuantizedVectors_binaryLessThanScalar() throws Exception {
      generateReaderWriter(VectorIndex.MOCK_INDEX_DEFINITION_GENERATION_SCALAR);
      indexDocumentsAndCommit(
          List.of(
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3}),
          (vector) ->
              new KnnFloatVectorField(
                  this.scalarFieldName, vector, VectorSimilarityFunction.COSINE));

      this.indexReader.refresh();
      long scalarFileSize = this.indexReader.getRequiredMemoryForVectorData();

      generateReaderWriter(VectorIndex.MOCK_INDEX_DEFINITION_GENERATION_BINARY);

      indexDocumentsAndCommit(
          List.of(
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3}),
          (vector) ->
              new KnnFloatVectorField(
                  this.binaryFieldName, vector, VectorSimilarityFunction.COSINE));

      this.indexReader.refresh();
      long binaryFileSize = this.indexReader.getRequiredMemoryForVectorData();

      Assert.assertEquals(12, scalarFileSize);
      Assert.assertEquals(2, binaryFileSize);
    }

    @Test
    public void requiredMemoryMetric_testVectors_quantizedLessThanNonquantized() throws Exception {
      generateReaderWriter(VectorIndex.MOCK_INDEX_DEFINITION_GENERATION);
      indexDocumentsAndCommit(
          List.of(
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3}),
          (vector) ->
              new KnnFloatVectorField(
                  this.floatFieldName, vector, VectorSimilarityFunction.COSINE));

      this.indexReader.refresh();
      long floatFileSize = this.indexReader.getRequiredMemoryForVectorData();

      generateReaderWriter(VectorIndex.MOCK_INDEX_DEFINITION_GENERATION_BINARY);

      indexDocumentsAndCommit(
          List.of(
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3}),
          (vector) ->
              new KnnFloatVectorField(
                  this.binaryFieldName, vector, VectorSimilarityFunction.COSINE));

      this.indexReader.refresh();
      long binaryFileSize = this.indexReader.getRequiredMemoryForVectorData();

      Assert.assertEquals(2, binaryFileSize);
      Assert.assertEquals(48, floatFileSize);
    }

    @Test
    public void requiredMemoryMetric_testVectors_assertCfsGenerated() throws Exception {
      generateReaderWriter(VectorIndex.MOCK_INDEX_DEFINITION_GENERATION);
      indexDocumentsAndCommit(
          List.of(
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3}),
          (vector) ->
              new KnnFloatVectorField(
                  this.floatFieldName, vector, VectorSimilarityFunction.COSINE));

      this.indexReader.refresh();
      Assert.assertTrue(hasCfsFile(this.indexPath));
      generateReaderWriter(VectorIndex.MOCK_INDEX_DEFINITION_GENERATION_BINARY);

      indexDocumentsAndCommit(
          List.of(
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3}),
          (vector) ->
              new KnnFloatVectorField(
                  this.binaryFieldName, vector, VectorSimilarityFunction.COSINE));

      this.indexReader.refresh();
      Assert.assertTrue(hasCfsFile(this.indexPath));
    }

    @Test
    public void requiredMemoryMetric_testVectors_multipleSegments() throws Exception {
      generateReaderWriter(VectorIndex.MOCK_INDEX_DEFINITION_GENERATION);
      indexDocumentsAndCommit(
          List.of(
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3}),
          (vector) ->
              new KnnFloatVectorField(
                  this.floatFieldName, vector, VectorSimilarityFunction.COSINE));
      this.indexReader.refresh();
      var floatFileSize = this.indexReader.getRequiredMemoryForVectorData();
      indexDocumentsAndCommit(
          List.of(
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3},
              new float[] {1, 2, 3}),
          (vector) ->
              new KnnFloatVectorField(
                  this.floatFieldName, vector, VectorSimilarityFunction.COSINE));

      this.indexReader.refresh();
      var doubleFloatFileSize = this.indexReader.getRequiredMemoryForVectorData();
      Assert.assertEquals(48, floatFileSize);
      Assert.assertEquals(96, doubleFloatFileSize);
    }

    private <T> void indexDocumentsAndCommit(
        List<T> vectors, Function<T, IndexableField> vectorFieldProvider) throws IOException {
      for (T vector : vectors) {
        var document = new Document();
        document.add(vectorFieldProvider.apply(vector));
        this.writer.addDocument(document);
      }
      this.writer.commit();
    }

    private IndexDirectoryFactory createDirectoryFactory(Path path) {
      var luceneConfig = LuceneConfigBuilder.builder().dataPath(path).build();
      var indexPath = path.resolve("indexGeneration");
      var metadataPath = path.resolve("indexMapping/indexGeneration");
      return new IndexDirectoryFactory(indexPath, metadataPath, luceneConfig, 1, Optional.empty());
    }

    private void generateReaderWriter(VectorIndexDefinitionGeneration indexDefinitionGeneration)
        throws IOException {
      var temporaryFolder = TestUtils.getTempFolder();
      var path = temporaryFolder.getRoot().toPath();
      var directoryFactory = createDirectoryFactory(path);
      this.indexPath = directoryFactory.getPath();
      SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
      LuceneVectorIndex index =
          LuceneVectorIndex.createDiskBacked(
              this.indexPath,
              directoryFactory.getMetadataPath(),
              directoryFactory.getConfig(),
              FeatureFlags.getDefault(),
              new InstrumentedConcurrentMergeScheduler(meterRegistry),
              new TieredMergePolicy(),
              new QueryCacheProvider.DefaultQueryCacheProvider(),
              mock(NamedScheduledExecutorService.class),
              Optional.empty(),
              Optional.empty(),
              indexDefinitionGeneration.getIndexDefinition(),
              indexDefinitionGeneration.generation().indexFormatVersion,
              new AtomicDirectoryRemover(path),
              VectorIndex.mockMetricsFactory());
      this.initializedIndex =
          InitializedLuceneVectorIndex.create(
              index,
              indexDefinitionGeneration.getGenerationId(),
              directoryFactory,
              mock(IndexDirectoryHelper.class),
              Optional.empty(),
              FeatureFlags.getDefault());
      this.writer =
          ((SingleLuceneIndexWriter)
                  ((MeteredIndexWriter) this.initializedIndex.getWriter()).getWrapped())
              .getLuceneWriter();
      this.indexReader = this.initializedIndex.getReader();
    }

    private boolean hasCfsFile(Path path) {
      String[] files = path.toFile().list();
      for (String file : files) {
        if (file.endsWith(".cfs")) {
          return true;
        }
      }
      return false;
    }
  }
}
