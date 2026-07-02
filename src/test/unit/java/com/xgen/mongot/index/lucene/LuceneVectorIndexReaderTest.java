package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockMetricsFactory;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_INDEX_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexTypeData;
import com.xgen.mongot.index.MeteredIndexWriter;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryFactory;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryHelper;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.query.LuceneVectorQueryFactoryDistributor;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import com.xgen.mongot.index.lucene.writer.SingleLuceneIndexWriter;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.QueryExecutionContext;
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
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

@RunWith(Enclosed.class)
public class LuceneVectorIndexReaderTest {
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
              FeatureFlags.getDefault(),
              DynamicFeatureFlagRegistry.empty(),
              () -> false);
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
          LuceneSearcherManager.create(
              this.writer, this.searcherFactory, mockMetricsFactory(), () -> false);

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
    public void testGetNumEmbeddedRootDocuments_countsRootsAndDecrementsOnDelete()
        throws Exception {
      // Use NoMergePolicy so a deleted root doc is not physically merged away; the accurate
      // term-count path must still exclude it (the collectionStatistics fallback would not).
      this.writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

      // Two parent (root) docs marked EMBEDDED_ROOT, plus three child (vector) docs without it.
      byte[] rootId1 = addRootDoc(1);
      addRootDoc(2);
      for (int i = 0; i < 3; i++) {
        addChildDoc(100 + i);
      }
      this.writer.commit();
      this.reader.refresh();

      // Only the two root docs are counted; the child (vector) docs are excluded.
      Assert.assertEquals(2, this.reader.getNumEmbeddedRootDocuments());

      // Deleting a parent decrements the count immediately, even without a merge — the delete-aware
      // behavior the vector count relies on (always-accurate path, no feature flag).
      this.writer.deleteDocuments(LuceneDocumentIdEncoder.documentIdTerm(rootId1));
      this.writer.commit();
      this.reader.refresh();
      Assert.assertEquals(1, this.reader.getNumEmbeddedRootDocuments());
    }

    private byte[] addRootDoc(int id) throws IOException {
      BsonDocument bson = new BsonDocument().append("_id", new BsonInt32(id));
      byte[] docId = LuceneDocumentIdEncoder.encodeDocumentId(bson);
      Document doc = new Document();
      doc.add(LuceneDocumentIdEncoder.documentIdField(docId));
      doc.add(
          new StringField(
              FieldName.MetaField.EMBEDDED_ROOT.getLuceneFieldName(),
              FieldValue.EMBEDDED_ROOT_FIELD_VALUE,
              Field.Store.NO));
      this.writer.addDocument(doc);
      return docId;
    }

    private void addChildDoc(int id) throws IOException {
      BsonDocument bson = new BsonDocument().append("_id", new BsonInt32(id));
      byte[] docId = LuceneDocumentIdEncoder.encodeDocumentId(bson);
      Document doc = new Document();
      doc.add(LuceneDocumentIdEncoder.documentIdField(docId));
      doc.add(new StringField("name", "child", Field.Store.NO));
      this.writer.addDocument(doc);
    }

    @Test
    public void testCanQueryMultipleTimesSimultaneously() throws Exception {
      // Use an invocation of LuceneVectorQueryFactory::createQuery as a signal that we're within
      // the
      // critical section in query().
      ConcurrencyTestUtils.assertCanBeInvokedConcurrently(
          this.queryFactory,
          factory -> factory.createQuery(any(), any()),
          () -> this.reader.query(UNQUANTIZED_MATERIALIZED_QUERY, QueryExecutionContext.empty()));
    }

    @Test
    public void testCanRunInvalidQueryMultipleTimesSimultaneously() throws Exception {
      var q = getInvalidQuery();
      ConcurrencyTestUtils.assertCanBeInvokedConcurrently(
          this.queryFactory,
          f -> f.createQuery(any(), any()),
          () -> {
            try {
              this.reader.query(q, QueryExecutionContext.empty());
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
      Assert.assertThrows(
          InvalidQueryException.class,
          () -> this.reader.query(getInvalidQuery(), QueryExecutionContext.empty()));
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
          () -> this.reader.query(UNQUANTIZED_MATERIALIZED_QUERY, QueryExecutionContext.empty()),
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

      this.reader.query(UNQUANTIZED_MATERIALIZED_QUERY, QueryExecutionContext.empty());

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

      this.reader.query(SCALAR_QUANTIZED_MATERIALIZED_QUERY, QueryExecutionContext.empty());
      Assert.assertEquals(1, unquantizedMetric.count());
      Assert.assertEquals(10, unquantizedMetric.mean(), 0.01);

      Assert.assertEquals(2, limitMetric.count());
      Assert.assertEquals(5, limitMetric.mean(), 0.01);

      Assert.assertEquals(1, scalarQuantizedMetric.count());
      Assert.assertEquals(10, scalarQuantizedMetric.mean(), 0.01);

      Assert.assertEquals(0, binaryQuantizedMetric.count());
    }

    private BsonArray query() throws IOException, InvalidQueryException, ReaderClosedException {
      return this.reader.query(UNQUANTIZED_MATERIALIZED_QUERY, QueryExecutionContext.empty());
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
}
