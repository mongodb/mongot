package com.xgen.mongot.index.lucene;

import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.google.common.util.concurrent.testing.TestingExecutors;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import com.xgen.mongot.util.AtomicDirectoryRemover;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.testing.ConcurrencyTestUtils;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.lucene.LuceneConfigBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class LuceneSearchIndexTest {

  @Test
  public void testUninitializedIndex() throws Exception {
    var temporaryFolder = TestUtils.getTempFolder();
    var indexPath = temporaryFolder.getRoot().toPath().resolve("indexData");
    var metadataPath = temporaryFolder.getRoot().toPath().resolve("indexMapping");
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    LuceneSearchIndex index =
        LuceneSearchIndex.createDiskBacked(
            indexPath,
            metadataPath,
            LuceneConfigBuilder.builder().dataPath(indexPath).build(),
            FeatureFlags.getDefault(),
            new InstrumentedConcurrentMergeScheduler(meterRegistry),
            new TieredMergePolicy(),
            new QueryCacheProvider.DefaultQueryCacheProvider(),
            Executors.namedExecutor(
                "refresh", TestingExecutors.noOpScheduledExecutor(), meterRegistry),
            Optional.of(
                Executors.namedExecutor(
                    "refresh", TestingExecutors.noOpScheduledExecutor(), meterRegistry)),
            Optional.of(
                Executors.namedExecutor(
                    "rescoring", TestingExecutors.noOpScheduledExecutor(), meterRegistry)),
            MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
            MOCK_INDEX_DEFINITION_GENERATION.generation().formatVersion.asLucene(),
            AnalyzerRegistryBuilder.empty(),
            new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
            SearchIndex.mockMetricsFactory(),
            Runnable::run);
    // Ensure the directory does not exist.
    Assert.assertFalse(indexPath.toFile().exists());
    Assert.assertFalse(metadataPath.toFile().exists());
    // Drop the Index and ensure the directory is non-existent.
    index.close();
    index.drop();
    Assert.assertFalse(indexPath.toFile().exists());
    Assert.assertFalse(metadataPath.toFile().exists());
  }

  @Test
  public void testDrop() throws Exception {
    // Create a temporary directory we can make an index in.
    var temporaryFolder = TestUtils.getTempFolder();
    var indexPath = temporaryFolder.getRoot().toPath();
    var metadataPath = temporaryFolder.getRoot().toPath().resolve("indexMapping");

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    LuceneSearchIndex index =
        LuceneSearchIndex.createDiskBacked(
            indexPath,
            metadataPath,
            LuceneConfigBuilder.builder().dataPath(indexPath).build(),
            FeatureFlags.getDefault(),
            new InstrumentedConcurrentMergeScheduler(meterRegistry),
            new TieredMergePolicy(),
            new QueryCacheProvider.DefaultQueryCacheProvider(),
            Mockito.mock(NamedScheduledExecutorService.class),
            Optional.of(Mockito.mock(NamedExecutorService.class)),
            Optional.of(Mockito.mock(NamedExecutorService.class)),
            MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
            MOCK_INDEX_DEFINITION_GENERATION.generation().formatVersion.asLucene(),
            AnalyzerRegistryBuilder.empty(),
            new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
            SearchIndex.mockMetricsFactory(),
            Runnable::run);

    // Ensure the directory exists after creating the Index.
    Assert.assertTrue(indexPath.toFile().exists());

    // Drop the Index and ensure the directory was deleted.
    index.close();
    index.drop();
    Assert.assertFalse(indexPath.toFile().exists());

    index.drop(); // shouldn't throw if already dropped
  }

  @Test
  public void testIndexBackingStrategy() throws IOException {
    var temporaryFolder = TestUtils.getTempFolder();
    var folderPath = temporaryFolder.getRoot().toPath();

    var backingStrategy =
        IndexBackingStrategyFactory.diskBacked(
            Mockito.mock(NamedScheduledExecutorService.class),
            LuceneConfigBuilder.builder().dataPath(folderPath).build().refreshInterval(),
            new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
            folderPath,
            folderPath.resolve("indexMapping"),
            SearchIndex.mockMetricsFactory(),
            1);

    backingStrategy.releaseResources();
    Assert.assertEquals(
        "should handle calling getIndexSize() after releaseResources() gracefully",
        0,
        backingStrategy.getDiskStats().totalFileByteSize());
  }

  @Test
  public void testIndexBackingStrategyConcurrently() throws Exception {
    var temporaryFolder = TestUtils.getTempFolder();
    var folderPath = temporaryFolder.getRoot().toPath();

    CountDownLatch firstReadyToStart = new CountDownLatch(1);
    CountDownLatch firstShouldStart = new CountDownLatch(1);

    var directoryRemover =
        spy(new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()));
    Answer<Void> answer =
        invocation -> {
          firstReadyToStart.countDown();
          firstShouldStart.await();
          invocation.callRealMethod();
          return null;
        };
    doAnswer(answer).when(directoryRemover).deleteDirectory(any());

    var backingStrategy =
        IndexBackingStrategyFactory.diskBacked(
            Mockito.mock(NamedScheduledExecutorService.class),
            LuceneConfigBuilder.builder().dataPath(folderPath).build().refreshInterval(),
            directoryRemover,
            folderPath,
            folderPath.resolve("indexMapping"),
            SearchIndex.mockMetricsFactory(),
            1);

    ConcurrencyTestUtils.assertCannotBeInvokedConcurrently(
        backingStrategy::releaseResources,
        firstReadyToStart,
        firstShouldStart,
        backingStrategy::getDiskStats);
  }

  @Test
  public void testGetDefinitions() throws Exception {
    // Create a temporary directory we can make an index in.
    var temporaryFolder = TestUtils.getTempFolder();
    var indexPath = temporaryFolder.getRoot().toPath().resolve("indexData");
    var metadataPath = temporaryFolder.getRoot().toPath().resolve("indexMapping");

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    LuceneSearchIndex index =
        LuceneSearchIndex.createDiskBacked(
            indexPath,
            metadataPath,
            LuceneConfigBuilder.builder().dataPath(indexPath).build(),
            FeatureFlags.getDefault(),
            new InstrumentedConcurrentMergeScheduler(meterRegistry),
            new TieredMergePolicy(),
            new QueryCacheProvider.DefaultQueryCacheProvider(),
            Mockito.mock(NamedScheduledExecutorService.class),
            Optional.of(Mockito.mock(NamedExecutorService.class)),
            Optional.of(Mockito.mock(NamedExecutorService.class)),
            MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(),
            MOCK_INDEX_DEFINITION_GENERATION.generation().formatVersion.asLucene(),
            AnalyzerRegistryBuilder.empty(),
            new AtomicDirectoryRemover(TestUtils.getTempFolder().getRoot().toPath()),
            SearchIndex.mockMetricsFactory(),
            Runnable::run);

    // it is not necessary for these definitions to be the same object, but better test this way
    // since we do not define equality for these objects.
    Assert.assertSame(MOCK_INDEX_DEFINITION_GENERATION.getIndexDefinition(), index.getDefinition());
    index.close();
    index.drop();
  }

  /**
   * Ensures that the Term from LuceneIndex.documentIdTerm can be used to update an existing
   * document.
   */
  @Test
  public void testDocumentIdQueryTermUpdate() throws Exception {
    try (Directory directory = new ByteBuffersDirectory()) {
      // First add two documents with different $meta._id fields to the index.
      BsonValue bsonDoc1Id = new BsonInt32(1);
      Document doc1 = new Document();
      byte[] id1 = LuceneDocumentIdEncoder.encodeDocumentId(bsonDoc1Id);
      doc1.add(LuceneDocumentIdEncoder.documentIdField(id1));
      doc1.add(new StringField("hello", "world", Field.Store.YES));

      BsonDocument bsonDoc2 = new BsonDocument().append("_id", new BsonInt32(2));
      Document doc2 = new Document();
      byte[] id2 = LuceneDocumentIdEncoder.encodeDocumentId(bsonDoc2);
      doc2.add(LuceneDocumentIdEncoder.documentIdField(id2));

      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        writer.addDocument(doc1);
        writer.addDocument(doc2);
        writer.commit();

        // Then try to search for the first document.
        @Var IndexReader reader = DirectoryReader.open(directory);
        @Var IndexSearcher searcher = new IndexSearcher(reader);

        // Ensure that we find two documents in the index.
        @Var TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);
        Assert.assertEquals(
            "expected to find two documents in the index", 2L, topDocs.totalHits.value());

        // Ensure we can find the first document.
        Query query = new TermQuery(LuceneDocumentIdEncoder.documentIdTerm(id1));
        topDocs = searcher.search(query, 10);
        Assert.assertEquals(
            "expected to only find one document with the matching _id",
            1L,
            topDocs.totalHits.value());

        // Ensure that the document we found was the intended document.
        @Var Document returned = searcher.storedFields().document(topDocs.scoreDocs[0].doc);
        @Var BsonValue documentId = LuceneDocumentIdEncoder.documentIdFromLuceneDocument(returned);
        Assert.assertTrue("document had invalid document _id type", documentId.isInt32());
        Assert.assertEquals("document had invalid document _id", bsonDoc1Id, documentId.asInt32());
        Assert.assertEquals("world", returned.get("hello"));

        // Then update the first document.
        Document doc1v2 = new Document();
        doc1v2.add(LuceneDocumentIdEncoder.documentIdField(id1));
        doc1v2.add(new StringField("hello", "moon", Field.Store.YES));

        writer.updateDocument(LuceneDocumentIdEncoder.documentIdTerm(id1), doc1v2);
        writer.commit();

        // Re-open the reader and searcher to see the committed results.
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);

        // Ensure that we still find only two documents in the index.
        topDocs = searcher.search(new MatchAllDocsQuery(), 10);
        Assert.assertEquals(
            "expected to find two documents in the index", 2L, topDocs.totalHits.value());

        // Ensure we can still find the first document.
        topDocs = searcher.search(query, 10);
        Assert.assertEquals(
            "expected to only find one document with the matching _id",
            1L,
            topDocs.totalHits.value());

        // Ensure that the document we found was the updated intended document.
        returned = searcher.storedFields().document(topDocs.scoreDocs[0].doc);
        documentId = LuceneDocumentIdEncoder.documentIdFromLuceneDocument(returned);
        Assert.assertTrue("document had invalid document _id type", documentId.isInt32());
        Assert.assertEquals("document had invalid document _id", bsonDoc1Id, documentId.asInt32());
        Assert.assertEquals("moon", returned.get("hello"));
      }
    }
  }
}
