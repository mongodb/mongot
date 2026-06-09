package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.lucene.InstrumentedConcurrentMergeSchedulerTest.createMergeScheduler;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.RateLimiter;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.writer.SingleLuceneIndexWriter;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.LoggableIdUtils;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Field;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

public class LuceneIndexingFailuresTest {
  private static class TestException extends RuntimeException {}

  @BeforeClass
  public static void setUpClass() {
    // Enable the loggable document ID feature for tests
    LoggableIdUtils.initialize(true);
  }

  /**
   * Reset the static rate limiter before each test to ensure tests don't interfere with each other.
   */
  @Before
  public void resetRateLimiter() throws Exception {
    Field rateLimiterField =
        SingleLuceneIndexWriter.class.getDeclaredField("INDEXING_FAILURE_LOG_RATE_LIMITER");
    rateLimiterField.setAccessible(true);
    RateLimiter limiter = (RateLimiter) rateLimiterField.get(null);
    // Bump the rate high so tests are not rate limited.
    limiter.setRate(1000.0);
  }

  @Test
  public void indexingInternalFailuresMetrics_whenWriterOperationsFail_incrementsCounters()
      throws Exception {
    var indexingMetricsUpdater =
        SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType());

    // need a new directory to work around write lock conflicts from the first writer
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    var writer =
        SingleLuceneIndexWriter.createForSearchIndex(
            directory,
            createMergeScheduler(
                new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
                SearchIndex.MOCK_INDEX_GENERATION_ID,
                0,
                1,
                false, 
                directory),
            new TieredMergePolicy(),
            10.0,
            Optional.empty(),
            Optional.empty(),
            LuceneAnalyzer.indexAnalyzer(
                SearchIndex.MOCK_INDEX_DEFINITION, AnalyzerRegistryBuilder.empty()),
            MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            indexingMetricsUpdater,
            Optional.empty(),
            FeatureFlags.getDefault(),
            DynamicFeatureFlagRegistry.empty(),
            false);

    // Replace the lucene writer with the mock BEFORE creating the spy
    // This ensures the spy delegates to a writer that has the mocked luceneWriter
    Field luceneWriterField = SingleLuceneIndexWriter.class.getDeclaredField("luceneWriter");
    luceneWriterField.setAccessible(true);
    var realLuceneWriter = (org.apache.lucene.index.IndexWriter) luceneWriterField.get(writer);
    var spyLuceneWriter = spy(realLuceneWriter);
    luceneWriterField.set(writer, spyLuceneWriter);

    // Now create the spy of the writer
    var spyWriter = spy(writer);

    // Mock doLogIndexingFailure to avoid rate limiting
    doNothing()
        .when(spyWriter)
        .doLogIndexingFailure(any(Throwable.class), anyString(), anyString());

    // Mock exceptions are thrown from lucene writer
    doThrow(new TestException())
        .when(spyLuceneWriter).deleteDocuments(any(Term.class));

    doThrow(new TestException())
        .when(spyLuceneWriter).updateDocuments(any(org.apache.lucene.index.Term.class), any());

    // Very tricky:  since commit() is still ran even with the mock and it failed
    // the lock holding check internally, we need to synchronize to make it work.
    synchronized (realLuceneWriter) {
      doThrow(new TestException()).when(spyLuceneWriter).commit();
    }

    doThrow(new TestException()).when(spyLuceneWriter).close();

    doThrow(new TestException()).when(spyLuceneWriter).deleteAll();

    var metricsFactory = indexingMetricsUpdater.getMetricsFactory();
    var counterName = "IndexingInternalFailures";

    BsonDocument doc = new BsonDocument();
    doc.append(
        MOCK_INDEX_DEFINITION.getIndexId().toString(), new BsonDocument("_id", new BsonInt32(1)));
    var rawDoc = BsonUtils.documentToRaw(doc);

    // test insert
    assertThrows(TestException.class, () -> {
      spyWriter.updateIndex(
          DocumentEvent.createInsert(
              DocumentMetadata.fromMetadataNamespace(Optional.of(rawDoc), MOCK_INDEX_ID), rawDoc));
    });
    assertEquals(1,
        (long) metricsFactory
            .get(counterName)
            .tag("exceptionName", TestException.class.getSimpleName())
            .tag("operation", DocumentEvent.EventType.INSERT.name()).counter().count());

    // test delete
    assertThrows(TestException.class, () -> {
      spyWriter.updateIndex(
          DocumentEvent.createDelete(new BsonInt32(0)));
    });
    assertEquals(1,
        (long) metricsFactory
            .get(counterName)
            .tag("exceptionName", TestException.class.getSimpleName())
            .tag("operation", DocumentEvent.EventType.DELETE.name()).counter().count());

    // test commit
    assertThrows(TestException.class, () -> {
      spyWriter.commit(EncodedUserData.EMPTY);
    });
    assertEquals(1,
        (long) metricsFactory
            .get(counterName)
            .tag("exceptionName", TestException.class.getSimpleName())
            .tag("operation", "commit").counter().count());

    // test close
    assertThrows(TestException.class, () -> {
      spyWriter.close();
    });
    assertEquals(1,
        (long) metricsFactory
            .get(counterName)
            .tag("exceptionName", TestException.class.getSimpleName())
            .tag("operation", "close").counter().count());

    // test deleteAll
    assertThrows(TestException.class, () -> {
      spyWriter.deleteAll(EncodedUserData.EMPTY);
    });
    assertEquals(1,
        (long) metricsFactory
            .get(counterName)
            .tag("exceptionName", TestException.class.getSimpleName())
            .tag("operation", "deleteAll").counter().count());
  }

  @Test
  public void logIndexingFailureWithRateLimited_bsonInt32_logsUnloggable() throws Exception {
    var indexingMetricsUpdater =
        SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType());

    // need a new directory to work around write lock conflicts from the first writer
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    var writer =
        SingleLuceneIndexWriter.createForSearchIndex(
            directory,
            createMergeScheduler(
                new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
                SearchIndex.MOCK_INDEX_GENERATION_ID,
                0,
                1,
                false, 
                directory),
            new TieredMergePolicy(),
            10.0,
            Optional.empty(),
            Optional.empty(),
            LuceneAnalyzer.indexAnalyzer(
                SearchIndex.MOCK_INDEX_DEFINITION, AnalyzerRegistryBuilder.empty()),
            MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            indexingMetricsUpdater,
            Optional.empty(),
            FeatureFlags.getDefault(),
            DynamicFeatureFlagRegistry.empty(),
            false);

    // Replace the lucene writer with the mock BEFORE creating the spy
    Field luceneWriterField = SingleLuceneIndexWriter.class.getDeclaredField("luceneWriter");
    luceneWriterField.setAccessible(true);
    var realLuceneWriter = (org.apache.lucene.index.IndexWriter) luceneWriterField.get(writer);
    var spyLuceneWriter = spy(realLuceneWriter);
    luceneWriterField.set(writer, spyLuceneWriter);

    // Now create the spy of the writer
    var spyWriter = spy(writer);

    // Mock doLogIndexingFailure to capture the call
    doNothing()
        .when(spyWriter)
        .doLogIndexingFailure(any(Throwable.class), anyString(), anyString());

    // Mock exception thrown from lucene writer updateDocuments to trigger
    // logIndexingFailureWithRateLimited
    doThrow(new TestException())
        .when(spyLuceneWriter).updateDocuments(any(org.apache.lucene.index.Term.class), any());

    // Test with BsonInt32 _id
    BsonInt32 int32Id = new BsonInt32(42);
    BsonDocument doc1 = new BsonDocument();
    doc1.append(
        MOCK_INDEX_DEFINITION.getIndexId().toString(), new BsonDocument("_id", int32Id));
    var rawDoc1 = BsonUtils.documentToRaw(doc1);

    assertThrows(TestException.class, () -> {
      spyWriter.updateIndex(
          DocumentEvent.createInsert(
              DocumentMetadata.fromMetadataNamespace(
                  Optional.of(rawDoc1), MOCK_INDEX_ID),
              rawDoc1));
    });

    // Verify that doLogIndexingFailure was called with "unloggable" since BsonInt32 is not
    // ObjectId or UUID
    verify(spyWriter).doLogIndexingFailure(
        any(TestException.class),
        eq("INSERT"),
        eq(LoggableIdUtils.UNLOGGABLE_ID_TYPE));
  }

  @Test
  public void logIndexingFailureWithRateLimited_bsonObjectId_logsHexString() throws Exception {
    var indexingMetricsUpdater =
        SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType());

    // need a new directory to work around write lock conflicts from the first writer
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    var writer =
        SingleLuceneIndexWriter.createForSearchIndex(
            directory,
            createMergeScheduler(
                new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
                SearchIndex.MOCK_INDEX_GENERATION_ID,
                0,
                1,
                false, 
                directory),
            new TieredMergePolicy(),
            10.0,
            Optional.empty(),
            Optional.empty(),
            LuceneAnalyzer.indexAnalyzer(
                SearchIndex.MOCK_INDEX_DEFINITION, AnalyzerRegistryBuilder.empty()),
            MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            indexingMetricsUpdater,
            Optional.empty(),
            FeatureFlags.getDefault(),
            DynamicFeatureFlagRegistry.empty(),
            false);

    // Replace the lucene writer with the mock BEFORE creating the spy
    Field luceneWriterField = SingleLuceneIndexWriter.class.getDeclaredField("luceneWriter");
    luceneWriterField.setAccessible(true);
    var realLuceneWriter = (org.apache.lucene.index.IndexWriter) luceneWriterField.get(writer);
    var spyLuceneWriter = spy(realLuceneWriter);
    luceneWriterField.set(writer, spyLuceneWriter);

    // Now create the spy of the writer
    var spyWriter = spy(writer);

    // Mock doLogIndexingFailure to capture the call
    doNothing()
        .when(spyWriter)
        .doLogIndexingFailure(any(Throwable.class), anyString(), anyString());

    // Mock exception thrown from lucene writer updateDocuments to trigger
    // logIndexingFailureWithRateLimited
    doThrow(new TestException())
        .when(spyLuceneWriter).updateDocuments(any(org.apache.lucene.index.Term.class), any());

    // Test with BsonObjectId _id
    ObjectId objectIdValue = new ObjectId();
    BsonObjectId objectId = new BsonObjectId(objectIdValue);
    BsonDocument doc2 = new BsonDocument();
    doc2.append(
        MOCK_INDEX_DEFINITION.getIndexId().toString(), new BsonDocument("_id", objectId));
    var rawDoc2 = BsonUtils.documentToRaw(doc2);

    assertThrows(TestException.class, () -> {
      spyWriter.updateIndex(
          DocumentEvent.createInsert(
              DocumentMetadata.fromMetadataNamespace(
                  Optional.of(rawDoc2), MOCK_INDEX_ID),
              rawDoc2));
    });

    // Verify that doLogIndexingFailure was called with the ObjectId hex string
    verify(spyWriter).doLogIndexingFailure(
        any(TestException.class),
        eq("INSERT"),
        eq(objectIdValue.toHexString()));
  }

  @Test
  public void logIndexingFailureWithRateLimited_bsonString_logsUnloggable() throws Exception {
    var indexingMetricsUpdater =
        SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType());

    // need a new directory to work around write lock conflicts from the first writer
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    var writer =
        SingleLuceneIndexWriter.createForSearchIndex(
            directory,
            createMergeScheduler(
                new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
                SearchIndex.MOCK_INDEX_GENERATION_ID,
                0,
                1,
                false, 
                directory),
            new TieredMergePolicy(),
            10.0,
            Optional.empty(),
            Optional.empty(),
            LuceneAnalyzer.indexAnalyzer(
                SearchIndex.MOCK_INDEX_DEFINITION, AnalyzerRegistryBuilder.empty()),
            MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            indexingMetricsUpdater,
            Optional.empty(),
            FeatureFlags.getDefault(),
            DynamicFeatureFlagRegistry.empty(),
            false);

    // Replace the lucene writer with the mock BEFORE creating the spy
    Field luceneWriterField = SingleLuceneIndexWriter.class.getDeclaredField("luceneWriter");
    luceneWriterField.setAccessible(true);
    var realLuceneWriter = (org.apache.lucene.index.IndexWriter) luceneWriterField.get(writer);
    var spyLuceneWriter = spy(realLuceneWriter);
    luceneWriterField.set(writer, spyLuceneWriter);

    // Now create the spy of the writer
    var spyWriter = spy(writer);

    // Mock doLogIndexingFailure to capture the call
    doNothing()
        .when(spyWriter)
        .doLogIndexingFailure(any(Throwable.class), anyString(), anyString());

    // Mock exception thrown from lucene writer updateDocuments to trigger
    // logIndexingFailureWithRateLimited
    doThrow(new TestException())
        .when(spyLuceneWriter).updateDocuments(any(org.apache.lucene.index.Term.class), any());

    // Test with BsonString _id
    BsonString stringId = new BsonString("test-id-123");
    BsonDocument doc3 = new BsonDocument();
    doc3.append(
        MOCK_INDEX_DEFINITION.getIndexId().toString(), new BsonDocument("_id", stringId));
    var rawDoc3 = BsonUtils.documentToRaw(doc3);

    assertThrows(TestException.class, () -> {
      spyWriter.updateIndex(
          DocumentEvent.createInsert(
              DocumentMetadata.fromMetadataNamespace(
                  Optional.of(rawDoc3), MOCK_INDEX_ID),
              rawDoc3));
    });

    // Verify that doLogIndexingFailure was called with "unloggable" since BsonString is not
    // ObjectId or UUID
    verify(spyWriter).doLogIndexingFailure(
        any(TestException.class),
        eq("INSERT"),
        eq(LoggableIdUtils.UNLOGGABLE_ID_TYPE));
  }

  @Test
  public void logIndexingFailureWithRateLimited_decodeFailure_logsUnknownId() throws Exception {
    var indexingMetricsUpdater =
        SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType());

    // need a new directory to work around write lock conflicts from the first writer
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    var writer =
        SingleLuceneIndexWriter.createForSearchIndex(
            directory,
            createMergeScheduler(
                new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
                SearchIndex.MOCK_INDEX_GENERATION_ID,
                0,
                1,
                false, 
                directory),
            new TieredMergePolicy(),
            10.0,
            Optional.empty(),
            Optional.empty(),
            LuceneAnalyzer.indexAnalyzer(
                SearchIndex.MOCK_INDEX_DEFINITION, AnalyzerRegistryBuilder.empty()),
            MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            indexingMetricsUpdater,
            Optional.empty(),
            FeatureFlags.getDefault(),
            DynamicFeatureFlagRegistry.empty(),
            false);

    // Spy the writer to mock doLogIndexingFailure method
    var spyWriter = spy(writer);

    // Mock doLogIndexingFailure to capture the call
    doNothing()
        .when(spyWriter)
        .doLogIndexingFailure(any(Throwable.class), anyString(), anyString());

    // Use invalid bytes that will cause decode to fail
    byte[] invalidEncodedId = new byte[] {1, 2, 3, 4, 5}; // Invalid BSON bytes
    TestException testException = new TestException();

    // Use reflection to directly call logIndexingFailureWithRateLimited with invalid bytes
    // This tests the fallback to "unknown" when decoding fails
    java.lang.reflect.Method logMethod =
        SingleLuceneIndexWriter.class.getDeclaredMethod(
            "logIndexingFailureWithRateLimited", Throwable.class, String.class, byte[].class);
    logMethod.setAccessible(true);
    logMethod.invoke(spyWriter, testException, "INSERT", invalidEncodedId);

    // Verify that doLogIndexingFailure was called with "unknown" fallback
    ArgumentCaptor<String> documentIdCaptor = ArgumentCaptor.forClass(String.class);
    verify(spyWriter).doLogIndexingFailure(
        eq(testException),
        eq("INSERT"),
        documentIdCaptor.capture());
    // Verify the captured documentIdString is "unknown" when decoding fails
    var capturedDocumentId = documentIdCaptor.getValue();
    assertThat(capturedDocumentId).isEqualTo(LoggableIdUtils.UNKNOWN_LOGGABLE_ID);
  }
}
