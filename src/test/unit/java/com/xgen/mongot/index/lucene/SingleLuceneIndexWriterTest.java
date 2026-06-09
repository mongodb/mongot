package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static com.xgen.mongot.index.lucene.InstrumentedConcurrentMergeSchedulerTest.createMergeScheduler;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.TOKEN;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.mockMetricsFactory;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.IndexClosedException;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.WriterClosedException;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.lucene.codec.bloom.BloomCodecPolicy;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.query.custom.ExactVectorSearchQuery;
import com.xgen.mongot.index.lucene.query.range.RangeQueryUtils;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.lucene.writer.SingleLuceneIndexWriter;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.ConcurrencyTestUtils;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFacetFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.hamcrest.core.Is;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public class SingleLuceneIndexWriterTest {

  private static SingleLuceneIndexWriter getWriter(
      Directory directory, AnalyzerRegistry analyzerRegistry) throws Exception {
    return getWriter(
        directory,
        MOCK_INDEX_DEFINITION,
        MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
        analyzerRegistry,
        SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType()));
  }

  private static SingleLuceneIndexWriter getWriter(
      Directory directory,
      AnalyzerRegistry analyzerRegistry,
      IndexDeletionPolicy indexDeletionPolicy)
      throws Exception {
    return getWriter(
        directory,
        MOCK_INDEX_DEFINITION,
        MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion,
        analyzerRegistry,
        SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType()),
        Optional.of(indexDeletionPolicy));
  }

  private static SingleLuceneIndexWriter getWriter(
      Directory directory, AnalyzerRegistry analyzerRegistry, int fieldLimit, int docsLimit)
      throws IOException {

    return SingleLuceneIndexWriter.createForSearchIndex(
        directory,
        createMergeScheduler(
            new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
            MOCK_INDEX_GENERATION_ID,
            0,
            1,
            false,
            directory),
        new TieredMergePolicy(),
        16D,
        Optional.of(fieldLimit),
        Optional.of(docsLimit),
        LuceneAnalyzer.indexAnalyzer(MOCK_INDEX_DEFINITION, analyzerRegistry),
        MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
            MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion),
        SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType()),
        Optional.empty(),
        FeatureFlags.getDefault(),
        DynamicFeatureFlagRegistry.empty(),
        false);
  }

  private static SingleLuceneIndexWriter getWriter(
      Directory directory,
      IndexFormatVersion indexFormatVersion,
      AnalyzerRegistry analyzerRegistry,
      int fieldLimit,
      int docsLimit)
      throws IOException {
    return SingleLuceneIndexWriter.createForSearchIndex(
        directory,
        createMergeScheduler(
            new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
            MOCK_INDEX_GENERATION_ID,
            0,
            1,
            false,
            directory),
        new TieredMergePolicy(),
        16D,
        Optional.of(fieldLimit),
        Optional.of(docsLimit),
        LuceneAnalyzer.indexAnalyzer(MOCK_INDEX_DEFINITION, analyzerRegistry),
        MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(indexFormatVersion),
        SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType()),
        Optional.empty(),
        FeatureFlags.getDefault(),
        DynamicFeatureFlagRegistry.empty(),
        false);
  }

  private static SingleLuceneIndexWriter getWriter() throws Exception {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    return getWriter(directory, AnalyzerRegistryBuilder.empty());
  }

  private static SingleLuceneIndexWriter getWriter(
      Directory indexDirectory,
      SearchIndexDefinition indexDefinition,
      IndexFormatVersion indexFormatVersion,
      AnalyzerRegistry analyzerRegistry,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater)
      throws Exception {
    return getWriter(
        indexDirectory,
        indexDefinition,
        indexFormatVersion,
        analyzerRegistry,
        indexingMetricsUpdater,
        Optional.empty());
  }

  private static SingleLuceneIndexWriter getWriter(
      Directory indexDirectory,
      SearchIndexDefinition indexDefinition,
      IndexFormatVersion indexFormatVersion,
      AnalyzerRegistry analyzerRegistry,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater,
      Optional<IndexDeletionPolicy> indexDeletionPolicy)
      throws Exception {
    return SingleLuceneIndexWriter.createForSearchIndex(
        indexDirectory,
        createMergeScheduler(
            new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
            MOCK_INDEX_GENERATION_ID,
            0,
            1,
            false,
            indexDirectory),
        new TieredMergePolicy(),
        16D,
        Optional.empty(),
        Optional.empty(),
        LuceneAnalyzer.indexAnalyzer(indexDefinition, analyzerRegistry),
        indexDefinition.createFieldDefinitionResolver(indexFormatVersion),
        indexingMetricsUpdater,
        indexDeletionPolicy,
        FeatureFlags.getDefault(),
        DynamicFeatureFlagRegistry.empty(),
        false);
  }

  private static SingleLuceneIndexWriter getVectorIndexWriter(
      Directory indexDirectory,
      VectorIndexDefinition indexDefinition,
      IndexFormatVersion indexFormatVersion)
      throws Exception {
    return SingleLuceneIndexWriter.createForVectorIndex(
        indexDirectory,
        createMergeScheduler(
            new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
            MOCK_INDEX_GENERATION_ID,
            0,
            1,
            false,
            indexDirectory),
        new TieredMergePolicy(),
        16D,
        Optional.empty(),
        Optional.empty(),
        indexDefinition,
        indexDefinition.getIndexCapabilities(indexFormatVersion),
        VectorIndex.mockIndexingMetricsUpdater(),
        Optional.empty(),
        FeatureFlags.getDefault(),
        () -> false);
  }

  @RunWith(Theories.class)
  public static class MainTests {

    @DataPoints
    public static IndexFormatVersion[] indexFormatVersions() {
      return new IndexFormatVersion[] {
        IndexFormatVersion.MIN_SUPPORTED_VERSION, IndexFormatVersion.CURRENT
      };
    }

    /**
     * Tests that LuceneIndexWriter can commit user data, and retrieve it again after re-opening the
     * index.
     */
    @Test
    public void testCommitUserData() throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create some user data to commit.
      EncodedUserData committedUserData = EncodedUserData.fromString("{\"foo\":\"bar\"}");

      // Create the lucene index and create a LuceneIndexWriter with it.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer = getWriter(directory, analyzerRegistry);

        // Add a document.
        ObjectId indexId = new ObjectId();
        RawBsonDocument document =
            BsonUtils.documentToRaw(
                new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));
        writer.updateIndex(
            DocumentEvent.createInsert(
                DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document));

        // Ensure that we can retrieve the user data after committing it.
        writer.commit(committedUserData);

        EncodedUserData retrievedUserData = writer.getCommitUserData();
        assertEquals(
            "retrieved user data did not match committed user data",
            committedUserData,
            retrievedUserData);

        writer.close();
      }

      // Close the index and re-open it, and ensure we can still retrieve the user data from the
      // last commit.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer = getWriter(directory, analyzerRegistry);

        // Ensure that we can retrieve the user data after re-opening the index.
        EncodedUserData retrievedUserData = writer.getCommitUserData();
        assertEquals(
            "retrieved user data did not match committed user data after re-opening",
            committedUserData,
            retrievedUserData);

        writer.close();
      }
    }

    @Test
    public void testNumDocs() throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create the lucene index and create a LuceneIndexWriter with it.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer = getWriter(directory, analyzerRegistry);

        // numDocs should start at 0
        assertEquals(0, writer.getNumDocs());
        assertEquals(0, writer.getNumLuceneMaxDocs());
        assertEquals(0, writer.getMaxLuceneMaxDocs());

        // Add a document and see numDocs increment.
        ObjectId indexId = new ObjectId();
        RawBsonDocument document =
            BsonUtils.documentToRaw(
                new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));
        writer.updateIndex(
            DocumentEvent.createInsert(
                DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document));
        writer.commit(EncodedUserData.EMPTY);
        assertEquals(1, writer.getNumDocs());
        assertEquals(1, writer.getNumLuceneMaxDocs());
        assertEquals(1, writer.getMaxLuceneMaxDocs());

        // Don't increment on update
        writer.updateIndex(
            DocumentEvent.createUpdate(
                DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document));
        writer.commit(EncodedUserData.EMPTY);
        // should be 1 still
        assertEquals(1, writer.getNumDocs());
        assertEquals(1, writer.getNumLuceneMaxDocs());
        assertEquals(1, writer.getMaxLuceneMaxDocs());

        // Now, another insert
        RawBsonDocument document2 =
            BsonUtils.documentToRaw(
                new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(2))));
        writer.updateIndex(
            DocumentEvent.createInsert(
                DocumentMetadata.fromMetadataNamespace(Optional.of(document2), indexId),
                document2));
        writer.commit(EncodedUserData.EMPTY);
        assertEquals(2, writer.getNumDocs());
        assertEquals(2, writer.getNumLuceneMaxDocs());
        assertEquals(2, writer.getMaxLuceneMaxDocs());

        // Now, a delete
        writer.updateIndex(DocumentEvent.createDelete(new BsonInt32(1)));
        writer.commit(EncodedUserData.EMPTY);
        assertEquals(1, writer.getNumDocs());
        assertEquals(1, writer.getNumLuceneMaxDocs());
        assertEquals(1, writer.getMaxLuceneMaxDocs());
        writer.close();
      }

      // Close the index and re-open it, then check that getNumDocs is what we expect.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer = getWriter(directory, analyzerRegistry);

        // Check that writer is initialized with correct number of docs
        assertEquals(1, writer.getNumDocs());

        writer.close();
      }
    }

    @Test
    public void testNumDocsAfterClose() throws Exception {
      var indexWriter = getWriter();

      indexWriter.close();

      assertThrows(WriterClosedException.class, indexWriter::getNumDocs);
    }

    @Test
    public void testNumFieldsAfterClose() throws Exception {
      var indexWriter = getWriter();

      indexWriter.close();

      assertThrows(WriterClosedException.class, indexWriter::getNumFields);
    }

    /** Tests that LuceneIndexWriter does not commit user data when the index is closing. */
    @Test
    public void testCloseDoesNotCommit() throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create the lucene index and create a LuceneIndexWriter with it.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer = getWriter(directory, analyzerRegistry);

        // numDocs should start at 0
        assertEquals(0, writer.getNumDocs());

        // Insert a document but don't commit.
        ObjectId indexId = new ObjectId();
        RawBsonDocument document =
            BsonUtils.documentToRaw(
                new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(1))));
        writer.updateIndex(
            DocumentEvent.createInsert(
                DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document));
        assertEquals(1, writer.getNumDocs());

        // Close the index
        writer.close();
      }

      // Re-open the index, then check that getNumDocs is what we expect.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
          SingleLuceneIndexWriter writer = getWriter(directory, AnalyzerRegistryBuilder.empty())) {
        // Check that writer did not insert the doc while closing
        assertEquals(0, writer.getNumDocs());
      }
    }

    @Test
    public void testAlreadyClosedIndexDoesNotThrowOnClose() throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create the lucene index and create a LuceneIndexWriter with it.
      Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
      AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
      SingleLuceneIndexWriter writer = getWriter(directory, analyzerRegistry);

      // Close the Directory out from under the LuceneIndexWriter, simulating an exception in
      // another thread such as an OOM.
      directory.close();

      // Close the index, which should not throw.
      writer.close();
    }

    @Test
    public void testAlreadyClosedDoesThrowOnCommit() throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create the lucene index and create a LuceneIndexWriter with it.
      Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
      AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
      SingleLuceneIndexWriter writer = getWriter(directory, analyzerRegistry);

      // Close the index
      writer.close();

      EncodedUserData userDataToCommit = EncodedUserData.fromString("{\"foo\":\"bar\"}");
      assertThrows(IndexClosedException.class, () -> writer.commit(userDataToCommit));
    }

    @Theory
    public void testUpdateIndexInsert(IndexFormatVersion indexFormatVersion) throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create the lucene index and create a LuceneIndexWriter with it.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer =
            getWriter(
                directory,
                MOCK_INDEX_DEFINITION,
                indexFormatVersion,
                analyzerRegistry,
                SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType()));
        BsonDocument bsonDocument =
            new BsonDocument()
                .append(MOCK_INDEX_ID.toString(), new BsonDocument("_id", new BsonInt32(1)))
                .append("_id", new BsonInt32(1))
                .append("foo", new BsonString("bar"));

        assertInsertDocumentWorks(writer, bsonDocument);
      }
    }

    @Theory
    public void testUpdateIndexUpdate(IndexFormatVersion indexFormatVersion) throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create the lucene index and create a LuceneIndexWriter with it.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer =
            getWriter(
                directory,
                MOCK_INDEX_DEFINITION,
                indexFormatVersion,
                analyzerRegistry,
                SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType()));
        BsonDocument originalBsonDocument =
            new BsonDocument()
                .append(MOCK_INDEX_ID.toString(), new BsonDocument("_id", new BsonInt32(1)))
                .append("_id", new BsonInt32(1))
                .append("foo", new BsonString("bar"));

        // First add a document to the index.
        assertInsertDocumentWorks(writer, originalBsonDocument);

        // Then update the same document, and ensure that the updated version of the document is
        // reflected in the index, and is the only document in the index.
        RawBsonDocument updatedBsonDocument =
            BsonUtils.documentToRaw(
                new BsonDocument()
                    .append(
                        MOCK_INDEX_DEFINITION.getIndexId().toString(),
                        new BsonDocument("_id", new BsonInt32(1)))
                    .append("_id", new BsonInt32(1))
                    .append("foo", new BsonString("buzz")));
        writer.updateIndex(
            DocumentEvent.createUpdate(
                DocumentMetadata.fromMetadataNamespace(
                    Optional.of(updatedBsonDocument), MOCK_INDEX_DEFINITION.getIndexId()),
                updatedBsonDocument));

        assertDocumentOnlyOneInIndexCurrentIfv(writer, updatedBsonDocument);
      }
    }

    @Theory
    public void testUpdateIndexDelete(IndexFormatVersion indexFormatVersion) throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create the lucene index and create a LuceneIndexWriter with it.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer =
            getWriter(
                directory,
                MOCK_INDEX_DEFINITION,
                indexFormatVersion,
                analyzerRegistry,
                SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType()));
        BsonDocument bsonDocument =
            new BsonDocument()
                .append(MOCK_INDEX_ID.toString(), new BsonDocument("_id", new BsonInt32(1)))
                .append("_id", new BsonInt32(1))
                .append("foo", new BsonString("bar"));

        // First add a document to the index.
        assertInsertDocumentWorks(writer, bsonDocument);

        // Then delete the document, and ensure that document is not in the index.
        writer.updateIndex(DocumentEvent.createDelete(new BsonInt32(1)));
        assertIndexIsEmpty(writer);
      }
    }

    @Test
    public void testDeleteAll() throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create the lucene index and create a LuceneIndexWriter with it.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer = getWriter(directory, analyzerRegistry);

        // Add two documents.
        RawBsonDocument document1 =
            BsonUtils.documentToRaw(
                new BsonDocument(
                    MOCK_INDEX_DEFINITION.getIndexId().toString(),
                    new BsonDocument("_id", new BsonInt32(1))));
        RawBsonDocument document2 =
            BsonUtils.documentToRaw(
                new BsonDocument(
                    MOCK_INDEX_DEFINITION.getIndexId().toString(),
                    new BsonDocument("_id", new BsonInt32(2))));

        writer.updateIndex(
            DocumentEvent.createInsert(
                DocumentMetadata.fromMetadataNamespace(
                    Optional.of(document1), MOCK_INDEX_DEFINITION.getIndexId()),
                document1));
        writer.updateIndex(
            DocumentEvent.createInsert(
                DocumentMetadata.fromMetadataNamespace(
                    Optional.of(document2), MOCK_INDEX_DEFINITION.getIndexId()),
                document2));

        // Ensure that we can retrieve the document.
        writer.commit(EncodedUserData.EMPTY);
        try (DirectoryReader reader = DirectoryReader.open(writer.getLuceneWriter())) {
          IndexSearcher searcher = new IndexSearcher(reader);
          assertEquals(2, searcher.search(new MatchAllDocsQuery(), 10).totalHits.value());
        }

        // Delete all of documents and ensure we can't retrieve any.
        writer.deleteAll(EncodedUserData.EMPTY);
        assertIndexIsEmpty(writer);
      }
    }

    // Verify index document deletion behavior when index snapshots are acquired.
    @Test
    public void testDeleteAllWithSnapshotDeletion() throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create the lucene index and create a LuceneIndexWriter with it.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SnapshotDeletionPolicy snapshotDeletionPolicy =
            new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
        SingleLuceneIndexWriter writer =
            getWriter(directory, analyzerRegistry, snapshotDeletionPolicy);

        // Add two documents.
        RawBsonDocument document1 =
            BsonUtils.documentToRaw(
                new BsonDocument(
                    MOCK_INDEX_DEFINITION.getIndexId().toString(),
                    new BsonDocument("_id", new BsonInt32(1))));
        RawBsonDocument document2 =
            BsonUtils.documentToRaw(
                new BsonDocument(
                    MOCK_INDEX_DEFINITION.getIndexId().toString(),
                    new BsonDocument("_id", new BsonInt32(2))));

        writer.updateIndex(
            DocumentEvent.createInsert(
                DocumentMetadata.fromMetadataNamespace(
                    Optional.of(document1), MOCK_INDEX_DEFINITION.getIndexId()),
                document1));
        writer.updateIndex(
            DocumentEvent.createInsert(
                DocumentMetadata.fromMetadataNamespace(
                    Optional.of(document2), MOCK_INDEX_DEFINITION.getIndexId()),
                document2));

        // Ensure that we can retrieve the document.
        writer.commit(EncodedUserData.EMPTY);
        try (DirectoryReader reader = DirectoryReader.open(writer.getLuceneWriter())) {
          IndexSearcher searcher = new IndexSearcher(reader);
          assertEquals(2, searcher.search(new MatchAllDocsQuery(), 10).totalHits.value());
        }

        // Acquire a snapshot, and verify that while deletions can happen, the snapshot files are
        // not removed.
        IndexCommit snapshot = snapshotDeletionPolicy.snapshot();

        // Delete all of documents and ensure we can't retrieve any.
        writer.deleteAll(EncodedUserData.EMPTY);
        assertIndexIsEmpty(writer);

        assertWithMessage("Snapshotted index files should not be removed")
            .that(
                Arrays.stream(temporaryFolder.getRoot().toPath().toFile().listFiles())
                    .map(File::getName))
            .containsAtLeastElementsIn(snapshot.getFileNames());
        snapshotDeletionPolicy.release(snapshot);
      }
    }

    /**
     * Similar to how we test testCommitUserData, we do the same for user data committed due to
     * deleteAll.
     */
    @Test
    public void testDeleteAllCommitsUserData() throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create some user data to commit.
      EncodedUserData committedUserData = EncodedUserData.fromString("{\"foo\":\"bar\"}");

      // Call deleteAll
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer = getWriter(directory, analyzerRegistry);

        writer.deleteAll(committedUserData);
        writer.close();
      }

      // Close the index and re-open it, and ensure we can still retrieve the user data from the
      // commit in deleteAll..
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer = getWriter(directory, analyzerRegistry);
        EncodedUserData persistedCommitData = writer.getCommitUserData();

        assertEquals(committedUserData, persistedCommitData);
      }
    }

    @Test
    public void testIdTooBigInsertion() throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create the lucene index and create a LuceneIndexWriter with it.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer = getWriter(directory, analyzerRegistry);

        // Add a document with an _id larger than MAX_TERM_LENGTH.
        BsonString id = new BsonString("a".repeat(IndexWriter.MAX_TERM_LENGTH + 1));

        RawBsonDocument document =
            BsonUtils.documentToRaw(
                new BsonDocument(
                    MOCK_INDEX_DEFINITION.getIndexId().toString(), new BsonDocument("_id", id)));

        writer.updateIndex(
            DocumentEvent.createInsert(
                DocumentMetadata.fromMetadataNamespace(
                    Optional.of(document), MOCK_INDEX_DEFINITION.getIndexId()),
                document));

        // Should not have indexed the document but also should not have thrown an exception.
        writer.commit(EncodedUserData.EMPTY);
        assertIndexIsEmpty(writer);
      }
    }

    @Test
    public void testIdTooBigUpdate() throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create the lucene index and create a LuceneIndexWriter with it.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer = getWriter(directory, analyzerRegistry);

        // Add a document with an _id larger than MAX_TERM_LENGTH.
        BsonString id = new BsonString("a".repeat(IndexWriter.MAX_TERM_LENGTH + 1));

        RawBsonDocument document =
            BsonUtils.documentToRaw(
                new BsonDocument(
                    MOCK_INDEX_DEFINITION.getIndexId().toString(), new BsonDocument("_id", id)));

        // Call update event.
        writer.updateIndex(
            DocumentEvent.createUpdate(
                DocumentMetadata.fromMetadataNamespace(
                    Optional.of(document), MOCK_INDEX_DEFINITION.getIndexId()),
                document));

        // Should not have indexed the document but also should not have thrown an exception.
        writer.commit(EncodedUserData.EMPTY);
        assertIndexIsEmpty(writer);
      }
    }

    @Test
    public void testIdTooBigInDeletion() throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create the lucene index and create a LuceneIndexWriter with it.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer = getWriter(directory, analyzerRegistry);

        // Add a document with an _id larger than MAX_TERM_LENGTH.
        BsonString id = new BsonString("a".repeat(IndexWriter.MAX_TERM_LENGTH + 1));

        // Call updateIndex with delete event.
        writer.updateIndex(DocumentEvent.createDelete(id));

        // Should not have indexed the document but also should not have thrown an exception.
        writer.commit(EncodedUserData.EMPTY);
        assertIndexIsEmpty(writer);
      }
    }

    @Test
    public void testCannotCloseWhileUpdating() throws Exception {
      // Use an invocation of DocumentEvent::getEventType as a signal that we're within the
      // critical section in updateIndex().
      DocumentEvent event = spy(DocumentEvent.createDelete(new BsonInt32(0)));
      SingleLuceneIndexWriter writer = getWriter();

      ConcurrencyTestUtils.assertCannotBeInvokedConcurrently(
          when(event.getEventType()), () -> writer.updateIndex(event), writer::close);
    }

    @Test
    public void testIndexAndDocValuesQueriesBehaveTheSameForStrings() throws Exception {
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        DocumentFieldDefinition documentFieldDefinition =
            DocumentFieldDefinitionBuilder.builder()
                .field(
                    "category",
                    FieldDefinitionBuilder.builder()
                        .token(TokenFieldDefinitionBuilder.builder().build())
                        .build())
                .build();

        SearchIndexDefinition indexDefinition =
            SearchIndex.mockDefinitionBuilder().mappings(documentFieldDefinition).build();

        SingleLuceneIndexWriter writer =
            getWriter(
                directory,
                indexDefinition,
                IndexFormatVersion.CURRENT,
                analyzerRegistry,
                SearchIndex.mockIndexingMetricsUpdater(indexDefinition.getType()));

        List<BsonDocument> documents =
            List.of(
                new BsonDocument()
                    .append(MOCK_INDEX_ID.toString(), new BsonDocument("_id", new BsonInt32(1)))
                    .append("category", new BsonString("a")),
                new BsonDocument()
                    .append(MOCK_INDEX_ID.toString(), new BsonDocument("_id", new BsonInt32(2)))
                    .append("category", new BsonString("b")),
                new BsonDocument()
                    .append(MOCK_INDEX_ID.toString(), new BsonDocument("_id", new BsonInt32(3)))
                    .append("category", new BsonString("c")));

        for (BsonDocument document : documents) {
          writer.updateIndex(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromMetadataNamespace(
                      Optional.of(BsonUtils.documentToRaw(document)), MOCK_INDEX_ID),
                  BsonUtils.documentToRaw(document)));
        }

        writer.commit(EncodedUserData.EMPTY);

        try (DirectoryReader reader = DirectoryReader.open(writer.getLuceneWriter())) {
          var searcher = new IndexSearcher(reader);
          var fieldName = TOKEN.getLuceneFieldName(FieldPath.newRoot("category"), Optional.empty());

          // equals
          assertEquals(
              getMatchedIds(
                  searcher,
                  new TermInSetQuery(fieldName, Collections.singletonList(new BytesRef("a")))),
              getMatchedIds(
                  searcher,
                  SortedSetDocValuesField.newSlowSetQuery(fieldName, List.of(new BytesRef("a")))));

          // in
          assertEquals(
              new TermInSetQuery(fieldName, List.of(new BytesRef("a"), new BytesRef("c"))),
              SortedSetDocValuesField.newSlowSetQuery(
                  fieldName, Arrays.asList(new BytesRef("a"), new BytesRef("c"))));

          // range
          assertEquals(
              getMatchedIds(
                  searcher,
                  new TermRangeQuery(fieldName, new BytesRef("a"), new BytesRef("c"), false, true)),
              getMatchedIds(
                  searcher,
                  SortedSetDocValuesField.newSlowRangeQuery(
                      fieldName, new BytesRef("a"), new BytesRef("c"), false, true)));
        }
      }
    }

    @Test
    public void testIndexAndDocValuesQueriesBehaveTheSameForNumericAndDateTypes() throws Exception {

      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();

        SingleLuceneIndexWriter writer =
            getWriter(
                directory,
                MOCK_INDEX_DEFINITION,
                IndexFormatVersion.CURRENT,
                analyzerRegistry,
                SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType()));

        List<BsonDocument> documents =
            List.of(
                new BsonDocument()
                    .append(MOCK_INDEX_ID.toString(), new BsonDocument("_id", new BsonInt32(1)))
                    .append("int", new BsonInt64(94))
                    .append("double", new BsonDouble(8942.21))
                    .append("date", new BsonDateTime(1679608929)),
                new BsonDocument()
                    .append(MOCK_INDEX_ID.toString(), new BsonDocument("_id", new BsonInt32(2)))
                    .append("int", new BsonInt64(Long.MAX_VALUE))
                    .append("double", new BsonDouble(Double.MIN_VALUE))
                    .append("date", new BsonDateTime(Integer.MAX_VALUE)),
                new BsonDocument(
                    MOCK_INDEX_ID.toString(), new BsonDocument("_id", new BsonInt32(3))));

        for (BsonDocument document : documents) {
          writer.updateIndex(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromMetadataNamespace(
                      Optional.of(BsonUtils.documentToRaw(document)), MOCK_INDEX_ID),
                  BsonUtils.documentToRaw(document)));
        }

        writer.commit(EncodedUserData.EMPTY);

        try (DirectoryReader reader = DirectoryReader.open(writer.getLuceneWriter())) {
          IndexSearcher searcher = new IndexSearcher(reader);

          assertIndexAndDocValuesResults(
              searcher,
              FieldName.TypeField.NUMBER_DOUBLE,
              "int",
              LuceneDoubleConversionUtils.toIndexedLong(93L),
              LuceneDoubleConversionUtils.toIndexedLong(94L),
              Set.of(1));

          assertIndexAndDocValuesResults(
              searcher,
              FieldName.TypeField.NUMBER_DOUBLE,
              "double",
              LuceneDoubleConversionUtils.toMqlSortableLong(8942.20),
              LuceneDoubleConversionUtils.toMqlSortableLong(8942.22),
              Set.of(1));

          assertIndexAndDocValuesResults(
              searcher, FieldName.TypeField.DATE, "date", 1679608928, 1679608930, Set.of(1));
        }
      }
    }

    @Theory
    public void testSortableStringTruncated(IndexFormatVersion indexFormatVersion)
        throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create the lucene index and create a LuceneIndexWriter with it.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        SearchIndexDefinition indexDef =
            SearchIndex.mockDefinitionBuilder()
                .mappings(
                    DocumentFieldDefinitionBuilder.builder()
                        .field(
                            "test",
                            FieldDefinitionBuilder.builder()
                                .token(TokenFieldDefinitionBuilder.builder().build())
                                .build())
                        .build())
                .build();
        var indexingMetricsUpdater = SearchIndex.mockIndexingMetricsUpdater(indexDef.getType());
        SingleLuceneIndexWriter writer =
            getWriter(
                directory,
                indexDef,
                indexFormatVersion,
                AnalyzerRegistryBuilder.empty(),
                indexingMetricsUpdater);

        // Write a string so long it gets truncated as part of a sortable field.
        int bigLen = 16 << 10;
        var bigString = "a".repeat(bigLen);
        BsonDocument bsonDocument =
            new BsonDocument()
                .append(MOCK_INDEX_ID.toString(), new BsonDocument("_id", new BsonInt32(1)))
                .append("test", new BsonString(bigString));

        writer.updateIndex(
            DocumentEvent.createInsert(
                DocumentMetadata.fromMetadataNamespace(
                    Optional.of(BsonUtils.documentToRaw(bsonDocument)), MOCK_INDEX_ID),
                BsonUtils.documentToRaw(bsonDocument)));

        assertEquals(1, (int) indexingMetricsUpdater.getSortableStringTruncatedCounter().count());
      }
    }

    private static void assertIndexAndDocValuesResults(
        IndexSearcher searcher,
        FieldName.TypeField typeField,
        String fieldName,
        long lowerValue,
        long upperValue,
        Set<Integer> expectedIds)
        throws IOException {
      Set<Integer> indexQueryDocIds =
          getMatchedIds(
              searcher,
              RangeQueryUtils.createLongPointQuery(
                  typeField.getLuceneFieldName(FieldPath.newRoot(fieldName), Optional.empty()),
                  lowerValue,
                  upperValue));

      Set<Integer> docValuesQueryDocIds =
          getMatchedIds(
              searcher,
              RangeQueryUtils.createDocValuesQuery(
                  typeField.getLuceneFieldName(FieldPath.newRoot(fieldName), Optional.empty()),
                  lowerValue,
                  upperValue));

      assertEquals(expectedIds, indexQueryDocIds);
      assertEquals(expectedIds, docValuesQueryDocIds);
    }

    private static Set<Integer> getMatchedIds(IndexSearcher searcher, Query query)
        throws IOException {
      return Arrays.stream(searcher.search(query, 10).scoreDocs)
          .map(
              doc -> {
                try {
                  return LuceneDocumentIdEncoder.documentIdFromLuceneDocument(
                          searcher.storedFields().document(doc.doc))
                      .asInt32()
                      .getValue();
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              })
          .collect(Collectors.toSet());
    }

    private static void assertInsertDocumentWorks(
        SingleLuceneIndexWriter writer, BsonDocument bsonDocument) throws Exception {
      // Insert a document.
      writer.updateIndex(
          DocumentEvent.createInsert(
              DocumentMetadata.fromMetadataNamespace(
                  Optional.of(BsonUtils.documentToRaw(bsonDocument)), MOCK_INDEX_ID),
              BsonUtils.documentToRaw(bsonDocument)));

      assertDocumentOnlyOneInIndexCurrentIfv(writer, bsonDocument);
    }

    /**
     * Ensures the document exists in the index and is the only document in the index.
     *
     * <p>NOTE: assumes the document is of form { _id: value, bar: value }.
     */
    private static void assertDocumentOnlyOneInIndexCurrentIfv(
        SingleLuceneIndexWriter writer, BsonDocument bsonDocument) throws Exception {
      try (DirectoryReader reader = DirectoryReader.open(writer.getLuceneWriter())) {
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs docs = searcher.search(new MatchAllDocsQuery(), 10);
        assertEquals(1, docs.totalHits.value());

        // Ensure the indexed document matches the expected.
        Document storedFields = searcher.storedFields().document(docs.scoreDocs[0].doc);
        assertEquals(2, storedFields.getFields().size());

        IndexableField idField = storedFields.getField("$meta/_id");
        assertNotNull(idField);
        byte[] expectedIdFieldValue =
            LuceneDocumentIdEncoder.encodeDocumentId(bsonDocument.get("_id"));
        assertArrayEquals(expectedIdFieldValue, idField.binaryValue().bytes);

        IndexableField fooField = storedFields.getField("$type:string/foo");
        assertNotNull(fooField);
        assertEquals(bsonDocument.get("foo").asString().getValue(), fooField.stringValue());
      }
    }

    /**
     * Ensures the document exists in the vector index and is the only document in the index.
     *
     * <p>NOTE: assumes the document is of form { _id: value, field: value }.
     */
    private static void assertAutoEmbeddingDocumentWithVectorIncluded(
        SingleLuceneIndexWriter writer, BsonDocument bsonDocument, Vector queryVector)
        throws Exception {
      try (DirectoryReader reader = DirectoryReader.open(writer.getLuceneWriter())) {
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs docs =
            searcher.search(
                new ExactVectorSearchQuery(
                    "$type:knnVector/field",
                    queryVector,
                    VectorSimilarityFunction.EUCLIDEAN,
                    new FieldExistsQuery("$type:knnVector/field")),
                10);
        assertEquals(1, docs.totalHits.value());
        // Ensure the indexed document matches the expected.
        Document storedFields = searcher.storedFields().document(docs.scoreDocs[0].doc);
        // Embeddings are not stored, only exists in index.
        assertEquals(1, storedFields.getFields().size());

        IndexableField idField = storedFields.getField("$meta/_id");
        assertNotNull(idField);

        byte[] expectedIdFieldValue =
            LuceneDocumentIdEncoder.encodeDocumentId(bsonDocument.get("_id"));
        assertArrayEquals(expectedIdFieldValue, idField.binaryValue().bytes);
      }
    }

    private static void assertIndexIsEmpty(SingleLuceneIndexWriter writer) throws Exception {
      try (DirectoryReader reader = DirectoryReader.open(writer.getLuceneWriter())) {
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs docs = searcher.search(new MatchAllDocsQuery(), 10);
        assertEquals(0, docs.totalHits.value());
      }
    }

    @Theory
    public void testFieldLimitsMeasuredConsistentlyCurrentIfv(IndexFormatVersion indexFormatVersion)
        throws Exception {
      Assume.assumeTrue(indexFormatVersion.versionNumber >= IndexFormatVersion.FIVE.versionNumber);

      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create the lucene index and create a LuceneIndexWriter with it.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        // writer with field limit
        SingleLuceneIndexWriter writer =
            getWriter(
                directory, indexFormatVersion, analyzerRegistry, 4, LuceneConfig.MAX_DOCS_LIMIT);
        writer.getLuceneWriter();
        // no documents, should not exceed:
        assertTrue(writer.exceededLimits().isEmpty());

        // add documents and monitor whether it has failed or not:
        insertWithField(writer, "f1");
        assertTrue(writer.exceededLimits().isEmpty());

        insertWithField(writer, "f2");
        // should exceed, we have 5 fields at this point [f1, f2, _id, $meta/_id, $meta/fieldNames]
        assertTrue(writer.exceededLimits().isPresent());
        assertEquals(
            "Field limit exceeded: 5 > 4", writer.exceededLimits().orElseThrow().getMessage());

        writer.commit(EncodedUserData.EMPTY);
        writer.close();
      }

      // Close the index and re-open it
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer =
            getWriter(directory, analyzerRegistry, 4, LuceneConfig.MAX_DOCS_LIMIT);

        // Check that writer is initialized with correct number of fields
        assertEquals(
            "Field limit exceeded: 5 > 4", writer.exceededLimits().orElseThrow().getMessage());

        // deleting all should clear the field names
        writer.deleteAll(EncodedUserData.EMPTY);
        assertTrue(writer.exceededLimits().isEmpty());
        writer.close();
      }
    }

    @Theory
    @Ignore
    public void testFieldLimitsMeasuredConsistentlyMinSupportedIfv(
        IndexFormatVersion indexFormatVersion) throws Exception {
      Assume.assumeThat(indexFormatVersion, Is.is(IndexFormatVersion.FIVE));

      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create the lucene index and create a LuceneIndexWriter with it.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        // writer with field limit
        SingleLuceneIndexWriter writer =
            getWriter(
                directory, indexFormatVersion, analyzerRegistry, 3, LuceneConfig.MAX_DOCS_LIMIT);
        writer.getLuceneWriter();
        // no documents, should not exceed:
        assertTrue(writer.exceededLimits().isEmpty());

        // add documents and monitor whether it has failed or not:
        insertWithField(writer, "f1");
        assertTrue(writer.exceededLimits().isEmpty());

        insertWithField(writer, "f2");
        // should exceed, we have 4 fields at this point [f1, f2, $meta/_id, $meta/fieldNames]
        assertTrue(writer.exceededLimits().isPresent());
        assertEquals(
            "Field limit exceeded: 4 > 3", writer.exceededLimits().orElseThrow().getMessage());

        writer.commit(EncodedUserData.EMPTY);
        writer.close();
      }

      // Close the index and re-open it
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer =
            getWriter(directory, analyzerRegistry, 3, LuceneConfig.MAX_DOCS_LIMIT);

        // Check that writer is initialized with correct number of fields
        assertEquals(
            "Field limit exceeded: 4 > 3", writer.exceededLimits().orElseThrow().getMessage());

        // deleting all should clear the field names
        writer.deleteAll(EncodedUserData.EMPTY);
        assertTrue(writer.exceededLimits().isEmpty());
        writer.close();
      }
    }

    @Theory
    public void testDocLimitsMeasuredConsistently(IndexFormatVersion indexFormatVersion)
        throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      // Create the lucene index and create a LuceneIndexWriter with it.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        // writer with docs limit
        SingleLuceneIndexWriter writer =
            getWriter(directory, indexFormatVersion, analyzerRegistry, 30, 1);
        writer.getLuceneWriter();
        // no documents, should not exceed:
        assertTrue(writer.exceededLimits().isEmpty());

        // add a document and check whether it has failed or not:
        insert(writer);
        // 1 doc, limit is 1, should not exceed
        assertTrue(writer.exceededLimits().isEmpty());

        insert(writer);
        // should exceed, we have 2 docs at this point
        assertTrue(writer.exceededLimits().isPresent());
        assertEquals(
            "Documents limit exceeded: 2 > 1", writer.exceededLimits().orElseThrow().getMessage());

        writer.commit(EncodedUserData.EMPTY);
        writer.close();
      }

      // Close the index and re-open it
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer = getWriter(directory, analyzerRegistry, 30, 1);

        // Check that writer is initialized with correct number of docs
        assertEquals(
            "Documents limit exceeded: 2 > 1", writer.exceededLimits().orElseThrow().getMessage());

        // deleting all should clear the field names
        writer.deleteAll(EncodedUserData.EMPTY);
        assertTrue(writer.exceededLimits().isEmpty());
        writer.close();
      }
    }

    @Test
    public void testFieldLimitEnforcedPerDocument() throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        // writer with field limit of 3
        SingleLuceneIndexWriter writer =
            getWriter(directory, analyzerRegistry, 5, LuceneConfig.MAX_DOCS_LIMIT);

        // I can insert a document with less than 5 fields
        insertWithManyFields(writer, 1);
        assertEquals(1, writer.getNumDocs());

        assertThrows(FieldExceededLimitsException.class, () -> insertWithManyFields(writer, 6));

        // make sure we still have only one doc in the index
        assertEquals(1, writer.getNumDocs());

        // we should not be exceeding the limits, the offending document was rejected
        assertTrue(writer.exceededLimits().isEmpty());
      }
    }

    @Test
    public void testFacetIndexing() throws Exception {

      var indexFolder = TestUtils.getTempFolder();

      try (var indexDirectory = new MMapDirectory(indexFolder.getRoot().toPath())) {
        var analyzerRegistry = AnalyzerRegistryBuilder.empty();

        // create definition with 2 fields (flat string and nested array)
        var indexDefinition =
            SearchIndex.mockDefinitionBuilder()
                .mappings(
                    DocumentFieldDefinitionBuilder.builder()
                        .field(
                            "field1",
                            FieldDefinitionBuilder.builder()
                                .stringFacet(StringFacetFieldDefinitionBuilder.builder().build())
                                .build())
                        .field(
                            "nested",
                            FieldDefinitionBuilder.builder()
                                .document(
                                    DocumentFieldDefinitionBuilder.builder()
                                        .field(
                                            "field2",
                                            FieldDefinitionBuilder.builder()
                                                .stringFacet(
                                                    StringFacetFieldDefinitionBuilder.builder()
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        var writer =
            getWriter(
                indexDirectory,
                indexDefinition,
                IndexFormatVersion.CURRENT,
                analyzerRegistry,
                SearchIndex.mockIndexingMetricsUpdater(indexDefinition.getType()));

        var indexId = new ObjectId();

        var bsonDocument =
            BsonUtils.documentToRaw(
                new BsonDocument()
                    .append(indexId.toString(), new BsonDocument("_id", new BsonInt32(1)))
                    .append("field1", new BsonString("value1"))
                    .append(
                        "nested",
                        new BsonDocument()
                            .append(
                                "field2",
                                new BsonArray(
                                    List.of(
                                        new BsonString("value21"),
                                        new BsonString("value22"),
                                        new BsonInt32(23))))));

        // index document
        writer.updateIndex(
            DocumentEvent.createInsert(
                DocumentMetadata.fromMetadataNamespace(
                    Optional.of(BsonUtils.documentToRaw(bsonDocument)), indexId),
                bsonDocument));
        writer.commit(EncodedUserData.EMPTY);

        // assert that the document can be found and facets are calculated correctly
        try (var manager =
            LuceneSearcherManager.create(
                writer.getLuceneWriter(),
                new LuceneSearcherFactory(
                    indexDefinition,
                    false,
                    new QueryCacheProvider.DefaultQueryCacheProvider(),
                    Optional.empty(),
                    SearchIndex.mockQueryMetricsUpdater(indexDefinition.getType())),
                mockMetricsFactory())) {

          var searcher = manager.acquire();
          var query = new DrillDownQuery(new FacetsConfig());
          query.add("nested.field2", "value22");

          var collectorManager = new FacetsCollectorManager();
          var result = FacetsCollectorManager.search(searcher, query, 10, collectorManager);
          assertEquals(1, result.topDocs().totalHits.value());

          var state =
              new DefaultSortedSetDocValuesReaderState(
                  searcher.getIndexReader(), new FacetsConfig());
          var facets = new SortedSetDocValuesFacetCounts(state, result.facetsCollector());
          assertEquals(1, facets.getTopChildren(10, "field1").childCount);
          assertEquals(2, facets.getTopChildren(10, "nested.field2").childCount);
        }
      }
    }

    @Test
    public void testCancelMerges() throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
        SingleLuceneIndexWriter writer = getWriter(directory, analyzerRegistry);

        // Add some documents to trigger potential merges
        ObjectId indexId = new ObjectId();
        for (int i = 0; i < 100; i++) {
          RawBsonDocument document =
              BsonUtils.documentToRaw(
                  new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(i))));
          writer.updateIndex(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId),
                  document));
        }
        writer.commit(EncodedUserData.EMPTY);

        // Cancel any running or pending merges
        writer.cancelMerges();

        // Verify writer is still functional after cancellation
        assertEquals(100, writer.getNumDocs());

        writer.close();
      }
    }

    /**
     * Tests that cancelMerges() only affects the target index partition and does not block on or
     * cancel merges from other index partitions when using a shared merge scheduler.
     *
     * <p>This test verifies the per-index isolation of the cancelMerges() implementation by:
     *
     * <ol>
     *   <li>Creating a shared InstrumentedConcurrentMergeScheduler
     *   <li>Creating two writers with different index partitions using the same shared scheduler
     *   <li>Adding documents to both indices to trigger merges
     *   <li>Cancelling merges on one index
     *   <li>Verifying both writers remain functional and independent
     * </ol>
     */
    @Test
    public void testCancelMergesPerIndexIsolation() throws Exception {
      // Create a shared merge scheduler that will be used by both indices
      InstrumentedConcurrentMergeScheduler sharedScheduler =
          new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry());

      // Create two temporary directories for two separate indices
      TemporaryFolder tempFolder1 = TestUtils.getTempFolder();
      TemporaryFolder tempFolder2 = TestUtils.getTempFolder();

      try (Directory directory1 = new MMapDirectory(tempFolder1.getRoot().toPath());
          Directory directory2 = new MMapDirectory(tempFolder2.getRoot().toPath())) {

        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();

        // Create two different GenerationIds for two separate indices
        var generationId1 = MOCK_INDEX_GENERATION_ID;
        var generationId2 =
            new GenerationId(new ObjectId(), MOCK_INDEX_DEFINITION_GENERATION.generation());

        // Create two writers with the same shared scheduler but different index partitions
        // Writer 1 uses partition 0, Writer 2 uses partition 1
        SingleLuceneIndexWriter writer1 =
            SingleLuceneIndexWriter.createForSearchIndex(
                directory1,
                createMergeScheduler(sharedScheduler, generationId1, 0, 2, false, directory1),
                new TieredMergePolicy(),
                16D,
                Optional.empty(),
                Optional.empty(),
                LuceneAnalyzer.indexAnalyzer(MOCK_INDEX_DEFINITION, analyzerRegistry),
                MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                    MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion),
                SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType()),
                Optional.empty(),
                FeatureFlags.withDefaults().build(),
                DynamicFeatureFlagRegistry.empty(),
                false);

        SingleLuceneIndexWriter writer2 =
            SingleLuceneIndexWriter.createForSearchIndex(
                directory2,
                createMergeScheduler(sharedScheduler, generationId2, 1, 2, false, directory2),
                new TieredMergePolicy(),
                16D,
                Optional.empty(),
                Optional.empty(),
                LuceneAnalyzer.indexAnalyzer(MOCK_INDEX_DEFINITION, analyzerRegistry),
                MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                    MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion),
                SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType()),
                Optional.empty(),
                FeatureFlags.withDefaults().build(),
                DynamicFeatureFlagRegistry.empty(),
                false);

        // Add documents to both indices to trigger potential merges
        ObjectId indexId1 = new ObjectId();
        ObjectId indexId2 = new ObjectId();

        // Add documents to writer1
        for (int i = 0; i < 100; i++) {
          RawBsonDocument document =
              BsonUtils.documentToRaw(
                  new BsonDocument(indexId1.toString(), new BsonDocument("_id", new BsonInt32(i))));
          writer1.updateIndex(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId1),
                  document));
        }
        writer1.commit(EncodedUserData.EMPTY);

        // Add documents to writer2
        for (int i = 0; i < 100; i++) {
          RawBsonDocument document =
              BsonUtils.documentToRaw(
                  new BsonDocument(indexId2.toString(), new BsonDocument("_id", new BsonInt32(i))));
          writer2.updateIndex(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId2),
                  document));
        }
        writer2.commit(EncodedUserData.EMPTY);

        // Cancel merges on writer1 only
        writer1.cancelMerges();

        // Verify both writers are still functional after cancellation
        // This demonstrates that:
        // 1. writer1's cancelMerges() did not block on writer2's merges
        // 2. writer2's merges were not cancelled
        // 3. Both writers remain independent and operational
        assertEquals(100, writer1.getNumDocs());
        assertEquals(100, writer2.getNumDocs());

        // Add more documents to both writers to verify they're still functional
        for (int i = 100; i < 110; i++) {
          RawBsonDocument document1 =
              BsonUtils.documentToRaw(
                  new BsonDocument(indexId1.toString(), new BsonDocument("_id", new BsonInt32(i))));
          writer1.updateIndex(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromMetadataNamespace(Optional.of(document1), indexId1),
                  document1));

          RawBsonDocument document2 =
              BsonUtils.documentToRaw(
                  new BsonDocument(indexId2.toString(), new BsonDocument("_id", new BsonInt32(i))));
          writer2.updateIndex(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromMetadataNamespace(Optional.of(document2), indexId2),
                  document2));
        }

        writer1.commit(EncodedUserData.EMPTY);
        writer2.commit(EncodedUserData.EMPTY);

        // Verify final document counts
        assertEquals(110, writer1.getNumDocs());
        assertEquals(110, writer2.getNumDocs());

        // Clean up
        writer1.close();
        writer2.close();
      }
    }

    /**
     * Tests that cancelAllMerges() cancels merges across all indices during shutdown.
     *
     * <p>This test verifies the global shutdown behavior by:
     *
     * <ol>
     *   <li>Creating a shared InstrumentedConcurrentMergeScheduler
     *   <li>Creating three writers with different index partitions using the same shared scheduler
     *   <li>Adding documents to all indices to trigger merges
     *   <li>Calling cancelAllMerges() to simulate shutdown
     *   <li>Verifying all writers remain functional after cancellation
     * </ol>
     */
    @Test
    public void testCancelAllMergesOnShutdown() throws Exception {
      // Create a shared merge scheduler that will be used by multiple indices
      InstrumentedConcurrentMergeScheduler sharedScheduler =
          new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry());

      // Create three temporary directories for three separate indices
      TemporaryFolder tempFolder1 = TestUtils.getTempFolder();
      TemporaryFolder tempFolder2 = TestUtils.getTempFolder();
      TemporaryFolder tempFolder3 = TestUtils.getTempFolder();

      try (Directory directory1 = new MMapDirectory(tempFolder1.getRoot().toPath());
          Directory directory2 = new MMapDirectory(tempFolder2.getRoot().toPath());
          Directory directory3 = new MMapDirectory(tempFolder3.getRoot().toPath())) {

        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();

        // Create three different GenerationIds for three separate indices
        var generationId1 = MOCK_INDEX_GENERATION_ID;
        var generationId2 =
            new GenerationId(new ObjectId(), MOCK_INDEX_DEFINITION_GENERATION.generation());
        var generationId3 =
            new GenerationId(new ObjectId(), MOCK_INDEX_DEFINITION_GENERATION.generation());

        // Create three writers with the same shared scheduler but different index partitions
        SingleLuceneIndexWriter writer1 =
            SingleLuceneIndexWriter.createForSearchIndex(
                directory1,
                createMergeScheduler(sharedScheduler, generationId1, 0, 3, false, directory1),
                new TieredMergePolicy(),
                16D,
                Optional.empty(),
                Optional.empty(),
                LuceneAnalyzer.indexAnalyzer(MOCK_INDEX_DEFINITION, analyzerRegistry),
                MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                    MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion),
                SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType()),
                Optional.empty(),
                FeatureFlags.withDefaults().build(),
                DynamicFeatureFlagRegistry.empty(),
                false);

        SingleLuceneIndexWriter writer2 =
            SingleLuceneIndexWriter.createForSearchIndex(
                directory2,
                createMergeScheduler(sharedScheduler, generationId2, 1, 3, false, directory2),
                new TieredMergePolicy(),
                16D,
                Optional.empty(),
                Optional.empty(),
                LuceneAnalyzer.indexAnalyzer(MOCK_INDEX_DEFINITION, analyzerRegistry),
                MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                    MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion),
                SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType()),
                Optional.empty(),
                FeatureFlags.withDefaults().build(),
                DynamicFeatureFlagRegistry.empty(),
                false);

        SingleLuceneIndexWriter writer3 =
            SingleLuceneIndexWriter.createForSearchIndex(
                directory3,
                createMergeScheduler(sharedScheduler, generationId3, 2, 3, false, directory3),
                new TieredMergePolicy(),
                16D,
                Optional.empty(),
                Optional.empty(),
                LuceneAnalyzer.indexAnalyzer(MOCK_INDEX_DEFINITION, analyzerRegistry),
                MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                    MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion),
                SearchIndex.mockIndexingMetricsUpdater(MOCK_INDEX_DEFINITION.getType()),
                Optional.empty(),
                FeatureFlags.withDefaults().build(),
                DynamicFeatureFlagRegistry.empty(),
                false);

        // Add documents to all three indices to trigger potential merges
        ObjectId indexId1 = new ObjectId();
        ObjectId indexId2 = new ObjectId();
        ObjectId indexId3 = new ObjectId();

        // Add documents to writer1
        for (int i = 0; i < 100; i++) {
          RawBsonDocument document =
              BsonUtils.documentToRaw(
                  new BsonDocument(indexId1.toString(), new BsonDocument("_id", new BsonInt32(i))));
          writer1.updateIndex(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId1),
                  document));
        }
        writer1.commit(EncodedUserData.EMPTY);

        // Add documents to writer2
        for (int i = 0; i < 100; i++) {
          RawBsonDocument document =
              BsonUtils.documentToRaw(
                  new BsonDocument(indexId2.toString(), new BsonDocument("_id", new BsonInt32(i))));
          writer2.updateIndex(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId2),
                  document));
        }
        writer2.commit(EncodedUserData.EMPTY);

        // Add documents to writer3
        for (int i = 0; i < 100; i++) {
          RawBsonDocument document =
              BsonUtils.documentToRaw(
                  new BsonDocument(indexId3.toString(), new BsonDocument("_id", new BsonInt32(i))));
          writer3.updateIndex(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId3),
                  document));
        }
        writer3.commit(EncodedUserData.EMPTY);

        // Verify all writers have documents
        assertEquals(100, writer1.getNumDocs());
        assertEquals(100, writer2.getNumDocs());
        assertEquals(100, writer3.getNumDocs());

        // Simulate shutdown: cancel all merges across all indices
        sharedScheduler.cancelAllMerges();

        // After cancelAllMerges, all writers should still be functional
        // Add more documents to verify writers still work
        for (int i = 100; i < 110; i++) {
          RawBsonDocument document1 =
              BsonUtils.documentToRaw(
                  new BsonDocument(indexId1.toString(), new BsonDocument("_id", new BsonInt32(i))));
          writer1.updateIndex(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromMetadataNamespace(Optional.of(document1), indexId1),
                  document1));

          RawBsonDocument document2 =
              BsonUtils.documentToRaw(
                  new BsonDocument(indexId2.toString(), new BsonDocument("_id", new BsonInt32(i))));
          writer2.updateIndex(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromMetadataNamespace(Optional.of(document2), indexId2),
                  document2));

          RawBsonDocument document3 =
              BsonUtils.documentToRaw(
                  new BsonDocument(indexId3.toString(), new BsonDocument("_id", new BsonInt32(i))));
          writer3.updateIndex(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromMetadataNamespace(Optional.of(document3), indexId3),
                  document3));
        }

        // Verify final document counts
        assertEquals(110, writer1.getNumDocs());
        assertEquals(110, writer2.getNumDocs());
        assertEquals(110, writer3.getNumDocs());

        // Clean up
        writer1.close();
        writer2.close();
        writer3.close();
      }
    }

    /**
     * Tests that when the CANCEL_MERGE feature flag is DISABLED, the writer.close() does NOT cancel
     * merges.
     *
     * <p>This test verifies backward compatibility by ensuring that when the feature flag is
     * disabled, the old behavior (no merge cancellation during close) is preserved.
     */
    @Test
    public void testCloseWithCancelMergeFeatureFlagDisabled() throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();

        // Create a writer with the feature flag DISABLED (default behavior)
        FeatureFlags featureFlagsDisabled = FeatureFlags.withDefaults().build();
        SingleLuceneIndexWriter writer =
            SingleLuceneIndexWriter.createForSearchIndex(
                directory,
                createMergeScheduler(
                    new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
                    MOCK_INDEX_GENERATION_ID,
                    0,
                    1,
                    false,
                    directory),
                new TieredMergePolicy(),
                16D,
                Optional.empty(),
                Optional.empty(),
                LuceneAnalyzer.indexAnalyzer(MOCK_INDEX_DEFINITION, analyzerRegistry),
                MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
                SearchIndex.mockIndexingMetricsUpdater(IndexDefinition.Type.SEARCH),
                Optional.empty(),
                featureFlagsDisabled,
                DynamicFeatureFlagRegistry.empty(),
                false);

        // Add some documents to trigger potential merges
        ObjectId indexId = MOCK_INDEX_ID;
        for (int i = 0; i < 100; i++) {
          RawBsonDocument document =
              BsonUtils.documentToRaw(
                  new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(i))));
          writer.updateIndex(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId),
                  document));
        }
        writer.commit(EncodedUserData.EMPTY);

        // Verify writer has documents
        assertEquals(100, writer.getNumDocs());

        // Close the writer - with feature flag disabled, merges should NOT be cancelled
        // This should work without any issues (old behavior)
        writer.close();
      }
    }

    /**
     * Tests that when the CANCEL_MERGE feature flag is ENABLED, the writer.close() cancels merges.
     *
     * <p>This test verifies the new behavior by ensuring that when the feature flag is enabled,
     * merges are cancelled during close to avoid blocking on long-running merges.
     */
    @Test
    public void testCloseWithCancelMergeFeatureFlagEnabled() throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();

      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();

        // Create a writer with the feature flag ENABLED
        FeatureFlags featureFlagsEnabled =
            FeatureFlags.withDefaults()
                .enable(com.xgen.mongot.featureflag.Feature.CANCEL_MERGE)
                .build();
        SingleLuceneIndexWriter writer =
            SingleLuceneIndexWriter.createForSearchIndex(
                directory,
                createMergeScheduler(
                    new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
                    MOCK_INDEX_GENERATION_ID,
                    0,
                    1,
                    true,
                    directory),
                new TieredMergePolicy(),
                16D,
                Optional.empty(),
                Optional.empty(),
                LuceneAnalyzer.indexAnalyzer(MOCK_INDEX_DEFINITION, analyzerRegistry),
                MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
                SearchIndex.mockIndexingMetricsUpdater(IndexDefinition.Type.SEARCH),
                Optional.empty(),
                featureFlagsEnabled,
                DynamicFeatureFlagRegistry.empty(),
                false);

        // Add some documents to trigger potential merges
        ObjectId indexId = MOCK_INDEX_ID;
        for (int i = 0; i < 100; i++) {
          RawBsonDocument document =
              BsonUtils.documentToRaw(
                  new BsonDocument(indexId.toString(), new BsonDocument("_id", new BsonInt32(i))));
          writer.updateIndex(
              DocumentEvent.createInsert(
                  DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId),
                  document));
        }
        writer.commit(EncodedUserData.EMPTY);

        // Verify writer has documents
        assertEquals(100, writer.getNumDocs());

        // Close the writer - with feature flag enabled, merges should be cancelled
        // This should complete quickly without blocking on long-running merges
        writer.close();
      }
    }

    @Test
    public void testAutoEmbeddingDocumentIndexingWithVectorSideInput() throws Exception {
      // Create a temporary directory we can make an index in.
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
      // Create the lucene index and create a LuceneIndexWriter with it.
      try (Directory directory = new MMapDirectory(temporaryFolder.getRoot().toPath())) {
        SingleLuceneIndexWriter writer =
            getVectorIndexWriter(
                directory,
                VectorIndex.mockAutoEmbeddingVectorDefinition(MOCK_INDEX_ID),
                IndexFormatVersion.CURRENT);
        BsonDocument bsonDocument =
            new BsonDocument()
                .append(MOCK_INDEX_ID.toString(), new BsonDocument("_id", new BsonInt32(1)))
                .append("_id", new BsonInt32(1))
                .append("field", new BsonString("text to be vectorized"));
        var rawDocument = BsonUtils.documentToRaw(bsonDocument);
        DocumentEvent rawEvent =
            DocumentEvent.createInsert(
                DocumentMetadata.fromMetadataNamespace(Optional.of(rawDocument), MOCK_INDEX_ID),
                rawDocument);
        DocumentEvent autoEmbeddingDocumentEvent =
            DocumentEvent.createFromDocumentEventAndVectors(
                rawEvent,
                ImmutableMap.of(
                    FieldPath.parse("field"),
                    ImmutableMap.of("text to be vectorized", getMockVector(1024))));
        writer.updateIndex(autoEmbeddingDocumentEvent);
        writer.commit(EncodedUserData.EMPTY);
        assertAutoEmbeddingDocumentWithVectorIncluded(writer, bsonDocument, getMockVector(1024));
      }
    }

    private void insert(SingleLuceneIndexWriter writer) throws Exception {
      var indexId = new ObjectId();
      var document =
          BsonUtils.documentToRaw(
              new BsonDocument(
                  indexId.toString(), new BsonDocument("_id", new BsonObjectId(new ObjectId()))));
      writer.updateIndex(
          DocumentEvent.createInsert(
              DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document));
    }

    private void insertWithField(SingleLuceneIndexWriter writer, String fieldName)
        throws Exception {
      var id = new BsonObjectId();
      var indexId = MOCK_INDEX_DEFINITION.getIndexId();
      var document =
          BsonUtils.documentToRaw(
              new BsonDocument(fieldName, new BsonString("value"))
                  .append("_id", id)
                  .append(indexId.toString(), new BsonDocument("_id", id)));
      writer.updateIndex(
          DocumentEvent.createInsert(
              DocumentMetadata.fromMetadataNamespace(Optional.of(document), indexId), document));
    }

    private void insertWithManyFields(SingleLuceneIndexWriter writer, int numberOfFields)
        throws Exception {
      var indexId = new ObjectId();
      var document =
          new BsonDocument(
              indexId.toString(), new BsonDocument("_id", new BsonObjectId(new ObjectId())));
      IntStream.range(0, numberOfFields)
          .forEach(i -> document.append("field_" + i, new BsonString("value")));
      writer.updateIndex(
          DocumentEvent.createInsert(
              DocumentMetadata.fromMetadataNamespace(
                  Optional.of(BsonUtils.documentToRaw(document)), indexId),
              BsonUtils.documentToRaw(document)));
    }

    private Vector getMockVector(int numDimensions) {
      float[] floatArray = new float[numDimensions];
      for (int i = 1; i <= numDimensions; i++) {
        floatArray[i - 1] = (float) i;
      }
      return Vector.fromFloats(floatArray, FloatVector.OriginalType.NATIVE);
    }
  }

  @RunWith(Parameterized.class)
  public static class BloomFilterForIdFieldEnabledSupplierTests {

    private static final boolean ENABLE_NATURAL_ORDER_SCAN = true;
    private static final boolean DISABLE_NATURAL_ORDER_SCAN = false;
    private static final boolean BLOOM_DFF_ENABLED = true;
    private static final boolean BLOOM_DFF_DISABLED = false;
    private static final boolean EXPECTED_BLOOM_ENABLED = true;
    private static final boolean EXPECTED_BLOOM_DISABLED = false;

    private final boolean enableNaturalOrderScan;
    private final boolean bloomDffEnabled;
    private final boolean expectedBloom;

    public BloomFilterForIdFieldEnabledSupplierTests(
        boolean enableNaturalOrderScan, boolean bloomDffEnabled, boolean expectedBloom) {
      this.enableNaturalOrderScan = enableNaturalOrderScan;
      this.bloomDffEnabled = bloomDffEnabled;
      this.expectedBloom = expectedBloom;
    }

    @Parameters(
        name = "Natural order enabled : {0}, Bloom DFF enabled : {1}, Expected Bloom enabled : {2}")
    public static Collection<Object[]> data() {
      return Arrays.asList(
          new Object[] {
            DISABLE_NATURAL_ORDER_SCAN, BLOOM_DFF_DISABLED, EXPECTED_BLOOM_DISABLED,
          },
          new Object[] {
            DISABLE_NATURAL_ORDER_SCAN, BLOOM_DFF_ENABLED, EXPECTED_BLOOM_DISABLED,
          },
          new Object[] {
            ENABLE_NATURAL_ORDER_SCAN, BLOOM_DFF_DISABLED, EXPECTED_BLOOM_DISABLED,
          },
          new Object[] {
            ENABLE_NATURAL_ORDER_SCAN, BLOOM_DFF_ENABLED, EXPECTED_BLOOM_ENABLED,
          });
    }

    @Test
    public void getBloomFilterForIdFieldEnabledSupplier_nonAutoEmbedding() {
      DynamicFeatureFlagRegistry registry = mock(DynamicFeatureFlagRegistry.class);
      when(registry.evaluateClusterInvariant(DynamicFeatureFlags.BLOOM_FILTER_FOR_ID_FIELD))
          .thenReturn(this.bloomDffEnabled);
      BooleanSupplier supplier =
          BloomCodecPolicy.getBloomFilterEnabledForIdField(
              registry,
              this.enableNaturalOrderScan,
              SearchIndex.MOCK_INDEX_DEFINITION,
              IndexStatus::initialSync);
      assertThat(supplier.getAsBoolean()).isEqualTo(this.expectedBloom);
    }
  }
}
