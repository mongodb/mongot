package com.xgen.mongot.catalogservice.schema;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.status.SynonymStatus;
import com.xgen.mongot.index.synonym.SynonymDetailedStatus;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      IndexStatsEntryTest.DeserializationTest.class,
      IndexStatsEntryTest.SerializationTest.class
    })
public class IndexStatsEntryTest {
  private static final String RESOURCE_PATH = "src/test/unit/resources/catalogservice/schema";

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "index-stats-deserialization";
    private static final BsonDeserializationTestSuite<IndexStatsEntry> TEST_SUITE =
        fromDocument(RESOURCE_PATH, SUITE_NAME, IndexStatsEntry::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<IndexStatsEntry> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<IndexStatsEntry> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<IndexStatsEntry>> data() {
      return TEST_SUITE.withExamples(
          simpleSearchIndex(),
          simpleSearchIndexWithSynonyms(),
          simpleSearchIndexWithStaged(),
          simpleSearchIndexWithStagedAndSynonyms(),
          simpleVectorIndex(),
          simpleVectorIndexWithStaged(),
          searchIndexWithEmptyMainIndex(),
          vectorIndexWithEmptyMainIndex());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatsEntry> simpleSearchIndex() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple search index",
          createSearchIndex(
              new ObjectId("695301d3bb11192ef11c42f6"),
              new ObjectId("695301d3bb11192ef11c42f7"),
              false,
              false));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatsEntry>
        simpleSearchIndexWithSynonyms() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple search index with synonym",
          createSearchIndex(
              new ObjectId("695301d3bb11192ef11c42f6"),
              new ObjectId("695301d3bb11192ef11c42f7"),
              true,
              false));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatsEntry>
        simpleSearchIndexWithStaged() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple search index with staged index",
          createSearchIndex(
              new ObjectId("695301d3bb11192ef11c42f6"),
              new ObjectId("695301d3bb11192ef11c42f7"),
              false,
              true));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatsEntry>
        simpleSearchIndexWithStagedAndSynonyms() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple search index with synonyms and staged index",
          createSearchIndex(
              new ObjectId("695301d3bb11192ef11c42f6"),
              new ObjectId("695301d3bb11192ef11c42f7"),
              true,
              true));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatsEntry> simpleVectorIndex() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple vector index",
          createVectorSearchIndex(
              new ObjectId("695301d3bb11192ef11c42f6"),
              new ObjectId("695301d3bb11192ef11c42f7"),
              false));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatsEntry>
        simpleVectorIndexWithStaged() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple vector index with staged index",
          createVectorSearchIndex(
              new ObjectId("695301d3bb11192ef11c42f6"),
              new ObjectId("695301d3bb11192ef11c42f7"),
              true));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatsEntry>
        searchIndexWithEmptyMainIndex() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "search index with empty main index",
          new IndexStatsEntry(
              new IndexStatsEntry.IndexStatsKey(
                  new ObjectId("695301d3bb11192ef11c42f6"),
                  new ObjectId("695301d3bb11192ef11c42f7")),
              IndexDefinition.Type.SEARCH,
              Optional.empty(),
              Optional.empty()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatsEntry>
        vectorIndexWithEmptyMainIndex() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "vector index with empty main index",
          new IndexStatsEntry(
              new IndexStatsEntry.IndexStatsKey(
                  new ObjectId("695301d3bb11192ef11c42f6"),
                  new ObjectId("695301d3bb11192ef11c42f7")),
              IndexDefinition.Type.VECTOR_SEARCH,
              Optional.empty(),
              Optional.empty()));
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "index-stats-serialization";
    private static final BsonSerializationTestSuite<IndexStatsEntry> TEST_SUITE =
        fromEncodable(RESOURCE_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<IndexStatsEntry> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<IndexStatsEntry> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<IndexStatsEntry>> data() {
      return Arrays.asList(
          simpleSearchIndex(),
          simpleSearchIndexWithSynonyms(),
          simpleSearchIndexWithStaged(),
          simpleSearchIndexWithStagedAndSynonyms(),
          simpleVectorIndex(),
          simpleVectorIndexWithStaged(),
          searchIndexWithEmptyMainIndex(),
          vectorIndexWithEmptyMainIndex());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatsEntry> simpleSearchIndex() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple search index",
          createSearchIndex(
              new ObjectId("695301d3bb11192ef11c42f6"),
              new ObjectId("695301d3bb11192ef11c42f7"),
              false,
              false));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatsEntry>
        simpleSearchIndexWithSynonyms() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple search index with synonym",
          createSearchIndex(
              new ObjectId("695301d3bb11192ef11c42f6"),
              new ObjectId("695301d3bb11192ef11c42f7"),
              true,
              false));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatsEntry>
        simpleSearchIndexWithStaged() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple search index with staged index",
          createSearchIndex(
              new ObjectId("695301d3bb11192ef11c42f6"),
              new ObjectId("695301d3bb11192ef11c42f7"),
              false,
              true));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatsEntry>
        simpleSearchIndexWithStagedAndSynonyms() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple search index with synonyms and staged index",
          createSearchIndex(
              new ObjectId("695301d3bb11192ef11c42f6"),
              new ObjectId("695301d3bb11192ef11c42f7"),
              true,
              true));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatsEntry> simpleVectorIndex() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple vector index",
          createVectorSearchIndex(
              new ObjectId("695301d3bb11192ef11c42f6"),
              new ObjectId("695301d3bb11192ef11c42f7"),
              false));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatsEntry>
        simpleVectorIndexWithStaged() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple vector index with staged index",
          createVectorSearchIndex(
              new ObjectId("695301d3bb11192ef11c42f6"),
              new ObjectId("695301d3bb11192ef11c42f7"),
              true));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatsEntry>
        searchIndexWithEmptyMainIndex() {
      return BsonSerializationTestSuite.TestSpec.create(
          "search index with empty main index",
          new IndexStatsEntry(
              new IndexStatsEntry.IndexStatsKey(
                  new ObjectId("695301d3bb11192ef11c42f6"),
                  new ObjectId("695301d3bb11192ef11c42f7")),
              IndexDefinition.Type.SEARCH,
              Optional.empty(),
              Optional.empty()));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatsEntry>
        vectorIndexWithEmptyMainIndex() {
      return BsonSerializationTestSuite.TestSpec.create(
          "vector index with empty main index",
          new IndexStatsEntry(
              new IndexStatsEntry.IndexStatsKey(
                  new ObjectId("695301d3bb11192ef11c42f6"),
                  new ObjectId("695301d3bb11192ef11c42f7")),
              IndexDefinition.Type.VECTOR_SEARCH,
              Optional.empty(),
              Optional.empty()));
    }
  }

  private static IndexStatsEntry createSearchIndex(
      ObjectId serverId, ObjectId indexId, boolean withSynonyms, boolean withStaged) {
    return new IndexStatsEntry(
        new IndexStatsEntry.IndexStatsKey(serverId, indexId),
        IndexDefinition.Type.SEARCH,
        Optional.of(
            createDetailedSearchIndexStats(indexId, IndexStatusP.StatusCode.STEADY, withSynonyms)),
        Optional.ofNullable(
            withStaged
                ? createDetailedSearchIndexStats(
                    indexId, IndexStatusP.StatusCode.INITIAL_SYNC, withSynonyms)
                : null));
  }

  private static IndexStatsEntry.DetailedIndexStats createDetailedSearchIndexStats(
      ObjectId indexId, IndexStatusP.StatusCode status, boolean withSynonyms) {
    return new IndexStatsEntry.DetailedIndexStats(
        new IndexStatusP(status, Optional.empty(), Optional.empty(), Optional.empty()),
        SearchIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .name("index")
            .database("database")
            .lastObservedCollectionName("collection")
            .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .build(),
        Optional.ofNullable(
            withSynonyms
                ? Map.of(
                    "index1",
                    new SynonymDetailedStatus(SynonymStatus.INITIAL_SYNC, Optional.of("Syncing")))
                : null));
  }

  private static IndexStatsEntry createVectorSearchIndex(
      ObjectId serverId, ObjectId indexId, boolean withStaged) {
    return new IndexStatsEntry(
        new IndexStatsEntry.IndexStatsKey(serverId, indexId),
        IndexDefinition.Type.VECTOR_SEARCH,
        Optional.of(createDetailedVectorSearchIndex(indexId, IndexStatusP.StatusCode.STEADY)),
        Optional.ofNullable(
            withStaged
                ? createDetailedVectorSearchIndex(indexId, IndexStatusP.StatusCode.INITIAL_SYNC)
                : null));
  }

  private static IndexStatsEntry.DetailedIndexStats createDetailedVectorSearchIndex(
      ObjectId indexId, IndexStatusP.StatusCode status) {
    return new IndexStatsEntry.DetailedIndexStats(
        new IndexStatusP(status, Optional.empty(), Optional.empty(), Optional.empty()),
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .name("index")
            .database("database")
            .lastObservedCollectionName("collection")
            .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
            .withCosineVectorField("my.vector.field", 1)
            .withFilterPath("my.filter.field")
            .build(),
        Optional.empty());
  }
}
