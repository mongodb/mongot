package com.xgen.mongot.catalogservice.schema;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.status.SynonymStatus;
import com.xgen.mongot.index.synonym.SynonymDetailedStatus;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.Arrays;
import java.util.Collections;
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
      DetailedIndexStatsTest.DeserializationTest.class,
      DetailedIndexStatsTest.SerializationTest.class
    })
public class DetailedIndexStatsTest {

  private static final String RESOURCE_PATH = "src/test/unit/resources/catalogservice/schema";

  @RunWith(Parameterized.class)
  public static class DeserializationTest {
    private static final String SUITE_NAME = "detailed-index-stats-deserialization";
    private static final BsonDeserializationTestSuite<IndexStatsEntry.DetailedIndexStats>
        TEST_SUITE =
            fromDocument(RESOURCE_PATH, SUITE_NAME, IndexStatsEntry.DetailedIndexStats::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<IndexStatsEntry.DetailedIndexStats>
        testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<IndexStatsEntry.DetailedIndexStats> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<
            BsonDeserializationTestSuite.TestSpecWrapper<IndexStatsEntry.DetailedIndexStats>>
        data() {
      return TEST_SUITE.withExamples(
          simpleSearch(),
          simpleVector(),
          simpleSearchWithEmptySynonymStatusMap(),
          simpleSearchWithSynonymStatusMap());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatsEntry.DetailedIndexStats>
        simpleSearch() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple search index",
          new IndexStatsEntry.DetailedIndexStats(
              new IndexStatusP(
                  IndexStatusP.StatusCode.INITIAL_SYNC,
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()),
              createSearchIndexDef(),
              Optional.empty()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatsEntry.DetailedIndexStats>
        simpleVector() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple vector index",
          new IndexStatsEntry.DetailedIndexStats(
              new IndexStatusP(
                  IndexStatusP.StatusCode.INITIAL_SYNC,
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()),
              createVectorSearchDef(),
              Optional.empty()));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatsEntry.DetailedIndexStats>
        simpleSearchWithEmptySynonymStatusMap() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "search with empty synonym detailed status map",
          new IndexStatsEntry.DetailedIndexStats(
              new IndexStatusP(
                  IndexStatusP.StatusCode.INITIAL_SYNC,
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()),
              createSearchIndexDef(),
              Optional.of(Collections.emptyMap())));
    }

    private static BsonDeserializationTestSuite.ValidSpec<IndexStatsEntry.DetailedIndexStats>
        simpleSearchWithSynonymStatusMap() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "search with synonym detailed status map",
          new IndexStatsEntry.DetailedIndexStats(
              new IndexStatusP(
                  IndexStatusP.StatusCode.INITIAL_SYNC,
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()),
              createSearchIndexDef(),
              Optional.of(
                  Map.of(
                      "index1",
                      new SynonymDetailedStatus(SynonymStatus.INITIAL_SYNC, Optional.of("Syncing")),
                      "index2",
                      new SynonymDetailedStatus(SynonymStatus.INITIAL_SYNC, Optional.empty())))));
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {
    private static final String SUITE_NAME = "detailed-index-stats-serialization";
    private static final BsonSerializationTestSuite<IndexStatsEntry.DetailedIndexStats> TEST_SUITE =
        fromEncodable(RESOURCE_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<IndexStatsEntry.DetailedIndexStats> testSpec;

    public SerializationTest(
        BsonSerializationTestSuite.TestSpec<IndexStatsEntry.DetailedIndexStats> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<IndexStatsEntry.DetailedIndexStats>>
        data() {
      return Arrays.asList(
          simpleSearch(),
          simpleVector(),
          simpleSearchWithEmptySynonymStatusMap(),
          simpleSearchWithSynonymStatusMap());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatsEntry.DetailedIndexStats>
        simpleSearch() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple search index",
          new IndexStatsEntry.DetailedIndexStats(
              new IndexStatusP(
                  IndexStatusP.StatusCode.INITIAL_SYNC,
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()),
              createSearchIndexDef(),
              Optional.empty()));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatsEntry.DetailedIndexStats>
        simpleVector() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple vector index",
          new IndexStatsEntry.DetailedIndexStats(
              new IndexStatusP(
                  IndexStatusP.StatusCode.INITIAL_SYNC,
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()),
              createVectorSearchDef(),
              Optional.empty()));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatsEntry.DetailedIndexStats>
        simpleSearchWithEmptySynonymStatusMap() {
      return BsonSerializationTestSuite.TestSpec.create(
          "search with empty synonym detailed status map",
          new IndexStatsEntry.DetailedIndexStats(
              new IndexStatusP(
                  IndexStatusP.StatusCode.INITIAL_SYNC,
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()),
              createSearchIndexDef(),
              Optional.of(Collections.emptyMap())));
    }

    private static BsonSerializationTestSuite.TestSpec<IndexStatsEntry.DetailedIndexStats>
        simpleSearchWithSynonymStatusMap() {
      return BsonSerializationTestSuite.TestSpec.create(
          "search with synonym detailed status map",
          new IndexStatsEntry.DetailedIndexStats(
              new IndexStatusP(
                  IndexStatusP.StatusCode.INITIAL_SYNC,
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty()),
              createSearchIndexDef(),
              Optional.of(
                  Map.of(
                      "index1",
                      new SynonymDetailedStatus(SynonymStatus.INITIAL_SYNC, Optional.of("Syncing")),
                      "index2",
                      new SynonymDetailedStatus(SynonymStatus.INITIAL_SYNC, Optional.empty())))));
    }
  }

  private static SearchIndexDefinition createSearchIndexDef() {
    return SearchIndexDefinitionBuilder.builder()
        .indexId(new ObjectId("695301d3bb11192ef11c42f6"))
        .name("index")
        .database("database")
        .lastObservedCollectionName("collection")
        .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
        .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
        .build();
  }

  private static VectorIndexDefinition createVectorSearchDef() {
    return VectorIndexDefinitionBuilder.builder()
        .indexId(new ObjectId("695301d3bb11192ef11c42f6"))
        .name("index")
        .database("database")
        .lastObservedCollectionName("collection")
        .collectionUuid(UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa"))
        .withCosineVectorField("my.vector.field", 1)
        .withFilterPath("my.filter.field")
        .build();
  }
}
