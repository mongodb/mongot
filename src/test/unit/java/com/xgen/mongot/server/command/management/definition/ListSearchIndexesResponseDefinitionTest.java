package com.xgen.mongot.server.command.management.definition;

import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesResponseDefinition.Cursor;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesResponseDefinition.DefinitionVersion;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesResponseDefinition.HostIndexStatusDetail;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesResponseDefinition.HostStatusDetail;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesResponseDefinition.IndexEntry;
import com.xgen.mongot.server.command.management.definition.ListSearchIndexesResponseDefinition.SynonymMappingStatusDetail;
import com.xgen.mongot.server.command.management.definition.common.UserSearchIndexDefinition;
import com.xgen.mongot.server.command.management.definition.common.UserVectorIndexDefinition;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite.TestSpec;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.server.command.management.definition.UserSearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.server.command.management.definition.UserVectorIndexDefinitionBuilder;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ListSearchIndexesResponseDefinitionTest {
  private static final String RESOURCES_PATH =
      "src/test/unit/resources/server/command/management/definition/";
  private static final String SUITE_NAME = "list-search-indexes-response-serialization";
  private static final BsonSerializationTestSuite<ListSearchIndexesResponseDefinition> TEST_SUITE =
      fromEncodable(RESOURCES_PATH, SUITE_NAME);

  private final TestSpec<ListSearchIndexesResponseDefinition> testSpec;

  public ListSearchIndexesResponseDefinitionTest(
      TestSpec<ListSearchIndexesResponseDefinition> testSpec) {
    this.testSpec = testSpec;
  }

  /** Test data. */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<TestSpec<ListSearchIndexesResponseDefinition>> data() {
    return Arrays.asList(
        emptySearchIndexList(),
        singleSearchIndexMinimal(),
        singleSearchIndexComplete(),
        singleVectorIndexMinimal(),
        singleVectorIndexComplete(),
        multipleIndexesMixed(),
        searchIndexWithSynonyms(),
        vectorIndexWithStagedIndex(),
        searchIndexWithEmptyLists(),
        vectorIndexWithEmptyLists(),
        searchIndexWithMultipleHosts(),
        vectorIndexWithMultipleHosts(),
        searchIndexNotQueryable(),
        vectorIndexBuilding(),
        searchIndexWithStagedIndex(),
        hostWithOnlyStagedIndex());
  }

  @Test
  public void runTest() throws Exception {
    TEST_SUITE.runTest(this.testSpec);
  }

  private static TestSpec<ListSearchIndexesResponseDefinition> emptySearchIndexList() {
    return TestSpec.create(
        "emptySearchIndexList",
        new ListSearchIndexesResponseDefinition(
            1, new Cursor("myDatabase.myCollection", List.of())));
  }

  private static TestSpec<ListSearchIndexesResponseDefinition> singleSearchIndexMinimal() {
    Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
    DefinitionVersion defVersion = new DefinitionVersion(1, createdAt);

    UserSearchIndexDefinition searchDef =
        UserSearchIndexDefinitionBuilder.builder()
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true))
            .build();

    HostStatusDetail hostStatus =
        new HostStatusDetail("host1", "READY", true, Optional.empty(), Optional.empty());

    IndexEntry indexEntry =
        new IndexEntry(
            new ObjectId("507f191e810c19729de860ea"),
            "searchIndex1",
            "READY",
            true,
            defVersion,
            searchDef.toBson(),
            List.of(hostStatus),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    return TestSpec.create(
        "singleSearchIndexMinimal",
        new ListSearchIndexesResponseDefinition(
            1, new Cursor("myDatabase.myCollection", List.of(indexEntry.toBson()))));
  }

  private static TestSpec<ListSearchIndexesResponseDefinition> singleSearchIndexComplete() {
    Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
    DefinitionVersion defVersion = new DefinitionVersion(1, createdAt);

    UserSearchIndexDefinition searchDef =
        UserSearchIndexDefinitionBuilder.builder()
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true))
            .analyzer("lucene.standard")
            .searchAnalyzer("lucene.english")
            .build();

    SynonymMappingStatusDetail synonymDetail =
        new SynonymMappingStatusDetail("READY", true, Optional.of("Synonym mapping is ready"));

    HostIndexStatusDetail mainIndexDetail =
        new HostIndexStatusDetail(
            "READY",
            true,
            Optional.of("READY"),
            Optional.of(List.of(synonymDetail)),
            defVersion,
            searchDef,
            Optional.of("Index is ready"));

    HostStatusDetail hostStatus =
        new HostStatusDetail(
            "host1", "READY", true, Optional.of(mainIndexDetail), Optional.empty());

    IndexEntry indexEntry =
        new IndexEntry(
            new ObjectId("507f191e810c19729de860ea"),
            "searchIndex1",
            "READY",
            true,
            defVersion,
            searchDef.toBson(),
            List.of(hostStatus),
            Optional.of("READY"),
            Optional.of(List.of(synonymDetail)),
            Optional.of(12345L));

    return TestSpec.create(
        "singleSearchIndexComplete",
        new ListSearchIndexesResponseDefinition(
            1, new Cursor("myDatabase.myCollection", List.of(indexEntry.toBson()))));
  }

  private static TestSpec<ListSearchIndexesResponseDefinition> singleVectorIndexMinimal() {
    Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
    DefinitionVersion defVersion = new DefinitionVersion(1, createdAt);

    UserVectorIndexDefinition vectorDef =
        UserVectorIndexDefinitionBuilder.builder()
            .addVectorField(128, VectorSimilarity.COSINE, VectorQuantization.NONE, "embedding")
            .build();

    HostStatusDetail hostStatus =
        new HostStatusDetail("host1", "READY", true, Optional.empty(), Optional.empty());

    IndexEntry indexEntry =
        new IndexEntry(
            new ObjectId("507f191e810c19729de860ea"),
            "vectorIndex1",
            "READY",
            true,
            defVersion,
            vectorDef.toBson(),
            List.of(hostStatus),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    return TestSpec.create(
        "singleVectorIndexMinimal",
        new ListSearchIndexesResponseDefinition(
            1, new Cursor("myDatabase.myCollection", List.of(indexEntry.toBson()))));
  }

  private static TestSpec<ListSearchIndexesResponseDefinition> singleVectorIndexComplete() {
    Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
    DefinitionVersion defVersion = new DefinitionVersion(1, createdAt);

    UserVectorIndexDefinition vectorDef =
        UserVectorIndexDefinitionBuilder.builder()
            .addVectorField(
                2048, VectorSimilarity.EUCLIDEAN, VectorQuantization.SCALAR, "my.vector.field")
            .addFilterField("my.filter.field")
            .build();

    HostIndexStatusDetail mainIndexDetail =
        new HostIndexStatusDetail(
            "READY",
            true,
            Optional.empty(),
            Optional.empty(),
            defVersion,
            vectorDef,
            Optional.of("Vector index is ready"));

    HostStatusDetail hostStatus =
        new HostStatusDetail(
            "host1", "READY", true, Optional.of(mainIndexDetail), Optional.empty());

    IndexEntry indexEntry =
        new IndexEntry(
            new ObjectId("507f191e810c19729de860ea"),
            "vectorIndex1",
            "READY",
            true,
            defVersion,
            vectorDef.toBson(),
            List.of(hostStatus),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    return TestSpec.create(
        "singleVectorIndexComplete",
        new ListSearchIndexesResponseDefinition(
            1, new Cursor("myDatabase.myCollection", List.of(indexEntry.toBson()))));
  }

  private static TestSpec<ListSearchIndexesResponseDefinition> multipleIndexesMixed() {
    Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
    DefinitionVersion defVersion = new DefinitionVersion(1, createdAt);

    UserSearchIndexDefinition searchDef =
        UserSearchIndexDefinitionBuilder.builder()
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true))
            .build();

    UserVectorIndexDefinition vectorDef =
        UserVectorIndexDefinitionBuilder.builder()
            .addVectorField(256, VectorSimilarity.COSINE, VectorQuantization.NONE, "embedding")
            .build();

    HostStatusDetail hostStatus1 =
        new HostStatusDetail("host1", "READY", true, Optional.empty(), Optional.empty());

    HostStatusDetail hostStatus2 =
        new HostStatusDetail("host2", "READY", true, Optional.empty(), Optional.empty());

    IndexEntry searchEntry =
        new IndexEntry(
            new ObjectId("507f191e810c19729de860ea"),
            "searchIndex1",
            "READY",
            true,
            defVersion,
            searchDef.toBson(),
            List.of(hostStatus1),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    IndexEntry vectorEntry =
        new IndexEntry(
            new ObjectId("507f191e810c19729de860eb"),
            "vectorIndex1",
            "READY",
            true,
            defVersion,
            vectorDef.toBson(),
            List.of(hostStatus2),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    return TestSpec.create(
        "multipleIndexesMixed",
        new ListSearchIndexesResponseDefinition(
            1,
            new Cursor(
                "myDatabase.myCollection", List.of(searchEntry.toBson(), vectorEntry.toBson()))));
  }

  private static TestSpec<ListSearchIndexesResponseDefinition> searchIndexWithSynonyms() {
    Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
    DefinitionVersion defVersion = new DefinitionVersion(1, createdAt);

    UserSearchIndexDefinition searchDef =
        UserSearchIndexDefinitionBuilder.builder()
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true))
            .build();

    SynonymMappingStatusDetail synonymDetail1 =
        new SynonymMappingStatusDetail("READY", true, Optional.empty());

    SynonymMappingStatusDetail synonymDetail2 =
        new SynonymMappingStatusDetail("PENDING", false, Optional.of("Loading synonyms"));

    HostIndexStatusDetail mainIndexDetail =
        new HostIndexStatusDetail(
            "READY",
            true,
            Optional.of("READY"),
            Optional.of(List.of(synonymDetail1, synonymDetail2)),
            defVersion,
            searchDef,
            Optional.empty());

    HostStatusDetail hostStatus =
        new HostStatusDetail(
            "host1", "READY", true, Optional.of(mainIndexDetail), Optional.empty());

    IndexEntry indexEntry =
        new IndexEntry(
            new ObjectId("507f191e810c19729de860ea"),
            "searchIndex1",
            "READY",
            true,
            defVersion,
            searchDef.toBson(),
            List.of(hostStatus),
            Optional.of("READY"),
            Optional.of(List.of(synonymDetail1, synonymDetail2)),
            Optional.empty());

    return TestSpec.create(
        "searchIndexWithSynonyms",
        new ListSearchIndexesResponseDefinition(
            1, new Cursor("myDatabase.myCollection", List.of(indexEntry.toBson()))));
  }

  private static TestSpec<ListSearchIndexesResponseDefinition> vectorIndexWithStagedIndex() {
    Instant createdAt1 = Instant.parse("2024-01-01T00:00:00Z");
    Instant createdAt2 = Instant.parse("2024-01-02T00:00:00Z");
    DefinitionVersion defVersion1 = new DefinitionVersion(1, createdAt1);
    DefinitionVersion defVersion2 = new DefinitionVersion(2, createdAt2);

    UserVectorIndexDefinition vectorDef1 =
        UserVectorIndexDefinitionBuilder.builder()
            .addVectorField(128, VectorSimilarity.COSINE, VectorQuantization.NONE, "embedding")
            .build();

    UserVectorIndexDefinition vectorDef2 =
        UserVectorIndexDefinitionBuilder.builder()
            .addVectorField(256, VectorSimilarity.EUCLIDEAN, VectorQuantization.SCALAR, "embedding")
            .build();

    HostIndexStatusDetail mainIndexDetail =
        new HostIndexStatusDetail(
            "READY",
            true,
            Optional.empty(),
            Optional.empty(),
            defVersion1,
            vectorDef1,
            Optional.empty());

    HostIndexStatusDetail stagedIndexDetail =
        new HostIndexStatusDetail(
            "BUILDING",
            false,
            Optional.empty(),
            Optional.empty(),
            defVersion2,
            vectorDef2,
            Optional.of("Building new index version"));

    HostStatusDetail hostStatus =
        new HostStatusDetail(
            "host1", "READY", true, Optional.of(mainIndexDetail), Optional.of(stagedIndexDetail));

    IndexEntry indexEntry =
        new IndexEntry(
            new ObjectId("507f191e810c19729de860ea"),
            "vectorIndex1",
            "READY",
            true,
            defVersion1,
            vectorDef1.toBson(),
            List.of(hostStatus),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    return TestSpec.create(
        "vectorIndexWithStagedIndex",
        new ListSearchIndexesResponseDefinition(
            1, new Cursor("myDatabase.myCollection", List.of(indexEntry.toBson()))));
  }

  private static TestSpec<ListSearchIndexesResponseDefinition> searchIndexWithEmptyLists() {
    Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
    DefinitionVersion defVersion = new DefinitionVersion(1, createdAt);

    UserSearchIndexDefinition searchDef =
        UserSearchIndexDefinitionBuilder.builder()
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true))
            .build();

    IndexEntry indexEntry =
        new IndexEntry(
            new ObjectId("507f191e810c19729de860ea"),
            "searchIndex1",
            "READY",
            true,
            defVersion,
            searchDef.toBson(),
            List.of(),
            Optional.of("READY"),
            Optional.of(List.of()),
            Optional.empty());

    return TestSpec.create(
        "searchIndexWithEmptyLists",
        new ListSearchIndexesResponseDefinition(
            1, new Cursor("myDatabase.myCollection", List.of(indexEntry.toBson()))));
  }

  private static TestSpec<ListSearchIndexesResponseDefinition> vectorIndexWithEmptyLists() {
    Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
    DefinitionVersion defVersion = new DefinitionVersion(1, createdAt);

    UserVectorIndexDefinition vectorDef =
        UserVectorIndexDefinitionBuilder.builder()
            .addVectorField(128, VectorSimilarity.COSINE, VectorQuantization.NONE, "embedding")
            .build();

    IndexEntry indexEntry =
        new IndexEntry(
            new ObjectId("507f191e810c19729de860ea"),
            "vectorIndex1",
            "READY",
            true,
            defVersion,
            vectorDef.toBson(),
            List.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    return TestSpec.create(
        "vectorIndexWithEmptyLists",
        new ListSearchIndexesResponseDefinition(
            1, new Cursor("myDatabase.myCollection", List.of(indexEntry.toBson()))));
  }

  private static TestSpec<ListSearchIndexesResponseDefinition> searchIndexWithMultipleHosts() {
    Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
    DefinitionVersion defVersion = new DefinitionVersion(1, createdAt);

    UserSearchIndexDefinition searchDef =
        UserSearchIndexDefinitionBuilder.builder()
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true))
            .build();

    HostStatusDetail hostStatus1 =
        new HostStatusDetail("host1", "READY", true, Optional.empty(), Optional.empty());

    HostStatusDetail hostStatus2 =
        new HostStatusDetail("host2", "READY", true, Optional.empty(), Optional.empty());

    HostStatusDetail hostStatus3 =
        new HostStatusDetail("host3", "BUILDING", false, Optional.empty(), Optional.empty());

    IndexEntry indexEntry =
        new IndexEntry(
            new ObjectId("507f191e810c19729de860ea"),
            "searchIndex1",
            "READY",
            true,
            defVersion,
            searchDef.toBson(),
            List.of(hostStatus1, hostStatus2, hostStatus3),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    return TestSpec.create(
        "searchIndexWithMultipleHosts",
        new ListSearchIndexesResponseDefinition(
            1, new Cursor("myDatabase.myCollection", List.of(indexEntry.toBson()))));
  }

  private static TestSpec<ListSearchIndexesResponseDefinition> vectorIndexWithMultipleHosts() {
    Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
    DefinitionVersion defVersion = new DefinitionVersion(1, createdAt);

    UserVectorIndexDefinition vectorDef =
        UserVectorIndexDefinitionBuilder.builder()
            .addVectorField(128, VectorSimilarity.COSINE, VectorQuantization.NONE, "embedding")
            .build();

    HostIndexStatusDetail mainIndexDetail =
        new HostIndexStatusDetail(
            "READY",
            true,
            Optional.empty(),
            Optional.empty(),
            defVersion,
            vectorDef,
            Optional.empty());

    HostStatusDetail hostStatus1 =
        new HostStatusDetail(
            "host1", "READY", true, Optional.of(mainIndexDetail), Optional.empty());

    HostStatusDetail hostStatus2 =
        new HostStatusDetail("host2", "READY", true, Optional.empty(), Optional.empty());

    IndexEntry indexEntry =
        new IndexEntry(
            new ObjectId("507f191e810c19729de860ea"),
            "vectorIndex1",
            "READY",
            true,
            defVersion,
            vectorDef.toBson(),
            List.of(hostStatus1, hostStatus2),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    return TestSpec.create(
        "vectorIndexWithMultipleHosts",
        new ListSearchIndexesResponseDefinition(
            1, new Cursor("myDatabase.myCollection", List.of(indexEntry.toBson()))));
  }

  private static TestSpec<ListSearchIndexesResponseDefinition> searchIndexNotQueryable() {
    Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
    DefinitionVersion defVersion = new DefinitionVersion(1, createdAt);

    UserSearchIndexDefinition searchDef =
        UserSearchIndexDefinitionBuilder.builder()
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true))
            .build();

    HostIndexStatusDetail mainIndexDetail =
        new HostIndexStatusDetail(
            "PENDING",
            false,
            Optional.empty(),
            Optional.empty(),
            defVersion,
            searchDef,
            Optional.of("Index is being built"));

    HostStatusDetail hostStatus =
        new HostStatusDetail(
            "host1", "PENDING", false, Optional.of(mainIndexDetail), Optional.empty());

    IndexEntry indexEntry =
        new IndexEntry(
            new ObjectId("507f191e810c19729de860ea"),
            "searchIndex1",
            "PENDING",
            false,
            defVersion,
            searchDef.toBson(),
            List.of(hostStatus),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    return TestSpec.create(
        "searchIndexNotQueryable",
        new ListSearchIndexesResponseDefinition(
            1, new Cursor("myDatabase.myCollection", List.of(indexEntry.toBson()))));
  }

  private static TestSpec<ListSearchIndexesResponseDefinition> vectorIndexBuilding() {
    Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
    DefinitionVersion defVersion = new DefinitionVersion(1, createdAt);

    UserVectorIndexDefinition vectorDef =
        UserVectorIndexDefinitionBuilder.builder()
            .addVectorField(256, VectorSimilarity.EUCLIDEAN, VectorQuantization.SCALAR, "embedding")
            .build();

    HostIndexStatusDetail mainIndexDetail =
        new HostIndexStatusDetail(
            "BUILDING",
            false,
            Optional.empty(),
            Optional.empty(),
            defVersion,
            vectorDef,
            Optional.of("Building index"));

    HostStatusDetail hostStatus =
        new HostStatusDetail(
            "host1", "BUILDING", false, Optional.of(mainIndexDetail), Optional.empty());

    IndexEntry indexEntry =
        new IndexEntry(
            new ObjectId("507f191e810c19729de860ea"),
            "vectorIndex1",
            "BUILDING",
            false,
            defVersion,
            vectorDef.toBson(),
            List.of(hostStatus),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    return TestSpec.create(
        "vectorIndexBuilding",
        new ListSearchIndexesResponseDefinition(
            1, new Cursor("myDatabase.myCollection", List.of(indexEntry.toBson()))));
  }

  private static TestSpec<ListSearchIndexesResponseDefinition> searchIndexWithStagedIndex() {
    Instant createdAt1 = Instant.parse("2024-01-01T00:00:00Z");
    Instant createdAt2 = Instant.parse("2024-01-02T00:00:00Z");
    DefinitionVersion defVersion1 = new DefinitionVersion(1, createdAt1);
    DefinitionVersion defVersion2 = new DefinitionVersion(2, createdAt2);

    UserSearchIndexDefinition searchDef1 =
        UserSearchIndexDefinitionBuilder.builder()
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true))
            .build();

    UserSearchIndexDefinition searchDef2 =
        UserSearchIndexDefinitionBuilder.builder()
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true))
            .analyzer("lucene.standard")
            .build();

    HostIndexStatusDetail mainIndexDetail =
        new HostIndexStatusDetail(
            "READY",
            true,
            Optional.empty(),
            Optional.empty(),
            defVersion1,
            searchDef1,
            Optional.empty());

    HostIndexStatusDetail stagedIndexDetail =
        new HostIndexStatusDetail(
            "BUILDING",
            false,
            Optional.empty(),
            Optional.empty(),
            defVersion2,
            searchDef2,
            Optional.of("Building new index version"));

    HostStatusDetail hostStatus =
        new HostStatusDetail(
            "host1", "READY", true, Optional.of(mainIndexDetail), Optional.of(stagedIndexDetail));

    IndexEntry indexEntry =
        new IndexEntry(
            new ObjectId("507f191e810c19729de860ea"),
            "searchIndex1",
            "READY",
            true,
            defVersion1,
            searchDef1.toBson(),
            List.of(hostStatus),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    return TestSpec.create(
        "searchIndexWithStagedIndex",
        new ListSearchIndexesResponseDefinition(
            1, new Cursor("myDatabase.myCollection", List.of(indexEntry.toBson()))));
  }

  private static TestSpec<ListSearchIndexesResponseDefinition> hostWithOnlyStagedIndex() {
    Instant createdAt = Instant.parse("2024-01-01T00:00:00Z");
    DefinitionVersion defVersion = new DefinitionVersion(1, createdAt);

    UserVectorIndexDefinition vectorDef =
        UserVectorIndexDefinitionBuilder.builder()
            .addVectorField(128, VectorSimilarity.COSINE, VectorQuantization.NONE, "embedding")
            .build();

    // When mainIndex is Optional.empty(), the mapper returns UNKNOWN status (maps to PENDING)
    HostIndexStatusDetail stagedIndexDetail =
        new HostIndexStatusDetail(
            "BUILDING",
            false,
            Optional.empty(),
            Optional.empty(),
            defVersion,
            vectorDef,
            Optional.of("Building new index"));

    HostStatusDetail hostStatus =
        new HostStatusDetail(
            "host1", "PENDING", false, Optional.empty(), Optional.of(stagedIndexDetail));

    IndexEntry indexEntry =
        new IndexEntry(
            new ObjectId("507f191e810c19729de860ea"),
            "vectorIndex1",
            "PENDING",
            false,
            defVersion,
            vectorDef.toBson(),
            List.of(hostStatus),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    return TestSpec.create(
        "hostWithOnlyStagedIndex",
        new ListSearchIndexesResponseDefinition(
            1, new Cursor("myDatabase.myCollection", List.of(indexEntry.toBson()))));
  }
}
