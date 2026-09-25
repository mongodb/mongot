package com.xgen.mongot.config.backup;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;

import com.xgen.mongot.config.util.Invariants;
import com.xgen.mongot.index.definition.AnalyzerBoundSearchIndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.util.FileUtils;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.JsonTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.config.backup.ConfigJournalV1Builder;
import com.xgen.testing.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.AnalyzerBoundSearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionGenerationBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      ConfigJournalV1Test.DeserializationTest.class,
      ConfigJournalV1Test.InvariantTest.class,
      ConfigJournalV1Test.SerializationTest.class,
      ConfigJournalV1Test.ClassTest.class,
    })
public class ConfigJournalV1Test {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {

    private static final String SUITE_NAME = "config-journal-v1-deserialization";
    private static final BsonDeserializationTestSuite<ConfigJournalV1> TEST_SUITE =
        fromDocument(
            "src/test/unit/resources/config/backup/", SUITE_NAME, ConfigJournalV1::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<ConfigJournalV1> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<ConfigJournalV1> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<ConfigJournalV1>> data() {
      return TEST_SUITE.withExamples(
          empty(),
          withAnalyzer(),
          withStagedIndex(),
          withIndex(),
          withDeletedIndex(),
          multipleValues(),
          withVectorIndex(),
          withVectorIndexAndNoQuantizationField(),
          withStagedVectorIndex(),
          withDeletedVectorIndex(),
          multipleValuesVector());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<ConfigJournalV1> empty() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "empty", ConfigJournalV1Builder.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<ConfigJournalV1> withIndex() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with index",
          ConfigJournalV1Builder.builder()
              .liveIndex(searchDefinition("012345678901234567890123", "foo", 1, 2))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<ConfigJournalV1> withVectorIndex() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with vector index",
          ConfigJournalV1Builder.builder()
              .liveIndex(
                  new VectorIndexDefinitionGeneration(
                      VectorIndexDefinitionBuilder.builder()
                          .setFields(VectorIndex.MOCK_VECTOR_DEFINITION.getFields())
                          .build(),
                      Generation.CURRENT))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<ConfigJournalV1>
        withVectorIndexAndNoQuantizationField() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with vector index and absent quantization field",
          ConfigJournalV1Builder.builder()
              .liveIndex(
                  new VectorIndexDefinitionGeneration(
                      VectorIndexDefinitionBuilder.builder()
                          .setFields(VectorIndex.MOCK_VECTOR_DEFINITION.getFields())
                          .build(),
                      Generation.CURRENT))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<ConfigJournalV1> withAnalyzer() {
      // TODO(CLOUDP-72963): This test can be removed once overridden analyzers are gone
      AnalyzerBoundSearchIndexDefinition withAnalyzer =
          AnalyzerBoundSearchIndexDefinitionBuilder.builder()
              .index(
                  SearchIndexDefinitionBuilder.builder()
                      .indexId(new ObjectId("012345678901234567890123"))
                      .name("foo")
                      .database("bar")
                      .collectionUuid(UUID.fromString("00000000-1111-2222-3333-444444444444"))
                      .lastObservedCollectionName("baz")
                      .dynamicMapping()
                      .analyzerName("foo")
                      .build())
              .analyzer(
                  OverriddenBaseAnalyzerDefinitionBuilder.builder()
                      .name("foo")
                      .baseAnalyzerName("bar")
                      .build())
              .build();

      return BsonDeserializationTestSuite.TestSpec.valid(
          "with analyzer",
          ConfigJournalV1Builder.builder()
              .liveIndex(
                  SearchIndexDefinitionGenerationBuilder.builder()
                      .definition(withAnalyzer)
                      .generation(4, 5)
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<ConfigJournalV1> withStagedIndex() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with staged index",
          ConfigJournalV1Builder.builder()
              .liveIndex(searchDefinition("012345678901234567890123", "foo", 0, 2))
              .stagedIndex(searchDefinition("012345678901234567890123", "foo", 1, 2))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<ConfigJournalV1> withStagedVectorIndex() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with staged vector index",
          ConfigJournalV1Builder.builder()
              .liveIndex(
                  new VectorIndexDefinitionGeneration(
                      VectorIndexDefinitionBuilder.builder()
                          .setFields(VectorIndex.MOCK_VECTOR_DEFINITION.getFields())
                          .build(),
                      Generation.FIRST))
              .stagedIndex(
                  new VectorIndexDefinitionGeneration(
                      VectorIndexDefinitionBuilder.builder()
                          .setFields(VectorIndex.MOCK_VECTOR_DEFINITION.getFields())
                          .build(),
                      Generation.CURRENT))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<ConfigJournalV1> withDeletedIndex() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with deletedIndex",
          ConfigJournalV1Builder.builder()
              .deletedIndex(searchDefinition("012345678901234567890123", "foo", 1, 2))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<ConfigJournalV1>
        withDeletedVectorIndex() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with deleted vector index",
          ConfigJournalV1Builder.builder()
              .deletedIndex(
                  new VectorIndexDefinitionGeneration(
                      VectorIndexDefinitionBuilder.builder()
                          .setFields(VectorIndex.MOCK_VECTOR_DEFINITION.getFields())
                          .build(),
                      Generation.CURRENT))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<ConfigJournalV1> multipleValues() {
      // "foo" indexId ends with 3 and "baz" indexId ends with 4
      // only changes between staged/indexes/deleted is that the generation information is changed
      // its the same collection for all indexes
      return BsonDeserializationTestSuite.TestSpec.valid(
          "multiple values",
          ConfigJournalV1Builder.builder()
              .stagedIndex(searchDefinition("012345678901234567890123", "foo", 5, 6))
              .stagedIndex(searchDefinition("012345678901234567890124", "baz", 5, 6))
              .liveIndex(searchDefinition("012345678901234567890123", "foo", 3, 4))
              .liveIndex(searchDefinition("012345678901234567890124", "baz", 3, 4))
              .deletedIndex(searchDefinition("012345678901234567890123", "foo", 1, 2))
              .deletedIndex(searchDefinition("012345678901234567890124", "baz", 1, 2))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<ConfigJournalV1> multipleValuesVector() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "multiple vector values",
          ConfigJournalV1Builder.builder()
              .stagedIndex(
                  new VectorIndexDefinitionGeneration(
                      VectorIndexDefinitionBuilder.builder()
                          .setFields(VectorIndex.MOCK_VECTOR_DEFINITION.getFields())
                          .build(),
                      new Generation(UserIndexVersion.FIRST, IndexFormatVersion.CURRENT)
                          .incrementUser()))
              .liveIndex(
                  new VectorIndexDefinitionGeneration(
                      VectorIndexDefinitionBuilder.builder()
                          .setFields(VectorIndex.MOCK_VECTOR_DEFINITION.getFields())
                          .build(),
                      new Generation(UserIndexVersion.FIRST, IndexFormatVersion.FIVE)))
              .deletedIndex(
                  new VectorIndexDefinitionGeneration(
                      VectorIndexDefinitionBuilder.builder()
                          .setFields(VectorIndex.MOCK_VECTOR_DEFINITION.getFields())
                          .build(),
                      Generation.FIRST.incrementUser()))
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {

    private static final String SUITE_NAME = "config-journal-v1-serialization";
    private static final BsonSerializationTestSuite<ConfigJournalV1> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable(
            "src/test/unit/resources/config/backup/", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<ConfigJournalV1> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<ConfigJournalV1> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<ConfigJournalV1>> data() {
      return Arrays.asList(
          empty(),
          withOverriddenAnalyzer(),
          withStagedIndex(),
          withIndex(),
          withDeletedIndex(),
          withStagedVectorIndex(),
          withVectorIndex(),
          withDeletedVectorIndex(),
          multipleValues(),
          multipleVectorValues());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<ConfigJournalV1> empty() {
      return BsonSerializationTestSuite.TestSpec.create(
          "empty", ConfigJournalV1Builder.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<ConfigJournalV1> withOverriddenAnalyzer() {
      AnalyzerBoundSearchIndexDefinition withAnalyzer =
          AnalyzerBoundSearchIndexDefinitionBuilder.builder()
              .index(
                  SearchIndexDefinitionBuilder.builder()
                      .indexId(new ObjectId("012345678901234567890123"))
                      .name("foo")
                      .database("bar")
                      .collectionUuid(UUID.fromString("00000000-1111-2222-3333-444444444444"))
                      .lastObservedCollectionName("baz")
                      .dynamicMapping()
                      .analyzerName("foo")
                      .build())
              .analyzer(
                  OverriddenBaseAnalyzerDefinitionBuilder.builder()
                      .name("foo")
                      .baseAnalyzerName("bar")
                      .build())
              .build();

      return BsonSerializationTestSuite.TestSpec.create(
          "with overridden analyzer",
          ConfigJournalV1Builder.builder()
              .liveIndex(
                  SearchIndexDefinitionGenerationBuilder.builder()
                      .definition(withAnalyzer)
                      .generation(1, 2)
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<ConfigJournalV1> withStagedIndex() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with staged index",
          ConfigJournalV1Builder.builder()
              .stagedIndex(searchDefinition("012345678901234567890123", "foo", 1, 2))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<ConfigJournalV1> withIndex() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with index",
          ConfigJournalV1Builder.builder()
              .liveIndex(searchDefinition("012345678901234567890123", "foo", 1, 2))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<ConfigJournalV1> withDeletedIndex() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with deleted index",
          ConfigJournalV1Builder.builder()
              .deletedIndex(searchDefinition("012345678901234567890123", "foo", 1, 2))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<ConfigJournalV1> withStagedVectorIndex() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with staged vector index",
          ConfigJournalV1Builder.builder()
              .stagedIndex(
                  new VectorIndexDefinitionGeneration(
                      VectorIndexDefinitionBuilder.builder()
                          .setFields(VectorIndex.MOCK_VECTOR_DEFINITION.getFields())
                          .collectionUuid(UUID.fromString("00000000-1111-2222-3333-444444444444"))
                          .build(),
                      Generation.CURRENT))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<ConfigJournalV1> withVectorIndex() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with vector index",
          ConfigJournalV1Builder.builder()
              .liveIndex(
                  new VectorIndexDefinitionGeneration(
                      VectorIndexDefinitionBuilder.builder()
                          .setFields(VectorIndex.MOCK_VECTOR_DEFINITION.getFields())
                          .collectionUuid(UUID.fromString("00000000-1111-2222-3333-444444444444"))
                          .build(),
                      Generation.CURRENT))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<ConfigJournalV1> withDeletedVectorIndex() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with deleted vector index",
          ConfigJournalV1Builder.builder()
              .deletedIndex(
                  new VectorIndexDefinitionGeneration(
                      VectorIndexDefinitionBuilder.builder()
                          .setFields(VectorIndex.MOCK_VECTOR_DEFINITION.getFields())
                          .collectionUuid(UUID.fromString("00000000-1111-2222-3333-444444444444"))
                          .build(),
                      Generation.CURRENT))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<ConfigJournalV1> multipleValues() {
      var expected =
          ConfigJournalV1Builder.builder()
              .stagedIndex(searchDefinition("012345678901234567890123", "foo", 10, 10))
              .liveIndex(searchDefinition("012345678901234567890123", "foo", 9, 9))
              .deletedIndex(searchDefinition("012345678901234567890123", "foo", 8, 8))
              .build();
      return BsonSerializationTestSuite.TestSpec.create("multiple values", expected);
    }

    private static BsonSerializationTestSuite.TestSpec<ConfigJournalV1> multipleVectorValues() {
      var index =
          new VectorIndexDefinitionGeneration(
              VectorIndexDefinitionBuilder.builder()
                  .setFields(VectorIndex.MOCK_VECTOR_DEFINITION.getFields())
                  .collectionUuid(UUID.fromString("00000000-1111-2222-3333-444444444444"))
                  .build(),
              new Generation(UserIndexVersion.FIRST, IndexFormatVersion.FIVE));

      var expected =
          ConfigJournalV1Builder.builder()
              .stagedIndex(index.upgradeToCurrentFormatVersion().incrementAttempt())
              .liveIndex(index)
              .deletedIndex(
                  new VectorIndexDefinitionGeneration(
                      VectorIndexDefinitionBuilder.builder()
                          .setFields(VectorIndex.MOCK_VECTOR_DEFINITION.getFields())
                          .collectionUuid(UUID.fromString("00000000-1111-2222-3333-444444444444"))
                          .build(),
                      Generation.FIRST))
              .build();
      return BsonSerializationTestSuite.TestSpec.create("multiple vector values", expected);
    }
  }

  public static class InvariantTest {
    @Test
    public void validateInvariantsTest() throws Exception {
      JsonTestSuite json =
          JsonTestSuite.load(
              "src/test/unit/resources/config/backup/", "config-journal-v1-deserialization");
      for (var testCase : json.getValid()) {
        try (var parser =
            BsonDocumentParser.fromRoot(testCase.getValueAsBsonValue().asDocument()).build()) {
          ConfigJournalV1 journal = ConfigJournalV1.fromBson(parser);
          Invariants.validateGenerationalInvariants(
              journal.getStagedIndexes(), journal.getLiveIndexes(), journal.getDeletedIndexes());
        }
      }
    }
  }

  private static IndexDefinitionGeneration searchDefinition(
      String indexId, String name, int user, int format) {
    return SearchIndexDefinitionBuilder.builder()
        .indexId(new ObjectId(indexId))
        .name(name)
        .database("bar")
        .collectionUuid(UUID.fromString("00000000-1111-2222-3333-444444444444"))
        .lastObservedCollectionName("baz")
        .dynamicMapping()
        .asDefinitionGeneration()
        .generation(user, format)
        .build();
  }

  @RunWith(Theories.class)
  public static class ClassTest {
    @DataPoints("indexDefinitions")
    public static List<IndexDefinitionGeneration> indexDefinitions() {
      return List.of(
          VectorIndex.MOCK_INDEX_DEFINITION_GENERATION,
          SearchIndex.MOCK_INDEX_DEFINITION_GENERATION);
    }

    @Test
    public void testNoFileReturnsEmpty() throws Exception {
      Path noSuchFile = getFilePath();
      Assert.assertTrue(ConfigJournalV1.fromFileIfExists(noSuchFile).isEmpty());
    }

    @Test
    public void testEmptyConfigJournalV1Present() throws Exception {
      Path path = getFilePath();
      var config = ConfigJournalV1Builder.builder().build();
      write(path, config.toBson());

      ConfigJournalV1 versioned = loadExisting(path);
      Assert.assertEquals(versioned, config);
    }

    @Theory
    public void testNonEmptyConfigJournalV1Present(
        @FromDataPoints("indexDefinitions") IndexDefinitionGeneration indexDefinitionGeneration)
        throws Exception {
      Path path = getFilePath();
      var config = ConfigJournalV1Builder.builder().liveIndex(indexDefinitionGeneration).build();
      write(path, config.toBson());

      ConfigJournalV1 versioned = loadExisting(path);
      Assert.assertEquals(versioned, config);
    }

    @Theory
    public void testUnknownFieldsInConfigJournalV1Allowed(
        @FromDataPoints("indexDefinitions") IndexDefinitionGeneration indexDefinitionGeneration)
        throws Exception {
      Path path = getFilePath();
      var originalConfig =
          ConfigJournalV1Builder.builder().liveIndex(indexDefinitionGeneration).build();

      var cloneWithUnknownField =
          originalConfig.toBson().clone().append("unknown-field", new BsonBoolean(true));
      write(path, cloneWithUnknownField);

      ConfigJournalV1 parsed = loadExisting(path);
      Assert.assertEquals(parsed, originalConfig);
    }

    @Test
    public void testInvalidVersionThrowsBsonParseException() throws Exception {
      Path path = getFilePath();
      FileUtils.atomicallyReplace(
          path, JsonCodec.toJson(new BsonDocument("version", new BsonInt32(2))));

      TestUtils.assertThrows(
          "must be set to 1 for this config format",
          BsonParseException.class,
          () -> loadExisting(path));
    }

    @Test
    public void testIncorrectVersionTypeThrowsBsonParseException() throws Exception {
      Path path = getFilePath();
      FileUtils.atomicallyReplace(
          path, JsonCodec.toJson(new BsonDocument("version", new BsonString("foo"))));

      TestUtils.assertThrows(
          "must be a integer", BsonParseException.class, () -> loadExisting(path));
      TestUtils.assertThrows(
          "must be a integer", BsonParseException.class, () -> loadExisting(path));
    }

    /**
     * A journal holding a single valid entry in one of the six index arrays, so that each array's
     * quarantine behaviour can be exercised the same way.
     */
    public record SingleEntryJournal(
        String arrayName, ConfigJournalV1 journal, IndexDefinitionGeneration.Type type) {
      @Override
      public String toString() {
        return this.arrayName;
      }
    }

    @DataPoints("singleEntryJournals")
    public static List<SingleEntryJournal> singleEntryJournals() {
      IndexDefinitionGeneration search = searchDefinition("012345678901234567890123", "foo", 1, 2);
      IndexDefinitionGeneration vector = VectorIndex.MOCK_INDEX_DEFINITION_GENERATION;

      return List.of(
          new SingleEntryJournal(
              "stagedIndexes",
              ConfigJournalV1Builder.builder().stagedIndex(search).build(),
              IndexDefinitionGeneration.Type.SEARCH),
          new SingleEntryJournal(
              "indexes",
              ConfigJournalV1Builder.builder().liveIndex(search).build(),
              IndexDefinitionGeneration.Type.SEARCH),
          new SingleEntryJournal(
              "deletedIndexes",
              ConfigJournalV1Builder.builder().deletedIndex(search).build(),
              IndexDefinitionGeneration.Type.SEARCH),
          new SingleEntryJournal(
              "stagedVectorIndexes",
              ConfigJournalV1Builder.builder().stagedIndex(vector).build(),
              IndexDefinitionGeneration.Type.VECTOR),
          new SingleEntryJournal(
              "vectorIndexes",
              ConfigJournalV1Builder.builder().liveIndex(vector).build(),
              IndexDefinitionGeneration.Type.VECTOR),
          new SingleEntryJournal(
              "deletedVectorIndexes",
              ConfigJournalV1Builder.builder().deletedIndex(vector).build(),
              IndexDefinitionGeneration.Type.VECTOR));
    }

    @DataPoints("policies")
    public static List<QuarantinePolicy> policies() {
      return List.of(QuarantinePolicy.FAIL_FAST, QuarantinePolicy.QUARANTINE);
    }

    @Theory
    public void fromFileIfExists_quarantine_unparseableEntryRoundTripsVerbatim(
        @FromDataPoints("singleEntryJournals") SingleEntryJournal singleEntryJournal)
        throws Exception {
      Path path = getFilePath();
      BsonDocument corrupted =
          withUnknownFieldInFirstElement(
              singleEntryJournal.journal().toBson(), singleEntryJournal.arrayName());
      write(path, corrupted);

      ConfigJournalV1 parsed =
          ConfigJournalV1.fromFileIfExists(path, QuarantinePolicy.QUARANTINE).orElseThrow();

      Assert.assertEquals(JsonCodec.toJson(corrupted), JsonCodec.toJson(parsed.toBson()));
      Assert.assertEquals(1, parsed.getQuarantinedEntries().size());
      Assert.assertTrue(parsed.getStagedIndexes().isEmpty());
      Assert.assertTrue(parsed.getLiveIndexes().isEmpty());
      Assert.assertTrue(parsed.getDeletedIndexes().isEmpty());
    }

    /**
     * The type of a quarantined entry comes from the array it was read from, since its own fields
     * could not be parsed.
     */
    @Theory
    public void fromFileIfExists_quarantine_entryCarriesTheTypeOfItsArray(
        @FromDataPoints("singleEntryJournals") SingleEntryJournal singleEntryJournal)
        throws Exception {
      Path path = getFilePath();
      write(
          path,
          withUnknownFieldInFirstElement(
              singleEntryJournal.journal().toBson(), singleEntryJournal.arrayName()));

      ConfigJournalV1 parsed =
          ConfigJournalV1.fromFileIfExists(path, QuarantinePolicy.QUARANTINE).orElseThrow();

      Assert.assertEquals(
          singleEntryJournal.type(), parsed.getQuarantinedEntries().get(0).getType());
    }

    @Theory
    public void fromFileIfExists_failFast_unparseableEntryThrows(
        @FromDataPoints("singleEntryJournals") SingleEntryJournal singleEntryJournal)
        throws Exception {
      Path path = getFilePath();
      write(
          path,
          withUnknownFieldInFirstElement(
              singleEntryJournal.journal().toBson(), singleEntryJournal.arrayName()));

      TestUtils.assertThrows(
          "unrecognized field \"fieldFromTheFuture\"",
          BsonParseException.class,
          () -> ConfigJournalV1.fromFileIfExists(path, QuarantinePolicy.FAIL_FAST));
    }

    @Test
    public void fromFileIfExists_quarantine_healthyEntriesInSameArrayStillParse() throws Exception {
      Path path = getFilePath();
      var journal =
          ConfigJournalV1Builder.builder()
              .liveIndex(searchDefinition("012345678901234567890123", "foo", 1, 2))
              .liveIndex(searchDefinition("012345678901234567890124", "baz", 1, 2))
              .build();
      write(path, withUnknownFieldInFirstElement(journal.toBson(), "indexes"));

      ConfigJournalV1 parsed =
          ConfigJournalV1.fromFileIfExists(path, QuarantinePolicy.QUARANTINE).orElseThrow();

      Assert.assertEquals(1, parsed.getLiveIndexes().size());
      Assert.assertEquals("baz", parsed.getLiveIndexes().get(0).getIndexDefinition().getName());
      Assert.assertEquals(1, parsed.getQuarantinedEntries().size());
    }

    /**
     * The corrupted entry is the one that sorts first, so a byte-identical round trip is only
     * possible if it sorted into the same slot its parseable twin would have.
     */
    @Test
    public void fromFileIfExists_quarantine_quarantinedEntrySortsLikeItsParseableTwin()
        throws Exception {
      Path path = getFilePath();
      var journal =
          ConfigJournalV1Builder.builder()
              .liveIndex(searchDefinition("012345678901234567890123", "foo", 1, 2))
              .liveIndex(searchDefinition("012345678901234567890124", "baz", 1, 2))
              .build();
      BsonDocument corrupted = withUnknownFieldInFirstElement(journal.toBson(), "indexes");
      write(path, corrupted);

      ConfigJournalV1 parsed =
          ConfigJournalV1.fromFileIfExists(path, QuarantinePolicy.QUARANTINE).orElseThrow();

      Assert.assertEquals(JsonCodec.toJson(corrupted), JsonCodec.toJson(parsed.toBson()));
    }

    @Theory
    public void fromFileIfExists_entryWithoutIdentity_throwsUnderEitherPolicy(
        @FromDataPoints("policies") QuarantinePolicy policy) throws Exception {
      Path path = getFilePath();
      BsonDocument journal =
          ConfigJournalV1Builder.builder()
              .liveIndex(searchDefinition("012345678901234567890123", "foo", 1, 2))
              .build()
              .toBson();
      journal.getArray("indexes").get(0).asDocument().getDocument("index").remove("indexID");
      write(path, journal);

      TestUtils.assertThrows(
          "is required",
          BsonParseException.class,
          () -> ConfigJournalV1.fromFileIfExists(path, policy));
    }

    @Theory
    public void fromFileIfExists_entryIsNotADocument_throwsUnderEitherPolicy(
        @FromDataPoints("policies") QuarantinePolicy policy) throws Exception {
      Path path = getFilePath();
      BsonDocument journal = ConfigJournalV1Builder.builder().build().toBson();
      journal.put("indexes", new BsonArray(List.of(new BsonInt32(5))));
      write(path, journal);

      TestUtils.assertThrows(
          "must be a document",
          BsonParseException.class,
          () -> ConfigJournalV1.fromFileIfExists(path, policy));
    }

    @Theory
    public void fromFileIfExists_arrayIsNotAnArray_throwsUnderEitherPolicy(
        @FromDataPoints("policies") QuarantinePolicy policy) throws Exception {
      Path path = getFilePath();
      BsonDocument journal = ConfigJournalV1Builder.builder().build().toBson();
      journal.put("indexes", new BsonInt32(5));
      write(path, journal);

      TestUtils.assertThrows(
          "must be a array",
          BsonParseException.class,
          () -> ConfigJournalV1.fromFileIfExists(path, policy));
    }

    @Theory
    public void fromFileIfExists_requiredArrayMissing_throwsUnderEitherPolicy(
        @FromDataPoints("policies") QuarantinePolicy policy) throws Exception {
      Path path = getFilePath();
      BsonDocument journal = ConfigJournalV1Builder.builder().build().toBson();
      journal.remove("indexes");
      write(path, journal);

      TestUtils.assertThrows(
          "is required",
          BsonParseException.class,
          () -> ConfigJournalV1.fromFileIfExists(path, policy));
    }

    /**
     * Adds a field no version of mongot knows about, which makes the entry unparseable while
     * leaving its identity readable.
     */
    private static BsonDocument withUnknownFieldInFirstElement(
        BsonDocument journal, String arrayName) {
      BsonDocument corrupted = journal.clone();
      corrupted
          .getArray(arrayName)
          .get(0)
          .asDocument()
          .append("fieldFromTheFuture", new BsonBoolean(true));
      return corrupted;
    }

    private Path getFilePath() throws Exception {
      var folder = TestUtils.getTempFolder().getRoot();
      return folder.toPath().resolve("file.json");
    }

    private void write(Path path, BsonDocument document) throws IOException {
      FileUtils.atomicallyReplace(path, JsonCodec.toJson(document));
    }

    private ConfigJournalV1 loadExisting(Path path) throws IOException, BsonParseException {
      return ConfigJournalV1.fromFileIfExists(path).orElseThrow();
    }
  }
}
