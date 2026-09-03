package com.xgen.mongot.config.backup;

import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.mongot.config.backup.ConfigJournalV1Builder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import java.util.Collections;
import java.util.List;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class JournalEditorTest {

  public record TestData(
      IndexDefinitionGeneration indexGen1,
      IndexDefinitionGeneration indexGen2,
      ConfigJournalV1 oneLiveIndex) {

    private static TestData create(IndexDefinitionGeneration indexDefinitionGeneration) {
      return new TestData(
          indexDefinitionGeneration,
          indexDefinitionGeneration.incrementAttempt(),
          ConfigJournalV1Builder.builder().liveIndex(indexDefinitionGeneration).build());
    }
  }

  @DataPoints("testData")
  public static List<TestData> clusterTypes() {
    return List.of(
        TestData.create(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION),
        TestData.create(VectorIndex.MOCK_INDEX_DEFINITION_GENERATION));
  }

  private static final ConfigJournalV1 EMPTY = ConfigJournalV1Builder.builder().build();

  @Test
  public void testEmptyUnchanged() {
    assertJournalEquals(EMPTY, JournalEditor.on(EMPTY).journal());
  }

  @Theory
  public void testStagedIndexUnchanged(@FromDataPoints("testData") TestData data) {
    ConfigJournalV1 oneStaged =
        ConfigJournalV1Builder.builder().stagedIndex(data.indexGen1).build();
    assertJournalEquals(oneStaged, JournalEditor.on(oneStaged).journal());
  }

  @Theory
  public void testLiveIndexUnchanged(@FromDataPoints("testData") TestData data) {
    assertJournalEquals(data.oneLiveIndex, JournalEditor.on(data.oneLiveIndex).journal());
  }

  @Theory
  public void testDeletedIndexUnchanged(@FromDataPoints("testData") TestData data) {
    ConfigJournalV1 oneDeleted =
        ConfigJournalV1Builder.builder().deletedIndex(data.indexGen1).build();
    assertJournalEquals(oneDeleted, JournalEditor.on(oneDeleted).journal());
  }

  @Theory
  public void testMovingEmptyListsDoesNothing(@FromDataPoints("testData") TestData data) {
    var input =
        ConfigJournalV1Builder.builder()
            .stagedIndex(data.indexGen1)
            .liveIndex(data.indexGen1)
            .build();
    var edited =
        JournalEditor.on(input)
            .fromLiveToDropped(Collections.emptyList())
            .fromStagedToLive(Collections.emptyList())
            .journal();
    assertJournalEquals(input, edited);
  }

  @Theory
  public void testEmptyAddedLive(@FromDataPoints("testData") TestData data) {
    assertJournalEquals(
        ConfigJournalV1Builder.builder().liveIndex(data.indexGen1).build(),
        JournalEditor.on(EMPTY).addLive(List.of(data.indexGen1)).journal());
  }

  @Theory
  public void testAddingTwoIndexes(@FromDataPoints("testData") TestData data) {
    assertJournalEquals(
        ConfigJournalV1Builder.builder()
            .liveIndex(data.indexGen1)
            .liveIndex(data.indexGen2)
            .build(),
        JournalEditor.on(EMPTY).addLive(List.of(data.indexGen1, data.indexGen2)).journal());
  }

  @Theory
  public void testMoveFromLiveAndAddNew(@FromDataPoints("testData") TestData data) {
    // drop INDEX_GEN_1 and add INDEX_GEN_2
    var expected =
        ConfigJournalV1Builder.builder()
            .deletedIndex(data.indexGen1)
            .liveIndex(data.indexGen2)
            .build();
    var actual =
        JournalEditor.on(data.oneLiveIndex)
            .addLive(List.of(data.indexGen2))
            .fromLiveToDropped(List.of(data.indexGen1.getGenerationId()))
            .journal();
    assertJournalEquals(expected, actual);

    var actualReverseOrderOfOperations =
        JournalEditor.on(data.oneLiveIndex)
            .fromLiveToDropped(List.of(data.indexGen1.getGenerationId()))
            .addLive(List.of(data.indexGen2))
            .journal();
    assertJournalEquals(expected, actualReverseOrderOfOperations);
  }

  @Theory
  public void testDropTwoIndexes(@FromDataPoints("testData") TestData data) {
    var expected =
        ConfigJournalV1Builder.builder()
            .deletedIndex(data.indexGen1)
            .deletedIndex(data.indexGen2)
            .build();
    var actual =
        JournalEditor.on(
                ConfigJournalV1Builder.builder()
                    .liveIndex(data.indexGen1)
                    .liveIndex(data.indexGen2)
                    .build())
            .fromLiveToDropped(
                List.of(data.indexGen1.getGenerationId(), data.indexGen2.getGenerationId()))
            .journal();
    assertJournalEquals(expected, actual);
  }

  @Theory
  public void testFromStagedToLive(@FromDataPoints("testData") TestData data) {
    // INDEX_GEN_2 is being moved from being staged to being live
    var expected =
        ConfigJournalV1Builder.builder()
            .liveIndex(data.indexGen1)
            .liveIndex(data.indexGen2)
            .build();
    var actual =
        JournalEditor.on(
                ConfigJournalV1Builder.builder()
                    .liveIndex(data.indexGen1)
                    .stagedIndex(data.indexGen2)
                    .build())
            .fromStagedToLive(List.of(data.indexGen2.getGenerationId()))
            .journal();
    assertJournalEquals(expected, actual);
  }

  @Theory
  public void testDroppingNonExistingIndexShouldThrow(@FromDataPoints("testData") TestData data) {
    var editor = JournalEditor.on(EMPTY);
    Assert.assertThrows(
        IllegalStateException.class,
        () -> editor.fromLiveToDropped(List.of(data.indexGen1.getGenerationId())));
    Assert.assertThrows(
        IllegalStateException.class,
        () ->
            editor.fromLiveToDropped(
                List.of(data.indexGen1.getGenerationId(), data.indexGen2.getGenerationId())));
  }

  @Theory
  public void testSwappingInNonExistingIndexShouldThrow(@FromDataPoints("testData") TestData data) {
    var editor = JournalEditor.on(EMPTY);
    Assert.assertThrows(
        IllegalStateException.class,
        () -> editor.fromStagedToLive(List.of(data.indexGen1.getGenerationId())));
    Assert.assertThrows(
        IllegalStateException.class,
        () ->
            editor.fromStagedToLive(
                List.of(data.indexGen1.getGenerationId(), data.indexGen2.getGenerationId())));
  }

  /**
   * The array each state's entries live in, so a quarantined entry can be produced in any of the
   * six of them.
   */
  public record QuarantinedCase(String arrayName, ConfigJournalV1 journal) {}

  @DataPoints("quarantinedLive")
  public static List<QuarantinedCase> quarantinedLive() {
    return List.of(
        new QuarantinedCase(
            "indexes",
            ConfigJournalV1Builder.builder()
                .liveIndex(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION)
                .build()),
        new QuarantinedCase(
            "vectorIndexes",
            ConfigJournalV1Builder.builder()
                .liveIndex(VectorIndex.MOCK_INDEX_DEFINITION_GENERATION)
                .build()));
  }

  /**
   * A quarantined entry has no definition, so an editor that held definitions would drop it here.
   * Round tripping it through an edit is what keeps it in the journal on the next persist.
   */
  @Theory
  public void quarantinedLiveEntrySurvivesAnUneditedRoundTrip(
      @FromDataPoints("quarantinedLive") QuarantinedCase testCase) throws Exception {
    ConfigJournalV1 quarantined = quarantineFirstEntry(testCase.journal(), testCase.arrayName());

    ConfigJournalV1 edited = JournalEditor.on(quarantined).journal();

    Assert.assertEquals(1, edited.getQuarantinedEntries().size());
    Assert.assertEquals(JsonCodec.toJson(quarantined.toBson()), JsonCodec.toJson(edited.toBson()));
  }

  /** An unrelated edit must not disturb a quarantined entry sitting alongside it. */
  @Theory
  public void quarantinedLiveEntrySurvivesAnUnrelatedEdit(@FromDataPoints("testData") TestData data)
      throws Exception {
    ConfigJournalV1 quarantined =
        quarantineFirstEntry(
            ConfigJournalV1Builder.builder()
                .liveIndex(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION)
                .build(),
            "indexes");

    ConfigJournalV1 edited =
        JournalEditor.on(quarantined).addLive(List.of(data.indexGen1)).journal();

    Assert.assertEquals(1, edited.getQuarantinedEntries().size());
    Assert.assertEquals(List.of(data.indexGen1), edited.getLiveIndexes());
  }

  /**
   * Moving a quarantined entry by generation id is how a live index whose definition cannot be
   * parsed gets scheduled for deletion, so it must work without a definition to move.
   */
  @Theory
  public void quarantinedLiveEntryCanBeMovedToDropped(
      @FromDataPoints("quarantinedLive") QuarantinedCase testCase) throws Exception {
    ConfigJournalV1 quarantined = quarantineFirstEntry(testCase.journal(), testCase.arrayName());
    var generationId = quarantined.getQuarantinedEntries().get(0).getGenerationId();

    ConfigJournalV1 edited =
        JournalEditor.on(quarantined).fromLiveToDropped(List.of(generationId)).journal();

    Assert.assertTrue(edited.getLiveEntries().isEmpty());
    Assert.assertEquals(1, edited.getDeletedEntries().size());
    Assert.assertTrue(edited.getDeletedEntries().get(0).isQuarantined());
  }

  /** A quarantined staged entry is promoted by generation id the same way a parsed one is. */
  @Test
  public void quarantinedStagedEntryCanBeMovedToLive() throws Exception {
    ConfigJournalV1 quarantined =
        quarantineFirstEntry(
            ConfigJournalV1Builder.builder()
                .liveIndex(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION)
                .stagedIndex(SearchIndex.MOCK_INDEX_DEFINITION_GENERATION.incrementAttempt())
                .build(),
            "stagedIndexes");
    var generationId = quarantined.getQuarantinedEntries().get(0).getGenerationId();

    ConfigJournalV1 edited =
        JournalEditor.on(quarantined).fromStagedToLive(List.of(generationId)).journal();

    Assert.assertTrue(edited.getStagedEntries().isEmpty());
    Assert.assertEquals(2, edited.getLiveEntries().size());
    Assert.assertEquals(1, edited.getLiveIndexes().size());
  }

  /**
   * Reads the journal back with an unparseable first element in the named array, so that the
   * resulting journal holds one quarantined entry.
   */
  private static ConfigJournalV1 quarantineFirstEntry(ConfigJournalV1 journal, String arrayName)
      throws BsonParseException {
    BsonDocument corrupted = journal.toBson().clone();
    corrupted
        .getArray(arrayName)
        .get(0)
        .asDocument()
        .append("fieldFromTheFuture", new BsonBoolean(true));

    try (BsonDocumentParser parser =
        BsonDocumentParser.fromRoot(corrupted).allowUnknownFields(true).build()) {
      return ConfigJournalV1.fromBson(parser, QuarantinePolicy.QUARANTINE);
    }
  }

  private void assertJournalEquals(ConfigJournalV1 expected, ConfigJournalV1 actual) {
    Assert.assertEquals(expected, actual);
  }
}
