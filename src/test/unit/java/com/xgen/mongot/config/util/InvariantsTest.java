package com.xgen.mongot.config.util;

import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.InvalidIndexDefinitionException;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorDataFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.config.backup.ConfigJournalV1Builder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionGenerationBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionGenerationBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      InvariantsTest.GenerationalInvariantsTest.class,
      InvariantsTest.ValidateInvariantsTest.class,
      InvariantsTest.ValidateVectorNestedRootReferencesTest.class,
    })
public class InvariantsTest {
  @RunWith(Parameterized.class)
  public static class GenerationalInvariantsTest {
    private final String indexType;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<String> indexType() {
      // all tests here should behave the same no matter what the index type is.
      // We don't need to test if different generations of the same index are different type,
      // that is an invalid state (tested elsewhere)
      return List.of("search", "vector");
    }

    public GenerationalInvariantsTest(String indexType) {
      this.indexType = indexType;
    }

    @Test
    public void testEmptyConfigValid() throws Exception {
      var empty = ConfigJournalV1Builder.builder().build();
      assertValid(empty);
    }

    @Test
    public void testDeletedIndexesShareIndexIdValid() throws Exception {
      var def1 = generationDefinition(1, 2);
      var def2 = generationDefinition(2, 2);
      var duplicateDeletedIndexIds =
          ConfigJournalV1Builder.builder().deletedIndex(def1).deletedIndex(def2).build();
      assertValid(duplicateDeletedIndexIds);
    }

    @Test
    public void testLiveIndexesShareIndexIdThrows() {
      var def1 = generationDefinition(1, 2);
      var def2 = generationDefinition(2, 2);
      var duplicateIndexIds =
          ConfigJournalV1Builder.builder().liveIndex(def1).liveIndex(def2).build();
      assertThrowsInvariantException(duplicateIndexIds);
    }

    @Test
    public void testStagedIndexesShareIndexIdThrows() {
      var def1 = generationDefinition(1, 2);
      var def2 = generationDefinition(2, 2);
      var duplicateIndexIds =
          ConfigJournalV1Builder.builder().stagedIndex(def1).stagedIndex(def2).build();
      assertThrowsInvariantException(duplicateIndexIds);
    }

    @Test
    public void testNonUniqueGenerationIdThrows() {
      var builder = SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping();
      var def1 = builder.name("one").asDefinitionGeneration().generation(2, 2).build();
      var def2 = builder.name("two").asDefinitionGeneration().generation(2, 2).build();

      assertThrowsInvariantException(
          ConfigJournalV1Builder.builder().stagedIndex(def1).stagedIndex(def2).build());
      assertThrowsInvariantException(
          ConfigJournalV1Builder.builder().liveIndex(def1).liveIndex(def2).build());
      assertThrowsInvariantException(
          ConfigJournalV1Builder.builder().deletedIndex(def1).deletedIndex(def2).build());

      // Test invariant between different states
      assertThrowsInvariantException(
          ConfigJournalV1Builder.builder().stagedIndex(def1).liveIndex(def2).build());
      assertThrowsInvariantException(
          ConfigJournalV1Builder.builder().stagedIndex(def1).deletedIndex(def2).build());
      assertThrowsInvariantException(
          ConfigJournalV1Builder.builder().liveIndex(def1).deletedIndex(def2).build());
    }

    /**
     * The indexes present in staged must not modify the live ones in any way that breaks
     * invariants. For instance, a staged index with the same id must have the same
     * database/name/collectionUuid.
     */
    @Test
    public void testDifferentNamespaceBetweenStagedAndLiveThrows() throws Exception {
      var existing = SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
      // modify `existing` with different invariance violations. Also make sure the generation is
      // different so we don't pass the test on a different validation.
      var changedName =
          SearchIndexDefinitionBuilder.from(existing.getIndexDefinition())
              .name("changed")
              .asDefinitionGeneration()
              .generation(3, 2)
              .build();
      assertThrowsInvariantException(
          ConfigJournalV1Builder.builder().liveIndex(existing).stagedIndex(changedName).build());

      var changedDb =
          SearchIndexDefinitionBuilder.from(SearchIndex.MOCK_INDEX_DEFINITION)
              .database("changed")
              .asDefinitionGeneration()
              .generation(3, 2)
              .build();
      assertThrowsInvariantException(
          ConfigJournalV1Builder.builder().liveIndex(existing).stagedIndex(changedDb).build());

      var changedCollection =
          SearchIndexDefinitionBuilder.from(existing.getIndexDefinition())
              .collectionUuid(UUID.randomUUID())
              .asDefinitionGeneration()
              .generation(3, 2)
              .build();
      assertThrowsInvariantException(
          ConfigJournalV1Builder.builder()
              .liveIndex(existing)
              .stagedIndex(changedCollection)
              .build());

      var existing2 =
          SearchIndexDefinitionBuilder.from(SearchIndex.MOCK_INDEX_DEFINITION)
              .indexId(new ObjectId())
              .name("existing2")
              .asDefinitionGeneration()
              .generation(3, 2)
              .build();
      var changedName2 =
          SearchIndexDefinitionBuilder.from(existing2.getIndexDefinition())
              .name("changed")
              .asDefinitionGeneration()
              .generation(3, 2)
              .build();
      assertThrowsMessages(
          ConfigJournalV1Builder.builder()
              .liveIndex(existing)
              .stagedIndex(changedDb)
              .liveIndex(existing2)
              .stagedIndex(changedName2)
              .build(),
          List.of(
              "originally had name existing2 but now has changed",
              "originally had database mock_database but now has changed"));

      var changedMappingIsValid =
          SearchIndexDefinitionBuilder.from(existing.getIndexDefinition().asSearchDefinition())
              .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(false).build())
              .asDefinitionGeneration()
              .generation(3, 2)
              .build();
      assertValid(
          ConfigJournalV1Builder.builder()
              .liveIndex(existing)
              .stagedIndex(changedMappingIsValid)
              .build());

      var changedCollectionNameIsValid =
          SearchIndexDefinitionBuilder.from(existing.getIndexDefinition().asSearchDefinition())
              .lastObservedCollectionName("rename")
              .asDefinitionGeneration()
              .generation(3, 2)
              .build();
      assertValid(
          ConfigJournalV1Builder.builder()
              .liveIndex(existing)
              .stagedIndex(changedCollectionNameIsValid)
              .build());
    }

    /**
     * It is not allowed for two indexes to have the same namespace, even if they have different
     * indexIds.
     */
    @Test
    public void testDuplicateNamespaceLiveIndexThrows() {
      var builder = SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping();
      var def1 = builder.indexId(new ObjectId()).asDefinitionGeneration().generation(1, 2).build();
      var def2 = builder.indexId(new ObjectId()).asDefinitionGeneration().generation(1, 2).build();
      var duplicateNamespace =
          ConfigJournalV1Builder.builder().liveIndex(def1).liveIndex(def2).build();
      assertThrowsInvariantException(duplicateNamespace);
    }

    @Test
    public void testDuplicateViewNamespaceLiveIndexThrows() {
      var builder = SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping();
      var def1 =
          builder
              .indexId(new ObjectId())
              .view(ViewDefinition.existing("view1", List.of()))
              .asDefinitionGeneration()
              .generation(1, 2)
              .build();
      var def2 =
          builder
              .indexId(new ObjectId())
              .view(ViewDefinition.existing("view1", List.of()))
              .asDefinitionGeneration()
              .generation(1, 2)
              .build();
      var duplicateNamespace =
          ConfigJournalV1Builder.builder().liveIndex(def1).liveIndex(def2).build();
      assertThrowsInvariantException(duplicateNamespace);
    }

    @Test
    public void testTheSameCollectionUuidForIndexCreatedOnCollectionAndOnViewThrows() {
      var builder = SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping();
      var def1 = builder.indexId(new ObjectId()).asDefinitionGeneration().generation(1, 2).build();
      // def2 is created on a view that has source collection used in def1
      var def2 =
          builder
              .indexId(new ObjectId())
              .view(ViewDefinition.existing("view", List.of()))
              .asDefinitionGeneration()
              .generation(1, 2)
              .build();
      var duplicateNamespace =
          ConfigJournalV1Builder.builder().liveIndex(def1).liveIndex(def2).build();
      assertThrowsInvariantException(duplicateNamespace);
    }

    @Test
    public void testDuplicateNamespaceBetweenDifferentIndexTypesThrows() {
      var def1 =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .dynamicMapping()
              .indexId(new ObjectId())
              .asDefinitionGeneration()
              .generation(1, 2)
              .build();
      var def2 =
          new VectorIndexDefinitionGeneration(
              VectorIndexDefinitionBuilder.builder()
                  .indexId(new ObjectId())
                  .database(def1.getIndexDefinition().getDatabase())
                  .collectionUuid(def1.getIndexDefinition().getCollectionUuid())
                  .name(def1.getIndexDefinition().getName())
                  .build(),
              Generation.FIRST);
      var duplicateNamespace =
          ConfigJournalV1Builder.builder().liveIndex(def1).liveIndex(def2).build();
      assertThrowsInvariantException(duplicateNamespace);
    }

    /** We do not allow a staged index to exist without a corresponding live index. */
    @Test
    public void testStagedWithoutLiveIndexThrows() {
      var orphanStaged =
          ConfigJournalV1Builder.builder().stagedIndex(generationDefinition(20, 20)).build();
      assertThrowsInvariantException(orphanStaged);
    }

    private void assertThrowsInvariantException(ConfigJournalV1 config) {
      Assert.assertThrows(
          Invariants.InvariantException.class,
          () ->
              Invariants.validateGenerationalInvariants(
                  config.getStagedIndexes(), config.getLiveIndexes(), config.getDeletedIndexes()));
    }

    private void assertThrowsMessages(ConfigJournalV1 config, List<String> messages) {
      try {
        Invariants.validateGenerationalInvariants(
            config.getStagedIndexes(), config.getLiveIndexes(), config.getDeletedIndexes());
      } catch (Invariants.InvariantException ex) {
        messages.stream()
            .forEach((message) -> Assert.assertTrue(ex.getMessage().contains(message)));
      }
    }

    private void assertValid(ConfigJournalV1 config) throws Exception {
      // should not throw
      Invariants.validateGenerationalInvariants(
          config.getStagedIndexes(), config.getLiveIndexes(), config.getDeletedIndexes());
    }

    private IndexDefinitionGeneration generationDefinition(int userVersion, int indexFormat) {
      if (this.indexType.equals("vector")) {
        return VectorIndexDefinitionGenerationBuilder.builder()
            .definition(VectorIndex.MOCK_VECTOR_DEFINITION)
            .generation(
                new Generation(
                    new UserIndexVersion(userVersion), IndexFormatVersion.create(indexFormat)))
            .build();
      }
      if (this.indexType.equals("search")) {
        return SearchIndexDefinitionGenerationBuilder.create(
            SearchIndex.MOCK_INDEX_DEFINITION,
            new Generation(
                new UserIndexVersion(userVersion), IndexFormatVersion.create(indexFormat)),
            Collections.emptyList());
      }
      Assert.fail("test bug: invalid index type");
      return Check.unreachable();
    }
  }

  public static class ValidateInvariantsTest {
    @Test
    public void emptyIsValid() throws Exception {
      assertValid(Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexRemovedOrAddedIsValid() throws Exception {
      assertValid(List.of(), List.of(SearchIndex.MOCK_INDEX_DEFINITION));
      assertValid(List.of(SearchIndex.MOCK_INDEX_DEFINITION), List.of());
    }

    @Test
    public void vectorIndexRemovedOrAddedIsValid() throws Exception {
      validateVector(List.of(VectorIndex.MOCK_VECTOR_DEFINITION), List.of());
      validateVector(List.of(), List.of(VectorIndex.MOCK_VECTOR_DEFINITION));
    }

    @Test
    public void indexUnchangedIsValid() throws Exception {
      assertValid(
          List.of(SearchIndex.MOCK_INDEX_DEFINITION), List.of(SearchIndex.MOCK_INDEX_DEFINITION));
      validateVector(
          List.of(VectorIndex.MOCK_VECTOR_DEFINITION), List.of(VectorIndex.MOCK_VECTOR_DEFINITION));
    }

    @Test
    public void changedNamespaceIsInvalid() throws Exception {
      var changedDatabase =
          SearchIndexDefinitionBuilder.from(SearchIndex.MOCK_INDEX_DEFINITION)
              .database("changed")
              .build();
      assertThrows(List.of(SearchIndex.MOCK_INDEX_DEFINITION), List.of(changedDatabase));

      var changedName =
          SearchIndexDefinitionBuilder.from(SearchIndex.MOCK_INDEX_DEFINITION)
              .name("changed")
              .build();
      assertThrows(List.of(SearchIndex.MOCK_INDEX_DEFINITION), List.of(changedName));

      var changedUuid =
          SearchIndexDefinitionBuilder.from(SearchIndex.MOCK_INDEX_DEFINITION)
              .collectionUuid(UUID.randomUUID())
              .build();
      assertThrows(List.of(SearchIndex.MOCK_INDEX_DEFINITION), List.of(changedUuid));

      // It is okay for a lastObservedCollectionName to be different.
      var changedCollectionNameIsValid =
          SearchIndexDefinitionBuilder.from(SearchIndex.MOCK_INDEX_DEFINITION)
              .lastObservedCollectionName("collection_renamed")
              .build();
      assertValid(
          List.of(SearchIndex.MOCK_INDEX_DEFINITION), List.of(changedCollectionNameIsValid));
    }

    @Test
    public void changedViewSourceIsInvalid() throws Exception {

      var existing =
          SearchIndexDefinitionBuilder.from(SearchIndex.MOCK_INDEX_DEFINITION)
              .view(ViewDefinition.existing("test1", List.of()))
              .build();

      var desired =
          SearchIndexDefinitionBuilder.from(SearchIndex.MOCK_INDEX_DEFINITION)
              .view(ViewDefinition.existing("test2", List.of()))
              .build();

      assertThrows(List.of(desired), List.of(existing));
    }

    @Test
    public void vectorIndexChangedNamespaceIsInvalid() throws Exception {
      var changedDatabase =
          VectorIndexDefinitionBuilder.from(VectorIndex.MOCK_VECTOR_DEFINITION)
              .database("changed")
              .build();
      assertThrowsVector(List.of(VectorIndex.MOCK_VECTOR_DEFINITION), List.of(changedDatabase));

      var changedName =
          VectorIndexDefinitionBuilder.from(VectorIndex.MOCK_VECTOR_DEFINITION)
              .name("changed")
              .build();
      assertThrowsVector(List.of(VectorIndex.MOCK_VECTOR_DEFINITION), List.of(changedName));

      var changedUuid =
          VectorIndexDefinitionBuilder.from(VectorIndex.MOCK_VECTOR_DEFINITION)
              .collectionUuid(UUID.randomUUID())
              .build();
      assertThrowsVector(List.of(VectorIndex.MOCK_VECTOR_DEFINITION), List.of(changedUuid));

      // It is okay for a lastObservedCollectionName to be different.
      var changedCollectionNameIsValid =
          VectorIndexDefinitionBuilder.from(VectorIndex.MOCK_VECTOR_DEFINITION)
              .lastObservedCollectionName("collection_renamed")
              .build();
      // should not throw:
      validateVector(
          List.of(VectorIndex.MOCK_VECTOR_DEFINITION), List.of(changedCollectionNameIsValid));
    }

    @Test
    public void testChangeIndexTypeIsInvalid() {
      var search = SearchIndex.MOCK_INDEX_DEFINITION;
      var vector =
          VectorIndexDefinitionBuilder.from(VectorIndex.MOCK_VECTOR_DEFINITION)
              .indexId(search.getIndexId())
              .database(search.getDatabase())
              .collectionUuid(search.getCollectionUuid())
              .lastObservedCollectionName(search.getLastObservedCollectionName())
              .build();
      Assert.assertThrows(
          Invariants.InvariantException.class,
          () ->
              Invariants.validateCrossTypeInvariants(List.of(), List.of(vector), List.of(search)));

      // same but reverse:
      Assert.assertThrows(
          Invariants.InvariantException.class,
          () ->
              Invariants.validateCrossTypeInvariants(List.of(search), List.of(), List.of(vector)));
    }

    @Test
    public void testCreatingSearchAndVectorWithSameIdInvalid() {
      var search = SearchIndex.MOCK_INDEX_DEFINITION;
      var vector =
          VectorIndexDefinitionBuilder.from(VectorIndex.MOCK_VECTOR_DEFINITION)
              .indexId(search.getIndexId())
              .database(search.getDatabase())
              .collectionUuid(search.getCollectionUuid())
              .lastObservedCollectionName(search.getLastObservedCollectionName())
              .build();

      Assert.assertThrows(
          Invariants.InvariantException.class,
          () ->
              Invariants.validateCrossTypeInvariants(List.of(search), List.of(vector), List.of()));
    }

    @Test
    public void testCreatingSearchAndVectorWithSameNamespaceInvalid() {
      var search = SearchIndex.MOCK_INDEX_DEFINITION;
      var vector =
          VectorIndexDefinitionBuilder.from(VectorIndex.MOCK_VECTOR_DEFINITION)
              .indexId(new ObjectId()) // not the same id
              .database(search.getDatabase())
              .collectionUuid(search.getCollectionUuid())
              .lastObservedCollectionName(search.getLastObservedCollectionName())
              .build();

      Assert.assertThrows(
          Invariants.InvariantException.class,
          () ->
              Invariants.validateCrossTypeInvariants(List.of(search), List.of(vector), List.of()));
    }

    private void assertValid(
        List<SearchIndexDefinition> desired, List<SearchIndexDefinition> existing)
        throws Exception {
      // should not throw
      validate(desired, existing);
    }

    private void assertThrows(
        List<SearchIndexDefinition> desired, List<SearchIndexDefinition> existing) {
      Assert.assertThrows(Invariants.InvariantException.class, () -> validate(desired, existing));
    }

    private void assertThrowsVector(
        List<VectorIndexDefinition> desired, List<VectorIndexDefinition> existing) {
      Assert.assertThrows(
          Invariants.InvariantException.class, () -> validateVector(desired, existing));
    }

    private void validate(
        List<SearchIndexDefinition> desiredDefinitions,
        List<SearchIndexDefinition> existingDefinitions)
        throws Invariants.InvariantException {
      Invariants.validateInvariants(
          Collections.emptyList(), desiredDefinitions, existingDefinitions);
    }

    private void validateVector(
        List<VectorIndexDefinition> desiredDefinitions,
        List<VectorIndexDefinition> existingDefinitions)
        throws Invariants.InvariantException {
      Invariants.validateInvariants(desiredDefinitions, existingDefinitions);
    }
  }

  @RunWith(org.junit.runners.JUnit4.class)
  public static class ValidateVectorNestedRootReferencesTest {

    private static final VectorFieldSpecification SIMPLE_SPEC =
        new VectorFieldSpecification(
            128,
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm());

    @Test
    public void testNestedRootMatchingFieldPathIsValid() throws Exception {
      VectorIndexDefinition definition =
          VectorIndexDefinitionBuilder.builder()
              .nestedRoot("sections")
              .setFields(
                  List.of(
                      new VectorDataFieldDefinition(
                          FieldPath.parse("sections.embedding"), SIMPLE_SPEC),
                      new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.name"))))
              .build();

      // should not throw
      Invariants.validateVectorNestedRootReferences(List.of(definition));
    }

    @Test
    public void testNoNestedRootIsValid() throws Exception {
      VectorIndexDefinition definition =
          VectorIndexDefinitionBuilder.builder()
              .setFields(
                  List.of(
                      new VectorDataFieldDefinition(
                          FieldPath.parse("embedding"), SIMPLE_SPEC)))
              .build();

      // should not throw
      Invariants.validateVectorNestedRootReferences(List.of(definition));
    }

    @Test
    public void testNestedRootNotMatchingAnyFieldPathThrows() {
      VectorIndexDefinition definition =
          VectorIndexDefinitionBuilder.builder()
              .nestedRoot("sections")
              .setFields(
                  List.of(
                      new VectorDataFieldDefinition(
                          FieldPath.parse("topLevelVector"), SIMPLE_SPEC)))
              .build();

      InvalidIndexDefinitionException exception =
          Assert.assertThrows(
              InvalidIndexDefinitionException.class,
              () -> Invariants.validateVectorNestedRootReferences(List.of(definition)));
      Assert.assertTrue(
          exception.getMessage().contains("nestedRoot \"sections\" does not match any field path"));
    }

    @Test
    public void testEmptyListIsValid() throws Exception {
      // should not throw
      Invariants.validateVectorNestedRootReferences(List.of());
    }
  }
}
