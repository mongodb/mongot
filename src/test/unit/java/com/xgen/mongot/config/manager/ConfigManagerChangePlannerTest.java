package com.xgen.mongot.config.manager;

import static com.xgen.testing.mongot.mock.index.SearchIndex.mockAnalyzerDefinitions;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGenerationProducer;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.config.manager.ConfigStateMocks;
import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.custom.TokenizerDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.definition.CustomAnalyzerDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionGenerationBuilder;
import com.xgen.testing.mongot.index.definition.SynonymMappingDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.IndexGeneration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class ConfigManagerChangePlannerTest {

  private static final ObjectId INDEX_ID = new ObjectId();

  @Test
  public void testNoIndexesExistNoIndexesDesiredProducesNoChanges() throws Exception {
    var mocks = new Mocks();
    var plan = mocks.plan(emptyList(), emptyList(), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoModifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    Assert.assertFalse(plan.hasChanges());
  }

  @Test
  public void testExistingIndexWhoseDesiredDefinitionIsUnreadableIsNotDropped() throws Exception {
    var indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(INDEX_ID)
            .dynamicMapping()
            .build();
    var mocks = Mocks.withIndexes(indexDefinition);

    // The index is still desired, but its definition could not be read, so it has no producer.
    var plan = mocks.planWithUnreadableDesired(emptyList(), emptyList(), Set.of(INDEX_ID));

    assertNoDroppedIndexes(plan);
    assertNoAddedIndexes(plan);
    assertNoModifiedIndexes(plan);
    Assert.assertFalse(plan.hasChanges());
  }

  @Test
  public void testExistingIndexAbsentFromBothDesiredAndIncompatibleIsDropped() throws Exception {
    var indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(INDEX_ID)
            .dynamicMapping()
            .build();
    var mocks = Mocks.withIndexes(indexDefinition);

    // A different index is unreadable; this one was genuinely deleted.
    var plan = mocks.planWithUnreadableDesired(emptyList(), emptyList(), Set.of(new ObjectId()));

    assertIndexDropped(INDEX_ID, plan);
  }

  @Test
  public void testUnreadableDesiredIndexThatWasNeverBuiltIsNotAdded() throws Exception {
    var mocks = new Mocks();

    var plan = mocks.planWithUnreadableDesired(emptyList(), emptyList(), Set.of(INDEX_ID));

    assertNoAddedIndexes(plan);
    assertNoDroppedIndexes(plan);
    Assert.assertFalse(plan.hasChanges());
  }

  @Test
  public void testNoIndexesExistSingleSearchIndexDesiredProducesNoChangesProducesAddedIndex()
      throws Exception {
    var mocks = new Mocks();
    var indexDefinition =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping().build();

    var plan = mocks.plan(emptyList(), singletonList(indexDefinition), emptyList());

    assertIndexAdded(indexDefinition, plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoModifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    Assert.assertTrue(plan.hasChanges());
  }

  @Test
  public void testNoIndexesExistSingleVectorIndexDesiredProducesNoChangesProducesAddedIndex()
      throws Exception {
    var mocks = new Mocks();
    var indexDefinition =
        VectorIndexDefinitionBuilder.builder().withCosineVectorField("vector", 1024).build();

    var plan = mocks.plan(singletonList(indexDefinition), emptyList(), emptyList());

    assertIndexAdded(indexDefinition, plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoModifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    Assert.assertTrue(plan.hasChanges());
  }

  @Test
  public void testNoIndexesExistManyIndexesDesiredProducesNoChangesProducesAddedIndexes()
      throws Exception {
    CheckedSupplier<SearchIndexDefinition, Exception> searchDefinitionSupplier =
        () -> SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping().build();
    var searchDefinitions =
        List.of(
            searchDefinitionSupplier.get(),
            searchDefinitionSupplier.get(),
            searchDefinitionSupplier.get());

    CheckedSupplier<VectorIndexDefinition, Exception> vectorDefinitionSupplier =
        () ->
            VectorIndexDefinitionBuilder.builder()
                .indexId(new ObjectId())
                .withCosineVectorField("vector", 1024)
                .build();

    var vectorDefinitions =
        List.of(
            vectorDefinitionSupplier.get(),
            vectorDefinitionSupplier.get(),
            vectorDefinitionSupplier.get());

    var mocks = new Mocks();
    var plan = mocks.plan(vectorDefinitions, searchDefinitions, emptyList());

    searchDefinitions.forEach(definition -> assertIndexAdded(definition, plan));
    vectorDefinitions.forEach(definition -> assertIndexAdded(definition, plan));

    assertNoUnmodifiedIndexes(plan);
    assertNoModifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    Assert.assertTrue(plan.hasChanges());
  }

  @Test
  public void testSingleSearchIndexExistsSameIndexDesiredProducesNoChanges() throws Exception {
    var indexId = new ObjectId();
    Supplier<SearchIndexDefinitionBuilder> builder =
        () ->
            SearchIndexDefinitionBuilder.builder()
                .defaultMetadata()
                .indexId(indexId)
                .dynamicMapping();

    // Create an object representing the index definition already existing.
    var originalDefinition = builder.get().build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Create a new object representing the same index definition. This tests that the equality
    // check works.
    var desiredDefinition = builder.get().build();
    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertIndexUnmodified(indexId, plan);
    assertNoModifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    Assert.assertFalse(plan.hasChanges());
  }

  @Test
  public void testDefinitionVersionChangeCases() throws Exception {
    testDefinitionVersionChanges(
        original -> original.definitionVersion(0L),
        desired -> {}, // not possible cause mms sends 0 if empty
        (indexId, plan) -> {
          assertUnchangedDefinitionVersion(plan);
        });

    testDefinitionVersionChanges(
        original -> {},
        desired -> desired.definitionVersion(0L),
        (desiredDefinition, plan) -> {
          assertUnchangedDefinitionVersion(plan);
        });

    testDefinitionVersionChanges(
        original -> original.definitionVersion(0L),
        desired -> desired.definitionVersion(0L),
        (indexId, plan) -> {
          assertUnchangedDefinitionVersion(plan);
        });

    testDefinitionVersionChanges(
        original -> {},
        desired -> desired.definitionVersion(1L),
        ConfigManagerChangePlannerTest::assertChangedDefinitionVersion);

    testDefinitionVersionChanges(
        original -> original.definitionVersion(0L),
        desired -> desired.definitionVersion(1L),
        ConfigManagerChangePlannerTest::assertChangedDefinitionVersion);

    testDefinitionVersionChanges(
        original -> original.definitionVersion(1L),
        desired -> desired.definitionVersion(0L),
        ConfigManagerChangePlannerTest::assertChangedDefinitionVersion);

    testDefinitionVersionChanges(
        original -> original.definitionVersion(1L),
        desired -> desired.definitionVersion(2L),
        ConfigManagerChangePlannerTest::assertChangedDefinitionVersion);
  }

  @Test
  public void testSingleVectorIndexExistsSameIndexDesiredProducesNoChanges() throws Exception {
    var indexId = new ObjectId();
    Supplier<VectorIndexDefinitionBuilder> builder =
        () ->
            VectorIndexDefinitionBuilder.builder()
                .indexId(indexId)
                .withCosineVectorField("vector", 1024);

    // Create an object representing the index definition already existing.
    var originalDefinition = builder.get().build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Create a new object representing the same index definition. This tests that the equality
    // check works.
    var desiredDefinition = builder.get().build();
    var plan = mocks.plan(singletonList(desiredDefinition), emptyList(), emptyList());

    assertNoAddedIndexes(plan);
    assertIndexUnmodified(indexId, plan);
    assertNoModifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    Assert.assertFalse(plan.hasChanges());
  }

  @Test
  public void testSingleVectorIndexExistsSameIndexDesiredWithDefinitionVersionProducesChanges()
      throws Exception {
    var indexId = new ObjectId();
    Supplier<VectorIndexDefinitionBuilder> builder =
        () ->
            VectorIndexDefinitionBuilder.builder()
                .indexId(indexId)
                .withCosineVectorField("vector", 1024);

    // Create an object representing the index definition already existing.
    var originalDefinition = builder.get().build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Create a new object representing the same index definition. This tests that the equality
    // check works.
    var desiredDefinition = builder.get().withDefinitionVersion(Optional.of(1L)).build();
    var plan = mocks.plan(singletonList(desiredDefinition), emptyList(), emptyList());

    assertNoAddedIndexes(plan);
    assertIndexModified(desiredDefinition, plan);
    assertNoDroppedIndexes(plan);
    Assert.assertTrue(plan.hasChanges());
  }

  @Test
  public void testSingleSearchIndexStagedForSwapSameIndexDesiredProducedNoChanges()
      throws Exception {
    var indexId = new ObjectId();
    Mocks mocks = Mocks.withStagedSearchIndex(indexId);

    var newStagedDefinition =
        mocks.configStateMocks.staged.getIndex(indexId).orElseThrow().getDefinition();

    // Create a new object representing the *staged* index definition.
    var desiredDefinition =
        SearchIndexDefinitionBuilder.from(newStagedDefinition.asSearchDefinition()).build();
    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertIndexUnmodified(indexId, plan);
    assertNoModifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    Assert.assertFalse(plan.hasChanges());
  }

  @Test
  public void testSingleVectorIndexStagedForSwapSameIndexDesiredProducedNoChanges()
      throws Exception {
    var indexId = new ObjectId();
    Mocks mocks = Mocks.withStagedVectorIndex(indexId);

    var newStagedDefinition =
        mocks.configStateMocks.staged.getIndex(indexId).orElseThrow().getDefinition();

    // Create a new object representing the *staged* index definition.
    var desiredDefinition =
        VectorIndexDefinitionBuilder.from(newStagedDefinition.asVectorDefinition()).build();

    var plan = mocks.plan(singletonList(desiredDefinition), emptyList(), emptyList());

    assertNoAddedIndexes(plan);
    assertIndexUnmodified(indexId, plan);
    assertNoModifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    Assert.assertFalse(plan.hasChanges());
  }

  @Test
  public void testSingleSearchIndexStagedDesiredSameAsInCatalogProducesModification()
      throws Exception {
    var indexId = new ObjectId();
    Mocks mocks = Mocks.withStagedSearchIndex(indexId);

    var oldIndexDefinition =
        mocks.configStateMocks.indexCatalog.getIndexById(indexId).orElseThrow().getDefinition();

    // Create a new object representing the *old* index definition.
    var desiredDefinition =
        SearchIndexDefinitionBuilder.from(oldIndexDefinition.asSearchDefinition()).build();
    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoDroppedIndexes(plan);

    ModifiedIndexInformation modified = assertIndexModified(desiredDefinition, plan);
    Assert.assertEquals(indexId, modified.getIndexId());
    Assert.assertEquals(desiredDefinition, modified.getDesiredDefinition().getIndexDefinition());
    Assert.assertEquals(1, modified.getReasons().size());
    Assert.assertEquals(
        ModifiedIndexInformation.Type.SAME_AS_LIVE_DIFFERENT_FROM_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "staged index: analyzer has changed");
  }

  @Test
  public void testSingleVectorIndexStagedDesiredSameAsInCatalogProducesModification()
      throws Exception {
    var indexId = new ObjectId();
    Mocks mocks = Mocks.withStagedVectorIndex(indexId);

    var oldIndexDefinition =
        mocks.configStateMocks.indexCatalog.getIndexById(indexId).orElseThrow().getDefinition();

    // Create a new object representing the *old* index definition.
    var desiredDefinition =
        VectorIndexDefinitionBuilder.from(oldIndexDefinition.asVectorDefinition()).build();
    var plan = mocks.plan(singletonList(desiredDefinition), emptyList(), emptyList());

    assertNoAddedIndexes(plan);
    assertNoDroppedIndexes(plan);

    ModifiedIndexInformation modified = assertIndexModified(desiredDefinition, plan);
    Assert.assertEquals(indexId, modified.getIndexId());
    Assert.assertEquals(desiredDefinition, modified.getDesiredDefinition().getIndexDefinition());
    Assert.assertEquals(1, modified.getReasons().size());
    Assert.assertEquals(
        ModifiedIndexInformation.Type.SAME_AS_LIVE_DIFFERENT_FROM_STAGED, modified.getType());
    TestUtils.assertContains(
        modified.getReasons(), "staged index: vector fields or filters have changed");
  }

  @Test
  public void testSingleSearchIndexStagedDesiredDifferentThanBothProducesModification()
      throws Exception {
    var indexId = new ObjectId();
    Mocks mocks = Mocks.withStagedSearchIndex(indexId);

    var oldIndexDefinition =
        mocks.configStateMocks.indexCatalog.getIndexById(indexId).orElseThrow().getDefinition();

    // Create a new definition different from both the live and the staged index.
    var desiredDefinition =
        SearchIndexDefinitionBuilder.from(oldIndexDefinition.asSearchDefinition())
            .analyzerName("lucene.cjk")
            .build();
    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoDroppedIndexes(plan);

    ModifiedIndexInformation modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(indexId, modified.getIndexId());
    Assert.assertEquals(desiredDefinition, modified.getDesiredDefinition().getIndexDefinition());
    Assert.assertTrue(modified.getReasons().size() > 1);
    Assert.assertEquals(ModifiedIndexInformation.Type.DIFFERENT_FROM_BOTH, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "staged index: analyzer has changed");
    TestUtils.assertContains(modified.getReasons(), "live index: analyzer has changed");
  }

  @Test
  public void testSingleVectorIndexStagedDesiredDifferentThanBothProducesModification()
      throws Exception {
    var indexId = new ObjectId();
    Mocks mocks = Mocks.withStagedVectorIndex(indexId);

    var oldIndexDefinition =
        mocks.configStateMocks.indexCatalog.getIndexById(indexId).orElseThrow().getDefinition();

    // Create a new definition different from both the live and the staged index.
    var desiredDefinition =
        VectorIndexDefinitionBuilder.from(oldIndexDefinition.asVectorDefinition())
            .withCosineVectorField("new.vector", 768)
            .build();
    var plan = mocks.plan(singletonList(desiredDefinition), emptyList(), emptyList());

    assertNoAddedIndexes(plan);
    assertNoDroppedIndexes(plan);

    ModifiedIndexInformation modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(indexId, modified.getIndexId());
    Assert.assertEquals(desiredDefinition, modified.getDesiredDefinition().getIndexDefinition());
    Assert.assertTrue(modified.getReasons().size() > 1);
    Assert.assertEquals(ModifiedIndexInformation.Type.DIFFERENT_FROM_BOTH, modified.getType());
    TestUtils.assertContains(
        modified.getReasons(), "staged index: vector fields or filters have changed");
    TestUtils.assertContains(
        modified.getReasons(), "live index: vector fields or filters have changed");
  }

  @Test
  public void testSingleIndexWithAnalyzerExistsSameIndexDesiredProducesNoChanges()
      throws Exception {
    CheckedSupplier<OverriddenBaseAnalyzerDefinition, Exception> analyzerDefinition =
        () ->
            OverriddenBaseAnalyzerDefinitionBuilder.builder()
                .name("foo")
                .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
                .build();

    var indexId = new ObjectId();
    Supplier<SearchIndexDefinitionBuilder> builder =
        () ->
            SearchIndexDefinitionBuilder.builder()
                .defaultMetadata()
                .indexId(indexId)
                .analyzerName("foo")
                .dynamicMapping();

    // Create an object representing the definitions already existing.
    var originalIndexDefinition = builder.get().build();
    var originalAnalyzerDefinition = analyzerDefinition.get();
    var mocks = Mocks.withIndexAndAnalyzers(originalIndexDefinition, originalAnalyzerDefinition);

    // Create a new object representing the same definitions. This tests that the equality
    // check works.
    var desiredIndexDefinition = builder.get().build();
    var desiredAnalyzerDefinition = analyzerDefinition.get();
    var plan =
        mocks.plan(
            emptyList(),
            singletonList(desiredIndexDefinition),
            singletonList(desiredAnalyzerDefinition));

    assertNoAddedIndexes(plan);
    assertIndexUnmodified(indexId, plan);
    assertNoModifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
  }

  @Test
  public void testSingleIndexExistsModifiedIndexDesiredMappingsProducesModification()
      throws Exception {
    var indexId = new ObjectId();

    // Start with dynamic mappings.
    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings from dynamic to not dynamic.
    var desiredDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(false).build())
            .build();

    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "mappings have changed");
  }

  @Test
  public void testSingleIndexExistsModifiedTokenFieldDefinitionProducesModification()
      throws Exception {
    var indexId = new ObjectId();

    // Start with dynamic mappings.
    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        "a",
                        FieldDefinitionBuilder.builder()
                            .token(TokenFieldDefinitionBuilder.builder().build())
                            .build())
                    .build())
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings from dynamic to not dynamic.
    var desiredDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        "a",
                        FieldDefinitionBuilder.builder()
                            .token(
                                TokenFieldDefinitionBuilder.builder()
                                    .normalizerName(StockNormalizerName.LOWERCASE)
                                    .build())
                            .build())
                    .build())
            .build();

    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "mappings have changed");
  }

  @Test
  public void testSingleIndexExistsModifiedViewDefinitionProducesModification() throws Exception {
    var indexId = new ObjectId();

    // Start with dynamic mappings.
    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .view(
                ViewDefinition.existing(
                    "test", List.of(new BsonDocument("key", new BsonString("value1")))))
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        "a",
                        FieldDefinitionBuilder.builder()
                            .token(TokenFieldDefinitionBuilder.builder().build())
                            .build())
                    .build())
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings from dynamic to not dynamic.
    var desiredDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .view(
                ViewDefinition.existing(
                    "test", List.of(new BsonDocument("key", new BsonString("value2")))))
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        "a",
                        FieldDefinitionBuilder.builder()
                            .token(
                                TokenFieldDefinitionBuilder.builder()
                                    .normalizerName(StockNormalizerName.LOWERCASE)
                                    .build())
                            .build())
                    .build())
            .build();

    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "view has changed");
  }

  @Test
  public void testSingleIndexExistsModifiedIndexDesiredIndexFeatureVersionProducesModification()
      throws Exception {
    var indexId = new ObjectId();
    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexFeatureVersion(0)
            .indexId(indexId)
            .dynamicMapping()
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // bump indexFeatureVersion
    var desiredDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexFeatureVersion(1)
            .indexId(indexId)
            .dynamicMapping()
            .build();

    var plan =
        mocks.plan(
            emptyList(),
            singletonList(desiredDefinition),
            mockAnalyzerDefinitions(desiredDefinition));

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertIndexModified(desiredDefinition, plan);
    assertNoDroppedIndexes(plan);
  }

  @Test
  public void testSingleSearchIndexExistsModifiedIndexDesiredNumSubFieldsProducesModification()
      throws Exception {
    var indexId = new ObjectId();
    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .numPartitions(1)
            .indexId(indexId)
            .dynamicMapping()
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // change numPartitions
    var desiredDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .numPartitions(2)
            .indexId(indexId)
            .dynamicMapping()
            .build();

    var plan =
        mocks.plan(
            emptyList(),
            singletonList(desiredDefinition),
            mockAnalyzerDefinitions(desiredDefinition));

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertIndexModified(desiredDefinition, plan);
    assertNoDroppedIndexes(plan);
  }

  @Test
  public void testSingleVectorIndexExistsModifiedIndexDesiredNumSubFieldsProducesModification()
      throws Exception {
    var indexId = new ObjectId();
    var originalDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withCosineVectorField("vector", 1024)
            .withFilterPath("a.b.c")
            .numPartitions(1)
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // change numPartitions
    var desiredDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withCosineVectorField("vector", 1024)
            .withFilterPath("a.b.c")
            .numPartitions(2)
            .build();

    var plan = mocks.plan(singletonList(desiredDefinition), emptyList(), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertIndexModified(desiredDefinition, plan);
    assertNoDroppedIndexes(plan);
  }

  @Test
  public void testSingleIndexExistsModifiedIndexDesiredAnalyzerProducesModification()
      throws Exception {
    var indexId = new ObjectId();
    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .dynamicMapping()
            .analyzerName("original")
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the analyzer.
    var desiredDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .dynamicMapping()
            .analyzerName("changed")
            .build();

    var plan =
        mocks.plan(
            emptyList(),
            singletonList(desiredDefinition),
            mockAnalyzerDefinitions(desiredDefinition));

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertIndexModified(desiredDefinition, plan);
    assertNoDroppedIndexes(plan);
  }

  @Test
  public void testSingleIndexExistsModifiedIndexDesiredSearchAnalyzerProducesModification()
      throws Exception {
    var indexId = new ObjectId();

    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .dynamicMapping()
            .searchAnalyzerName("original")
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings from dynamic to not dynamic.
    var desiredDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .dynamicMapping()
            .searchAnalyzerName("changed")
            .build();

    var plan =
        mocks.plan(
            emptyList(),
            singletonList(desiredDefinition),
            mockAnalyzerDefinitions(desiredDefinition));

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertIndexModified(desiredDefinition, plan);
    assertNoDroppedIndexes(plan);
  }

  @Test
  public void
      testSingleIndexWithAnalyzerExistsModifiedIndexDesiredAnalyzerDefinitionProducesModification()
          throws Exception {

    var indexId = new ObjectId();
    Supplier<SearchIndexDefinitionBuilder> builder =
        () ->
            SearchIndexDefinitionBuilder.builder()
                .defaultMetadata()
                .indexId(indexId)
                .analyzerName("foo")
                .dynamicMapping();

    // Create an object representing the definitions already existing.
    var originalIndexDefinition = builder.get().build();
    var originalAnalyzerDefinition =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("foo")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
            .build();
    var mocks = Mocks.withIndexAndAnalyzers(originalIndexDefinition, originalAnalyzerDefinition);

    var desiredIndexDefinition = builder.get().build();
    // Add some stopwords to the analyzer.
    var desiredAnalyzerDefinition =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("foo")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
            .stopword("stop")
            .build();
    var plan =
        mocks.plan(
            emptyList(),
            singletonList(desiredIndexDefinition),
            singletonList(desiredAnalyzerDefinition));

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertIndexModified(desiredIndexDefinition, plan);
    assertNoDroppedIndexes(plan);
  }

  @Test
  public void testSingleVectorIndexExistsModifiedIndexDesiredFilterProducesModification()
      throws Exception {
    var indexId = new ObjectId();

    var originalDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withCosineVectorField("vector", 1024)
            .withFilterPath("a.b.c")
            .build();

    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings from dynamic to not dynamic.
    var desiredDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withCosineVectorField("vector", 1024)
            .withFilterPath("a.b.X")
            .build();

    var plan = mocks.plan(singletonList(desiredDefinition), emptyList(), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "vector fields or filters have changed");
  }

  @Test
  public void testSingleVectorIndexExistsModifiedIndexDefinitionVersionProducesModification()
      throws Exception {
    var indexId = new ObjectId();

    var originalDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withCosineVectorField("vector", 1024)
            .withFilterPath("a.b.c")
            .withDefinitionVersion(Optional.of(3L))
            .build();

    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings from dynamic to not dynamic.
    var desiredDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withCosineVectorField("vector", 1024)
            .withFilterPath("a.b.c")
            .withDefinitionVersion(Optional.of(4L))
            .build();

    var plan = mocks.plan(singletonList(desiredDefinition), emptyList(), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "definitionVersion has changed");
  }

  @Test
  public void testSingleVectorIndexExistsModifiedIndexDefinitionVersionProducesModification2()
      throws Exception {
    var indexId = new ObjectId();

    var originalDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withCosineVectorField("vector", 1024)
            .withFilterPath("a.b.c")
            .withDefinitionVersion(Optional.of(0L))
            .build();

    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings from dynamic to not dynamic.
    var desiredDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withCosineVectorField("vector", 1024)
            .withFilterPath("a.b.c")
            .withDefinitionVersion(Optional.of(1L))
            .build();

    var plan = mocks.plan(singletonList(desiredDefinition), emptyList(), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "definitionVersion has changed");
  }

  @Test
  public void
      testSingleVectorIndexExistsModifiedAutoEmbeddingDefinitionVersionProducesModification()
          throws Exception {
    var indexId = new ObjectId();

    var originalDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withCosineVectorField("vector", 1024)
            .autoEmbeddingDefinitionVersion(1L)
            .build();

    var mocks = Mocks.withIndexes(originalDefinition);

    // Bump only the autoEmbeddingDefinitionVersion.
    var desiredDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withCosineVectorField("vector", 1024)
            .autoEmbeddingDefinitionVersion(2L)
            .build();

    var plan = mocks.plan(singletonList(desiredDefinition), emptyList(), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "autoEmbeddingDefinitionVersion has changed");
  }

  @Test
  public void testSingleVectorIndexExistsFirstAutoEmbeddingDefinitionVersionProducesModification()
      throws Exception {
    var indexId = new ObjectId();

    // The existing index has no autoEmbeddingDefinitionVersion set.
    var originalDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withCosineVectorField("vector", 1024)
            .build();

    var mocks = Mocks.withIndexes(originalDefinition);

    var desiredDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withCosineVectorField("vector", 1024)
            .autoEmbeddingDefinitionVersion(1L)
            .build();

    var plan = mocks.plan(singletonList(desiredDefinition), emptyList(), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "autoEmbeddingDefinitionVersion has changed");
  }

  @Test
  public void
      testSingleVectorIndexExistsDesiredAutoEmbeddingDefinitionVersionZeroProducesNoChanges()
          throws Exception {
    var indexId = new ObjectId();

    // The existing index has no autoEmbeddingDefinitionVersion set; an explicit 0 is equivalent.
    var originalDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withCosineVectorField("vector", 1024)
            .build();

    var mocks = Mocks.withIndexes(originalDefinition);

    var desiredDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withCosineVectorField("vector", 1024)
            .autoEmbeddingDefinitionVersion(0L)
            .build();

    var plan = mocks.plan(singletonList(desiredDefinition), emptyList(), emptyList());

    assertNoAddedIndexes(plan);
    assertIndexUnmodified(indexId, plan);
    assertNoModifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    Assert.assertFalse(plan.hasChanges());
  }

  @Test
  public void
      testSingleSearchIndexExistsModifiedAutoEmbeddingDefinitionVersionProducesModification()
          throws Exception {
    var indexId = new ObjectId();

    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .dynamicMapping()
            .autoEmbeddingDefinitionVersion(1L)
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Bump only the autoEmbeddingDefinitionVersion.
    var desiredDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .dynamicMapping()
            .autoEmbeddingDefinitionVersion(2L)
            .build();

    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "autoEmbeddingDefinitionVersion has changed");
  }

  @Test
  public void testSingleSearchIndexExistsNoIndexesDesiredProducesDrop() throws Exception {
    var indexId = new ObjectId();

    var indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .dynamicMapping()
            .build();
    var mocks = Mocks.withIndexes(indexDefinition);

    var plan = mocks.plan(emptyList(), emptyList(), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoModifiedIndexes(plan);
    assertIndexDropped(indexId, plan);
  }

  @Test
  public void testSingleVectorIndexExistsNoIndexesDesiredProducesDrop() throws Exception {
    var indexId = new ObjectId();

    var indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .withCosineVectorField("vector", 1024)
            .withFilterPath("a.b.c")
            .build();
    var mocks = Mocks.withIndexes(indexDefinition);

    var plan = mocks.plan(emptyList(), emptyList(), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoModifiedIndexes(plan);
    assertIndexDropped(indexId, plan);
  }

  @Test
  public void testSingleSearchIndexExistsModifiedIndexDefinitionVersionProducesModification()
      throws Exception {
    var indexId = new ObjectId();

    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .definitionVersion(1L)
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    var desiredDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .definitionVersion(2L)
            .build();

    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "definitionVersion has changed");
  }

  @Test
  public void
      testSingleSearchIndexExistsModifiedIndexMappingsAndSameDefinitionVersionProducesModification()
          throws Exception {
    var indexId = new ObjectId();

    // Start with dynamic mappings.
    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings and definitionVersion.
    var desiredDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(false).build())
            .definitionVersion(3L)
            .build();

    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "mappings have changed");
    Assert.assertTrue(modified.getReasons().contains("definitionVersion has changed"));
  }

  @Test
  public void
      testSingleIndexWithCustomAnalyzerExistsModifiedDesiredCustomAnalyzerProducesModification()
          throws Exception {

    var indexId = new ObjectId();
    Function<List<CustomAnalyzerDefinition>, SearchIndexDefinitionBuilder> builder =
        (analyzers) ->
            SearchIndexDefinitionBuilder.builder()
                .defaultMetadata()
                .indexId(indexId)
                .analyzerName("bar")
                .analyzers(analyzers)
                .dynamicMapping();

    Supplier<CustomAnalyzerDefinitionBuilder> analyzerBuilder =
        () ->
            CustomAnalyzerDefinitionBuilder.builder(
                "bar", TokenizerDefinitionBuilder.StandardTokenizer.builder().build());

    // Create an object representing the definitions already existing.
    var originalIndexDefinition = builder.apply(List.of(analyzerBuilder.get().build())).build();
    var overriddenBaseAnalyzerDefinition =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .name("foo")
            .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
            .build();
    var mocks =
        Mocks.withIndexAndAnalyzers(originalIndexDefinition, overriddenBaseAnalyzerDefinition);

    // add a length token filter
    var desiredCustomAnalyzers =
        List.of(
            analyzerBuilder
                .get()
                .tokenFilter(TokenFilterDefinitionBuilder.LengthTokenFilter.builder().build())
                .build());

    var desiredIndexDefinition = builder.apply(desiredCustomAnalyzers).build();

    var plan =
        mocks.plan(
            emptyList(),
            singletonList(desiredIndexDefinition),
            singletonList(overriddenBaseAnalyzerDefinition));

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertIndexModified(desiredIndexDefinition, plan);
    assertNoDroppedIndexes(plan);
  }

  @Test
  public void testSingleIndexNoSynonymsModifiedDesiredWithSynonymsProducesModification()
      throws Exception {
    var indexId = new ObjectId();

    // Start with dynamic mappings.
    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings to include a synonym mapping definition.
    var desiredDefinition =
        SearchIndexDefinitionBuilder.from(originalDefinition)
            .synonyms(
                SynonymMappingDefinitionBuilder.builder()
                    .name("mySynonyms")
                    .analyzer("lucene.standard")
                    .synonymSourceDefinition("synonymCollection")
                    .buildAsList())
            .build();

    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "synonyms have changed");
  }

  @Test
  public void testSingleIndexHasSynonymsModifiedDesiredSynonymsProducesModification()
      throws Exception {
    var indexId = new ObjectId();

    // Start with dynamic mappings and a synonym mapping definition.
    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .synonyms(
                SynonymMappingDefinitionBuilder.builder()
                    .name("mySynonyms")
                    .analyzer("lucene.english")
                    .synonymSourceDefinition("synonymCollection")
                    .buildAsList())
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings to modify the synonym mapping definition.
    var desiredDefinition =
        SearchIndexDefinitionBuilder.from(originalDefinition)
            .synonyms(
                SynonymMappingDefinitionBuilder.builder()
                    .name("mySynonyms")
                    .analyzer("lucene.standard")
                    .synonymSourceDefinition("synonymCollection")
                    .buildAsList())
            .build();

    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "synonyms have changed");
  }

  @Test
  public void testSingleIndexSynonymsRemovedProducesModification() throws Exception {
    var indexId = new ObjectId();

    // Start with dynamic mappings and a synonym mapping definition.
    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .synonyms(
                SynonymMappingDefinitionBuilder.builder()
                    .name("mySynonyms")
                    .analyzer("lucene.english")
                    .synonymSourceDefinition("synonymCollection")
                    .buildAsList())
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings, removing the synonym mapping definition.
    var desiredDefinition =
        SearchIndexDefinitionBuilder.from(originalDefinition)
            .synonyms(Collections.emptyList())
            .build();

    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "synonyms have changed");
  }

  @Test
  public void testSingleIndexNoStoredSourceModifiedDesiredWithStoredSourceProducesModification()
      throws Exception {
    var indexId = new ObjectId();

    // Start with dynamic mappings.
    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings to include a stored source definition.
    var desiredDefinition =
        SearchIndexDefinitionBuilder.from(originalDefinition)
            .storedSource(
                StoredSourceDefinition.create(
                    StoredSourceDefinition.Mode.INCLUSION, List.of("a", "b.c")))
            .build();

    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "storedSource has changed");
  }

  @Test
  public void testSingleIndexHasStoredSourceModifiedDesiredStoredSourceProducesModification()
      throws Exception {
    var indexId = new ObjectId();

    // Start with dynamic mappings and a stored source definition.
    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .storedSource(
                StoredSourceDefinition.create(
                    StoredSourceDefinition.Mode.INCLUSION, List.of("a", "b.c")))
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings to modify the stored source definition.
    var desiredDefinition =
        SearchIndexDefinitionBuilder.from(originalDefinition)
            .storedSource(
                StoredSourceDefinition.create(
                    StoredSourceDefinition.Mode.INCLUSION, List.of("a", "b.c.d")))
            .build();

    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "storedSource has changed");
  }

  @Test
  public void testSingleIndexStoredSourceRemovedProducesModification() throws Exception {
    var indexId = new ObjectId();

    // Start with dynamic mappings and a stored source definition.
    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .storedSource(
                StoredSourceDefinition.create(
                    StoredSourceDefinition.Mode.INCLUSION, List.of("a", "b.c")))
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings, removing the stored source definition.
    var desiredDefinition = SearchIndexDefinitionBuilder.from(originalDefinition).build();

    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "storedSource has changed");
  }

  @Test
  public void testSingleIndexStoredAllReplacedWithStoredSpecificPathsProducesModification()
      throws Exception {
    var indexId = new ObjectId();

    // Start with dynamic mappings and a stored source definition which includes the whole document
    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .storedSource(StoredSourceDefinition.createIncludeAll())
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings, adding specific paths instead of "include all"
    var desiredDefinition =
        SearchIndexDefinitionBuilder.from(originalDefinition)
            .storedSource(
                StoredSourceDefinition.create(
                    StoredSourceDefinition.Mode.INCLUSION, List.of("a", "b.c")))
            .build();

    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "storedSource has changed");
  }

  @Test
  public void testSingleIndexStoredSpecificPathsReplacedWithIncludeAllProducesModification()
      throws Exception {
    var indexId = new ObjectId();

    // Start with dynamic mappings and a stored source definition which includes specific paths
    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .storedSource(
                StoredSourceDefinition.create(
                    StoredSourceDefinition.Mode.INCLUSION, List.of("a", "b.c")))
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings, replacing stored source with "include all" setting
    var desiredDefinition =
        SearchIndexDefinitionBuilder.from(originalDefinition)
            .storedSource(StoredSourceDefinition.createIncludeAll())
            .build();

    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoUnmodifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
    var modified = assertIndexModified(desiredDefinition, plan);

    Assert.assertEquals(
        ModifiedIndexInformation.Type.DIFFERENT_FROM_LIVE_NO_STAGED, modified.getType());
    TestUtils.assertContains(modified.getReasons(), "storedSource has changed");
  }

  @Test
  public void testSingleIndexStoredReplacedWithDefaultOptionDoesNotProduceModification()
      throws Exception {
    var indexId = new ObjectId();

    // Start with dynamic mappings.
    var originalDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
            .build();
    var mocks = Mocks.withIndexes(originalDefinition);

    // Change the mappings, adding the default stored source definition.
    var desiredDefinition =
        SearchIndexDefinitionBuilder.from(originalDefinition)
            .storedSource(StoredSourceDefinition.defaultValue())
            .build();

    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertNoAddedIndexes(plan);
    assertNoModifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
  }

  private static void assertNoAddedIndexes(ConfigManagerChangePlan plan) {
    Assert.assertTrue(plan.addedIndexDefinitions().isEmpty());
  }

  private static void assertIndexAdded(IndexDefinition definition, ConfigManagerChangePlan plan) {
    assertIndexInList(
        plan.addedIndexDefinitions().stream()
            .map(IndexDefinitionGenerationProducer::getIndexDefinition)
            .collect(Collectors.toList()),
        definition);
  }

  private static void assertNoUnmodifiedIndexes(ConfigManagerChangePlan plan) {
    Assert.assertTrue(plan.unmodifiedIndexes().isEmpty());
  }

  private static void assertIndexUnmodified(ObjectId id, ConfigManagerChangePlan plan) {
    TestUtils.assertContains(plan.unmodifiedIndexes(), id);
  }

  private static void assertNoModifiedIndexes(ConfigManagerChangePlan plan) {
    Assert.assertTrue(plan.modifiedIndexes().isEmpty());
  }

  private static ModifiedIndexInformation assertIndexModified(
      IndexDefinition definition, ConfigManagerChangePlan plan) {
    assertIndexInList(
        plan.modifiedIndexes().stream()
            .map(ModifiedIndexInformation::getDesiredDefinition)
            .map(IndexDefinitionGenerationProducer::getIndexDefinition)
            .collect(Collectors.toList()),
        definition);

    return plan.modifiedIndexes().stream()
        .filter(modification -> modification.getIndexId().equals(definition.getIndexId()))
        .findFirst()
        .orElseThrow();
  }

  private static void assertNoDroppedIndexes(ConfigManagerChangePlan plan) {
    Assert.assertTrue(plan.droppedIndexes().isEmpty());
  }

  private static void assertIndexDropped(ObjectId id, ConfigManagerChangePlan plan) {
    Assert.assertTrue(plan.droppedIndexes().contains(id));
  }

  private static void assertIndexInList(
      List<? extends IndexDefinition> definitions, IndexDefinition desiredDefinition) {
    var found =
        definitions.stream()
            .filter(d -> d.getIndexId().equals(desiredDefinition.getIndexId()))
            .collect(Collectors.toList());

    Assert.assertEquals(1, found.size());
  }

  private static class Mocks {
    final ConfigStateMocks configStateMocks;
    final IndexCatalog indexCatalog;

    private Mocks() throws Exception {
      this.configStateMocks = ConfigStateMocks.create();
      this.indexCatalog = this.configStateMocks.indexCatalog;
    }

    static Mocks withIndexes(IndexDefinition... indexDefinitions) throws Exception {
      var mocks = new Mocks();
      Arrays.stream(indexDefinitions)
          .forEach(
              definition -> {
                switch (definition.getType()) {
                  case SEARCH:
                    mocks.indexCatalog.addIndex(
                        IndexGeneration.mockIndexGeneration(definition.asSearchDefinition()));
                    return;
                  case VECTOR_SEARCH:
                    mocks.indexCatalog.addIndex(
                        IndexGeneration.mockIndexGeneration(definition.asVectorDefinition()));
                }
              });

      return mocks;
    }

    static Mocks withIndexAndAnalyzers(
        SearchIndexDefinition indexDefinition,
        OverriddenBaseAnalyzerDefinition... analyzerDefinitions)
        throws Exception {
      var mocks = new Mocks();
      mocks.indexCatalog.addIndex(
          IndexGeneration.mockIndexGeneration(indexDefinition, List.of(analyzerDefinitions)));
      return mocks;
    }

    private static Mocks withStagedSearchIndex(ObjectId indexId) throws Exception {
      // with two indexes, a live one and a staged one with slightly different definition.

      // Create an object representing the index definition already existing.
      var originalDefinition =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .indexId(indexId)
              .dynamicMapping()
              .build();
      var mocks = Mocks.withIndexes(originalDefinition);

      mocks.configStateMocks.addIndex(
          SearchIndexDefinitionGenerationBuilder.create(
              SearchIndexDefinitionBuilder.from(originalDefinition)
                  .analyzerName("lucene.keyword")
                  .build(),
              mocks
                  .indexCatalog
                  .getIndexById(indexId)
                  .orElseThrow()
                  .getDefinitionGeneration()
                  .getGenerationId()
                  .generation
                  .incrementUser(),
              emptyList()),
          ConfigStateMocks.State.STAGED);
      return mocks;
    }

    private static Mocks withStagedVectorIndex(ObjectId indexId) throws Exception {
      // with two indexes, a live one and a staged one with slightly different definition.

      // Create an object representing the index definition already existing.
      var originalDefinition =
          VectorIndexDefinitionBuilder.builder()
              .indexId(indexId)
              .withCosineVectorField("vector", 1024)
              .build();

      var mocks = Mocks.withIndexes(originalDefinition);

      mocks.configStateMocks.addIndex(
          new VectorIndexDefinitionGeneration(
              VectorIndexDefinitionBuilder.from(originalDefinition)
                  .withCosineVectorField("vector2", 1024)
                  .build(),
              mocks
                  .indexCatalog
                  .getIndexById(indexId)
                  .orElseThrow()
                  .getDefinitionGeneration()
                  .getGenerationId()
                  .generation
                  .incrementUser()),
          ConfigStateMocks.State.STAGED);

      return mocks;
    }

    ConfigManagerChangePlan plan(
        List<VectorIndexDefinition> desiredVectorIndexes,
        List<SearchIndexDefinition> desiredSearchIndexes,
        List<OverriddenBaseAnalyzerDefinition> desiredAnalyzers) {
      List<IndexDefinitionGenerationProducer> producers =
          IndexDefinitionGenerationProducer.createProducers(
              desiredVectorIndexes, desiredSearchIndexes, desiredAnalyzers);
      return ConfigManagerChangePlanner.plan(
          this.configStateMocks.configState, producers, Set.of());
    }

    ConfigManagerChangePlan planWithUnreadableDesired(
        List<VectorIndexDefinition> desiredVectorIndexes,
        List<SearchIndexDefinition> desiredSearchIndexes,
        Set<ObjectId> unreadableDesiredIds) {
      List<IndexDefinitionGenerationProducer> producers =
          IndexDefinitionGenerationProducer.createProducers(
              desiredVectorIndexes, desiredSearchIndexes, emptyList());
      return ConfigManagerChangePlanner.plan(
          this.configStateMocks.configState, producers, unreadableDesiredIds);
    }
  }

  private void testDefinitionVersionChanges(
      Consumer<SearchIndexDefinitionBuilder> originalSearchIndexDefinitionBuilderConsumer,
      Consumer<SearchIndexDefinitionBuilder> desiredSearchIndexDefinitionBuilderConsumer,
      BiConsumer<SearchIndexDefinition, ConfigManagerChangePlan> assertions)
      throws Exception {
    var originalSearchIndexDefinitionBuilder =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().indexId(INDEX_ID).dynamicMapping();
    originalSearchIndexDefinitionBuilderConsumer.accept(originalSearchIndexDefinitionBuilder);
    var originalDefinition = originalSearchIndexDefinitionBuilder.build();

    var desiredSearchIndexDefinitionBuilder =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().indexId(INDEX_ID).dynamicMapping();
    desiredSearchIndexDefinitionBuilderConsumer.accept(desiredSearchIndexDefinitionBuilder);
    var desiredDefinition = desiredSearchIndexDefinitionBuilder.build();

    var mocks = Mocks.withIndexes(originalDefinition);

    var plan = mocks.plan(emptyList(), singletonList(desiredDefinition), emptyList());

    assertions.accept(desiredDefinition, plan);
  }

  private static void assertChangedDefinitionVersion(
      SearchIndexDefinition desiredDefinition, ConfigManagerChangePlan plan) {
    assertNoAddedIndexes(plan);
    assertIndexModified(desiredDefinition, plan);
    assertNoDroppedIndexes(plan);
    Assert.assertTrue(plan.hasChanges());
  }

  private static void assertUnchangedDefinitionVersion(ConfigManagerChangePlan plan) {
    Assert.assertFalse(plan.hasChanges());
    assertNoAddedIndexes(plan);
    assertIndexUnmodified(INDEX_ID, plan);
    assertNoModifiedIndexes(plan);
    assertNoDroppedIndexes(plan);
  }
}
