package com.xgen.mongot.replication.mongodb.common;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Var;
import com.mongodb.client.model.changestream.TruncatedArray;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(value = {ChangeStreamsTest.UpdateDescriptionAppliesToIndexTest.class})
public class ChangeStreamsTest {

  @RunWith(Theories.class)
  public static class UpdateDescriptionAppliesToIndexTest {

    @DataPoints
    public static IndexDefinition.Type[] type() {
      return new IndexDefinition.Type[] {
        IndexDefinition.Type.SEARCH, IndexDefinition.Type.VECTOR_SEARCH
      };
    }

    private static final SearchIndexDefinition DYNAMIC_DEFINITION =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping().build();

    @Theory
    public void testNullFields(IndexDefinition.Type type) {
      var description = new UpdateDescription(null, null);
      assertDoesNotApplyToDefinition(description, DYNAMIC_DEFINITION);
      assertDoesNotApplyToDefinition(description, createDefinition(type, "a", "bb"));
    }

    @Test
    public void testDynamicMatchingRemovedField() {
      var description = new UpdateDescription(List.of("a.bb"), new BsonDocument());
      assertAppliesToDefinition(description, DYNAMIC_DEFINITION);
    }

    @Test
    public void testDynamicMatchingUpdatedField() {
      var description =
          new UpdateDescription(
              Collections.emptyList(), new BsonDocument("a.bb", new BsonString("new")));
      assertAppliesToDefinition(description, DYNAMIC_DEFINITION);
    }

    @Test
    public void testDynamicMatchingTruncatedArray() {
      var description =
          new UpdateDescription(
              Collections.emptyList(), new BsonDocument(), List.of(new TruncatedArray("a.bb", 2)));
      assertAppliesToDefinition(description, DYNAMIC_DEFINITION);
    }

    @Test
    public void searchAutoEmbedSourceField_staticExactUpdate_applies() {
      SearchIndexDefinition definition =
          createSearchAutoEmbedDefinition(false, FieldPath.parse("content.body"));
      var description =
          new UpdateDescription(
              Collections.emptyList(), new BsonDocument("content.body", new BsonString("updated")));

      assertAppliesToDefinition(description, definition);
    }

    @Test
    public void searchAutoEmbedSourceField_dynamicExactUpdate_applies() {
      SearchIndexDefinition definition =
          createSearchAutoEmbedDefinition(true, FieldPath.parse("content.body"));
      var description =
          new UpdateDescription(
              Collections.emptyList(), new BsonDocument("content.body", new BsonString("updated")));

      assertAppliesToDefinition(description, definition);
    }

    @Test
    public void searchAutoEmbedSourceField_nestedDeclaration_applies() {
      SearchIndexDefinition definition =
          createNestedSearchAutoEmbedDefinition(FieldPath.parse("content.body"));
      var description =
          new UpdateDescription(
              Collections.emptyList(), new BsonDocument("content.body", new BsonString("updated")));

      assertAppliesToDefinition(description, definition);
    }

    @Test
    public void searchAutoEmbedSourceField_ancestorUpdate_applies() {
      SearchIndexDefinition definition =
          createSearchAutoEmbedDefinition(false, FieldPath.parse("content.body"));
      var description =
          new UpdateDescription(
              Collections.emptyList(), new BsonDocument("content", new BsonString("updated")));

      assertAppliesToDefinition(description, definition);
    }

    @Test
    public void searchAutoEmbedSourceField_descendantUpdate_applies() {
      SearchIndexDefinition definition =
          createSearchAutoEmbedDefinition(false, FieldPath.parse("content"));
      var description =
          new UpdateDescription(
              Collections.emptyList(), new BsonDocument("content.body", new BsonString("updated")));

      assertAppliesToDefinition(description, definition);
    }

    @Test
    public void searchAutoEmbedSourceField_numericArrayUpdate_applies() {
      SearchIndexDefinition definition =
          createSearchAutoEmbedDefinition(false, FieldPath.parse("items.content"));
      var description =
          new UpdateDescription(
              Collections.emptyList(),
              new BsonDocument("items.0.content", new BsonString("updated")));

      assertAppliesToDefinition(description, definition);
    }

    @Test
    public void searchAutoEmbedSourceField_removed_applies() {
      SearchIndexDefinition definition =
          createSearchAutoEmbedDefinition(false, FieldPath.parse("content.body"));
      var description = new UpdateDescription(List.of("content.body"), new BsonDocument());

      assertAppliesToDefinition(description, definition);
    }

    @Test
    public void searchAutoEmbedSourceField_truncatedArray_applies() {
      SearchIndexDefinition definition =
          createSearchAutoEmbedDefinition(false, FieldPath.parse("content.body"));
      var description =
          new UpdateDescription(
              Collections.emptyList(),
              new BsonDocument(),
              List.of(new TruncatedArray("content.body", 1)));

      assertAppliesToDefinition(description, definition);
    }

    @Test
    public void searchAutoEmbedSourceField_prefixCollision_doesNotApply() {
      SearchIndexDefinition definition =
          createSearchAutoEmbedDefinition(false, FieldPath.parse("content.body"));
      var description =
          new UpdateDescription(
              Collections.emptyList(),
              new BsonDocument("content.bodyguard", new BsonString("updated")));

      assertDoesNotApplyToDefinition(description, definition);
    }

    @Theory
    public void testStaticMatchingRemovedField(IndexDefinition.Type type) {
      var description = new UpdateDescription(List.of("a.bb"), new BsonDocument());
      assertAppliesToDefinition(description, createDefinition(type, "a", "bb"));
    }

    @Theory
    public void testStaticMatchingUpdatedField(IndexDefinition.Type type) {
      var description =
          new UpdateDescription(
              Collections.emptyList(), new BsonDocument("a.bb", new BsonString("new")));
      assertAppliesToDefinition(description, createDefinition(type, "a", "bb"));
    }

    @Theory
    public void testStaticMatchingTruncatedArray(IndexDefinition.Type type) {
      var description =
          new UpdateDescription(
              Collections.emptyList(), new BsonDocument(), List.of(new TruncatedArray("a.bb", 2)));
      assertAppliesToDefinition(description, createDefinition(type, "a", "bb"));
    }

    @Theory
    public void testStaticMatchingRemovedFieldParent(IndexDefinition.Type type) {
      var description = new UpdateDescription(List.of("a"), new BsonDocument());
      assertAppliesToDefinition(description, createDefinition(type, "a", "bb"));
    }

    @Theory
    public void testStaticNotMatching(IndexDefinition.Type type) {
      var description =
          new UpdateDescription(
              List.of("a.bb.ccc"),
              new BsonDocument("other", new BsonString("new")),
              List.of(new TruncatedArray("a.bb.dddd", 2)));
      assertDoesNotApplyToDefinition(description, createDefinition(type, "a", "bb"));
    }

    @Theory
    public void testEmptyLeadingFieldElement(IndexDefinition.Type type) {
      var description = new UpdateDescription(List.of(""), null);
      assertAppliesToDefinition(description, DYNAMIC_DEFINITION);
      assertAppliesToDefinition(description, createDefinition(type, ""));
      assertAppliesToDefinition(description, createDefinition(type, "", "bb"));
      assertDoesNotApplyToDefinition(description, createDefinition(type, "a", "bb"));
    }

    @Theory
    public void testEmptyFieldElement(IndexDefinition.Type type) {
      var description = new UpdateDescription(List.of("a.bb..ccc"), null);
      assertDoesNotApplyToDefinition(description, createDefinition(type, "a", "bb"));
    }

    @Theory
    public void testIntegerLikeFieldElement(IndexDefinition.Type type) {
      var updateUpdateDescription = new UpdateDescription(List.of("a.bb.13z"), null);
      var removeUpdateDescription =
          new UpdateDescription(null, new BsonDocument("a.bb.13z", new BsonString("new")));

      // We should only not filter out the event if the exact field is indexed.
      var exactPathDefinition = createDefinition(type, "a", "bb", "13z");
      assertAppliesToDefinition(updateUpdateDescription, exactPathDefinition);
      assertAppliesToDefinition(removeUpdateDescription, exactPathDefinition);

      var parentConfiguredDefinition = createDefinition(type, "a", "bb");
      assertDoesNotApplyToDefinition(updateUpdateDescription, parentConfiguredDefinition);
      assertDoesNotApplyToDefinition(removeUpdateDescription, parentConfiguredDefinition);

      var parentNotConfiguredDefinition = createDefinition(type, "a");
      assertDoesNotApplyToDefinition(updateUpdateDescription, parentNotConfiguredDefinition);
      assertDoesNotApplyToDefinition(removeUpdateDescription, parentNotConfiguredDefinition);
    }

    @Theory
    public void testLeadingIntegerFieldElement(IndexDefinition.Type type) {
      var definition = createDefinition(type, "123");

      // Should match an exact match on the field name.
      assertAppliesToDefinition(new UpdateDescription(List.of("123"), null), definition);
      assertAppliesToDefinition(
          new UpdateDescription(null, new BsonDocument("123", new BsonString("new"))), definition);

      // Should not match a prefix of the field name.
      assertDoesNotApplyToDefinition(new UpdateDescription(List.of("12"), null), definition);
      assertDoesNotApplyToDefinition(
          new UpdateDescription(null, new BsonDocument("12", new BsonString("new"))), definition);

      // Should not match a field name with the configured field as a prefix.
      assertDoesNotApplyToDefinition(new UpdateDescription(List.of("1234"), null), definition);
      assertDoesNotApplyToDefinition(
          new UpdateDescription(null, new BsonDocument("1234", new BsonString("new"))), definition);
    }

    @Theory
    public void testSingleIntegerFieldElementNoChildren(IndexDefinition.Type type) {
      var removeUpdateDescription = new UpdateDescription(List.of("a.bb.3"), null);
      var updateUpdateDescription =
          new UpdateDescription(null, new BsonDocument("a.bb.3", new BsonString("new")));

      // If the exact path including the integer field is configured to be indexed, we should not
      // filter out the event.
      var exactPathDefinition = createDefinition(type, "a", "bb", "3");
      assertAppliesToDefinition(updateUpdateDescription, exactPathDefinition);
      assertAppliesToDefinition(removeUpdateDescription, exactPathDefinition);

      // If just the parent of the integer field element is configured to be indexed, we should also
      // not filter out the event.
      var parentConfiguredDefinition = createDefinition(type, "a", "bb");
      assertAppliesToDefinition(updateUpdateDescription, parentConfiguredDefinition);
      assertAppliesToDefinition(removeUpdateDescription, parentConfiguredDefinition);

      // If the parent of the integer field path is not configured to be indexed, we can filter out
      // the event.
      var parentNotConfiguredDefinition = createDefinition(type, "a");
      assertDoesNotApplyToDefinition(updateUpdateDescription, parentNotConfiguredDefinition);
      assertDoesNotApplyToDefinition(removeUpdateDescription, parentNotConfiguredDefinition);
    }

    @Theory
    public void testSingleIntegerFieldElementWithChildren(IndexDefinition.Type type) {
      var removeUpdateDescription = new UpdateDescription(List.of("a.bb.13.ccc"), null);
      var updateUpdateDescription =
          new UpdateDescription(null, new BsonDocument("a.bb.13.ccc", new BsonString("new")));

      // Even if only a sibling of the child is configured to be indexed, we should still not filter
      // out the event.
      var siblingPathDefinition = createDefinition(type, "a", "bb", "ddd");
      assertAppliesToDefinition(updateUpdateDescription, siblingPathDefinition);
      assertAppliesToDefinition(removeUpdateDescription, siblingPathDefinition);

      // If the exact path including the integer field and its child is configured to be indexed, we
      // should not filter out the event.
      var exactPathDefinition = createDefinition(type, "a", "bb", "13", "ccc");
      assertAppliesToDefinition(updateUpdateDescription, exactPathDefinition);
      assertAppliesToDefinition(removeUpdateDescription, exactPathDefinition);

      // If the path including the integer field but without its child is configured to be indexed,
      // we should not filter out the event.
      var integerPathDefinition = createDefinition(type, "a", "bb", "13");
      assertAppliesToDefinition(updateUpdateDescription, integerPathDefinition);
      assertAppliesToDefinition(removeUpdateDescription, integerPathDefinition);

      // If the path excluding the integer field is configured to be indexed, we should not filter
      // the event.
      var exactPathNoIntegerDefinition = createDefinition(type, "a", "bb", "ccc");
      assertAppliesToDefinition(updateUpdateDescription, exactPathNoIntegerDefinition);
      assertAppliesToDefinition(removeUpdateDescription, exactPathNoIntegerDefinition);

      // If just the parent of the integer field element is configured to be indexed, we should also
      // not filter out the event.
      var integerParentConfiguredDefinition = createDefinition(type, "a", "bb");
      assertAppliesToDefinition(updateUpdateDescription, integerParentConfiguredDefinition);
      assertAppliesToDefinition(removeUpdateDescription, integerParentConfiguredDefinition);

      // If the parent of the integer field path is not configured to be indexed, we can filter out
      // the event.
      var parentNotConfiguredDefinition = createDefinition(type, "a");
      assertDoesNotApplyToDefinition(updateUpdateDescription, parentNotConfiguredDefinition);
      assertDoesNotApplyToDefinition(removeUpdateDescription, parentNotConfiguredDefinition);
    }

    @Theory
    public void testMultipleIntegerFieldElementWithChildren(IndexDefinition.Type type) {
      var removeUpdateDescription = new UpdateDescription(List.of("a.bb.3.ccc.4.dddd"), null);
      var updateUpdateDescription =
          new UpdateDescription(null, new BsonDocument("a.bb.3.ccc.4.dddd", new BsonString("new")));

      // All paths, either including or excluding the integers, who have a.bb as a prefix should not
      // be filtered.
      var fullWithIntegers = createDefinition(type, "a", "bb", "3", "ccc", "4", "dddd");
      assertAppliesToDefinition(updateUpdateDescription, fullWithIntegers);
      assertAppliesToDefinition(removeUpdateDescription, fullWithIntegers);

      var fullNoIntegers = createDefinition(type, "a", "bb", "ccc", "dddd");
      assertAppliesToDefinition(updateUpdateDescription, fullNoIntegers);
      assertAppliesToDefinition(removeUpdateDescription, fullNoIntegers);

      var fullFirstInteger = createDefinition(type, "a", "bb", "3", "ccc", "dddd");
      assertAppliesToDefinition(updateUpdateDescription, fullFirstInteger);
      assertAppliesToDefinition(removeUpdateDescription, fullFirstInteger);

      var fullSecondInteger = createDefinition(type, "a", "bb", "ccc", "4", "dddd");
      assertAppliesToDefinition(updateUpdateDescription, fullSecondInteger);
      assertAppliesToDefinition(removeUpdateDescription, fullSecondInteger);

      var parentWithInteger = createDefinition(type, "a", "bb", "3", "ccc");
      assertAppliesToDefinition(updateUpdateDescription, parentWithInteger);
      assertAppliesToDefinition(removeUpdateDescription, parentWithInteger);

      var parentNoInteger = createDefinition(type, "a", "bb", "ccc");
      assertAppliesToDefinition(updateUpdateDescription, parentNoInteger);
      assertAppliesToDefinition(removeUpdateDescription, parentNoInteger);

      var firstBeforeInteger = createDefinition(type, "a", "bb");
      assertAppliesToDefinition(updateUpdateDescription, firstBeforeInteger);
      assertAppliesToDefinition(removeUpdateDescription, firstBeforeInteger);

      // If the parent of the integer field path is not configured to be indexed, we can filter out
      // the event.
      var parentNotConfiguredDefinition = createDefinition(type, "a");
      assertDoesNotApplyToDefinition(updateUpdateDescription, parentNotConfiguredDefinition);
      assertDoesNotApplyToDefinition(removeUpdateDescription, parentNotConfiguredDefinition);
    }

    @Test
    public void testStoredSourceIsIncludedWithInclusionModeAndUpdatedFields() {
      var description =
          new UpdateDescription(List.of(), new BsonDocument("a.b.c.d", new BsonString("new")));

      // case when a subdocument is updated
      assertAppliesToDefinition(
          description,
          createDefinitionWithStoredSource(StoredSourceDefinition.Mode.INCLUSION, List.of("a.b")));

      // case when the element at the exact path is updated
      assertAppliesToDefinition(
          description,
          createDefinitionWithStoredSource(
              StoredSourceDefinition.Mode.INCLUSION, List.of("a.b.c.d")));

      // case when an ancestor document or array is updated
      assertAppliesToDefinition(
          description,
          createDefinitionWithStoredSource(
              StoredSourceDefinition.Mode.INCLUSION, List.of("a.b.c.d.e")));
    }

    @Test
    public void testStoredSourceIsIncludedWithInclusionModeAndArrayFields() {
      var description =
          new UpdateDescription(List.of(), new BsonDocument("a.b.c.123.d", new BsonString("new")));

      // case when a subdocument is updated
      assertAppliesToDefinition(
          description,
          createDefinitionWithStoredSource(StoredSourceDefinition.Mode.INCLUSION, List.of("a.b")));

      // case when the element at the exact path is updated
      assertAppliesToDefinition(
          description,
          createDefinitionWithStoredSource(
              StoredSourceDefinition.Mode.INCLUSION, List.of("a.b.c.d")));

      // case when an ancestor document or array is updated
      assertAppliesToDefinition(
          description,
          createDefinitionWithStoredSource(
              StoredSourceDefinition.Mode.INCLUSION, List.of("a.b.c.d.e")));
    }

    @Test
    public void testStoredSourceIsIncludedWithInclusionModeAndRemovedFields() {
      var description = new UpdateDescription(List.of("a.b.c.d"), new BsonDocument());
      assertAppliesToDefinition(
          description,
          createDefinitionWithStoredSource(
              StoredSourceDefinition.Mode.INCLUSION, List.of("a.b.c.d")));
    }

    @Test
    public void testStoredSourceIsIncludedWithInclusionModeAndTruncatedArrays() {
      var description =
          new UpdateDescription(
              List.of(), new BsonDocument(), List.of(new TruncatedArray("a.b.c.d", 1)));
      assertAppliesToDefinition(
          description,
          createDefinitionWithStoredSource(
              StoredSourceDefinition.Mode.INCLUSION, List.of("a.b.c.d")));
    }

    @Test
    public void testStoredSourceIsNotIncludedWithInclusionMode() {
      var description =
          new UpdateDescription(List.of(), new BsonDocument("a.b.c.X", new BsonString("new")));
      assertDoesNotApplyToDefinition(
          description,
          createDefinitionWithStoredSource(
              StoredSourceDefinition.Mode.INCLUSION, List.of("a.b.c.Y")));
    }

    @Test
    public void testStoredSourceIsNotIncludedWithInclusionModeAndArrayFields() {
      var description =
          new UpdateDescription(List.of(), new BsonDocument("a.b.c.123.d", new BsonString("new")));
      assertDoesNotApplyToDefinition(
          description,
          createDefinitionWithStoredSource(
              StoredSourceDefinition.Mode.INCLUSION, List.of("a.b.Y")));
    }

    @Test
    public void testStoredSourceIsIncludedWithExclusionMode() {
      var description =
          new UpdateDescription(List.of(), new BsonDocument("a.b.c.d", new BsonString("new")));
      assertAppliesToDefinition(
          description,
          createDefinitionWithStoredSource(
              StoredSourceDefinition.Mode.EXCLUSION, List.of("a.b.c.d.e")));
    }

    @Test
    public void testStoredSourceIsIncludedWithExclusionModeAndArrayFields() {
      var description =
          new UpdateDescription(
              Collections.emptyList(), new BsonDocument("a.b.c.123.d", new BsonString("new")));
      assertAppliesToDefinition(
          description,
          createDefinitionWithStoredSource(
              StoredSourceDefinition.Mode.EXCLUSION, List.of("a.b.c.d")));
    }

    @Test
    public void testStoredSourceIsNotIncludedWithExclusionMode() {
      var description =
          new UpdateDescription(
              Collections.emptyList(), new BsonDocument("a.b.c.d", new BsonString("new")));

      assertDoesNotApplyToDefinition(
          description,
          createDefinitionWithStoredSource(
              StoredSourceDefinition.Mode.EXCLUSION, List.of("a.b.c.d")));

      assertDoesNotApplyToDefinition(
          description,
          createDefinitionWithStoredSource(
              StoredSourceDefinition.Mode.EXCLUSION, List.of("a.b.c")));
    }

    @Test
    public void testStoredSourceIsNotIncludedWithExclusionModeAndArrayFields() {
      var description =
          new UpdateDescription(
              Collections.emptyList(), new BsonDocument("a.b.c.123.d", new BsonString("new")));
      assertDoesNotApplyToDefinition(
          description,
          createDefinitionWithStoredSource(
              StoredSourceDefinition.Mode.EXCLUSION, List.of("a.b.c")));
    }

    @Test
    public void testUpdateDescriptionIsIgnoredForViewWithSyntheticFields() {
      UpdateDescription updateDescription =
          new UpdateDescription(List.of("a", "b"), new BsonDocument());
      IndexDefinition definition = createDefinition(IndexDefinition.Type.SEARCH, "c");
      ViewDefinition view =
          ViewDefinition.existing("test", List.of(new BsonDocument("$set", new BsonInt32(1))));
      assertAppliesToDefinition(updateDescription, definition, Optional.of(view));
    }

    private static void assertAppliesToDefinition(
        UpdateDescription updateDescription, IndexDefinition definition) {
      assertAppliesToDefinition(updateDescription, definition, Optional.empty());
    }

    private static void assertAppliesToDefinition(
        UpdateDescription updateDescription,
        IndexDefinition definition,
        Optional<ViewDefinition> view) {
      Assert.assertTrue(
          ChangeStreams.updateDescriptionAppliesToIndex(
              updateDescription,
              definition,
              definition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
              view));
    }

    private static void assertDoesNotApplyToDefinition(
        UpdateDescription updateDescription, IndexDefinition definition) {
      assertDoesNotApplyToDefinition(updateDescription, definition, Optional.empty());
    }

    private static void assertDoesNotApplyToDefinition(
        UpdateDescription updateDescription,
        IndexDefinition definition,
        Optional<ViewDefinition> view) {
      Assert.assertFalse(
          ChangeStreams.updateDescriptionAppliesToIndex(
              updateDescription,
              definition,
              definition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
              view));
    }

    /**
     * Creates an IndexDefinition with a single path configured as a string, and all of its parents
     * configured as a document with only its child.
     *
     * <p>For example, createDefinition("a", "b", "c") would create the following: <code>
     *   {
     *     ...
     *     mappings: {
     *       fields: {
     *         a: {
     *           type: "document",
     *           fields: {
     *             b: {
     *               type: "document",
     *               fields: {
     *                 c: { type: "string" }
     *               }
     *             }
     *           }
     *         }
     *       }
     *     }
     *   }
     * </code>
     */
    private static IndexDefinition createDefinition(IndexDefinition.Type type, String... elements) {

      if (type == IndexDefinition.Type.VECTOR_SEARCH) {
        return VectorIndexDefinitionBuilder.builder()
            .withFilterPath(String.join(".", elements))
            .build();
      }

      var elementsList = ImmutableList.copyOf(elements);
      var innerNodes = elementsList.subList(0, Math.max(0, elementsList.size() - 1));
      var leaf = elementsList.getLast();

      @Var
      var mappings =
          DocumentFieldDefinitionBuilder.builder()
              .field(
                  leaf,
                  FieldDefinitionBuilder.builder()
                      .string(StringFieldDefinitionBuilder.builder().build())
                      .build())
              .build();

      // Wrap the previously created child in another DocumentFieldDefinition, all the way to the
      // root.
      for (var field : innerNodes.reverse()) {
        mappings =
            DocumentFieldDefinitionBuilder.builder()
                .field(field, FieldDefinitionBuilder.builder().document(mappings).build())
                .build();
      }

      return SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
    }

    private static SearchIndexDefinition createDefinitionWithStoredSource(
        StoredSourceDefinition.Mode mode, List<String> dotSeparatedPaths) {
      return SearchIndexDefinitionBuilder.builder()
          .defaultMetadata()
          .mappings(
              DocumentFieldDefinitionBuilder.builder()
                  .field(
                      "irrelevant field",
                      FieldDefinitionBuilder.builder()
                          .string(StringFieldDefinitionBuilder.builder().build())
                          .build())
                  .build())
          .storedSource(StoredSourceDefinition.create(mode, dotSeparatedPaths))
          .build();
    }

    private static SearchIndexDefinition createSearchAutoEmbedDefinition(
        boolean dynamic, FieldPath sourceField) {
      SearchAutoEmbedFieldDefinition autoEmbed =
          new SearchAutoEmbedFieldDefinition("voyage-3-large", sourceField);
      return SearchIndexDefinitionBuilder.builder()
          .defaultMetadata()
          .mappings(
              DocumentFieldDefinitionBuilder.builder()
                  .dynamic(dynamic)
                  .field(
                      "embedding",
                      FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbed).build())
                  .build())
          .build();
    }

    private static SearchIndexDefinition createNestedSearchAutoEmbedDefinition(
        FieldPath sourceField) {
      SearchAutoEmbedFieldDefinition autoEmbed =
          new SearchAutoEmbedFieldDefinition("voyage-3-large", sourceField);
      var nestedMappings =
          DocumentFieldDefinitionBuilder.builder()
              .dynamic(false)
              .field(
                  "embedding", FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbed).build())
              .build();
      return SearchIndexDefinitionBuilder.builder()
          .defaultMetadata()
          .mappings(
              DocumentFieldDefinitionBuilder.builder()
                  .dynamic(false)
                  .field(
                      "nested", FieldDefinitionBuilder.builder().document(nestedMappings).build())
                  .build())
          .build();
    }

    @Test
    public void testCreateDefinition() {
      // The other tests heavily rely on this helper method's correctness, so test it as well.
      var singleDepth = createDefinition(IndexDefinition.Type.SEARCH, "a");
      var singleDepthExpected =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .indexId(singleDepth.getIndexId())
              .collectionUuid(singleDepth.getCollectionUuid())
              .mappings(
                  DocumentFieldDefinitionBuilder.builder()
                      .field(
                          "a",
                          FieldDefinitionBuilder.builder()
                              .string(StringFieldDefinitionBuilder.builder().build())
                              .build())
                      .build())
              .build();
      Assert.assertEquals(singleDepthExpected, singleDepth);

      var deeper = createDefinition(IndexDefinition.Type.SEARCH, "a", "bb", "ccc");
      var deeperExpected =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .indexId(deeper.getIndexId())
              .collectionUuid(deeper.getCollectionUuid())
              .mappings(
                  DocumentFieldDefinitionBuilder.builder()
                      .field(
                          "a",
                          FieldDefinitionBuilder.builder()
                              .document(
                                  DocumentFieldDefinitionBuilder.builder()
                                      .field(
                                          "bb",
                                          FieldDefinitionBuilder.builder()
                                              .document(
                                                  DocumentFieldDefinitionBuilder.builder()
                                                      .field(
                                                          "ccc",
                                                          FieldDefinitionBuilder.builder()
                                                              .string(
                                                                  StringFieldDefinitionBuilder
                                                                      .builder()
                                                                      .build())
                                                              .build())
                                                      .build())
                                              .build())
                                      .build())
                              .build())
                      .build())
              .build();
      Assert.assertEquals(deeperExpected, deeper);
    }
  }
}
