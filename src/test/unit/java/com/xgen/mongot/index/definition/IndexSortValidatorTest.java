package com.xgen.mongot.index.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.index.query.sort.EmbeddedSortScope;
import com.xgen.mongot.index.query.sort.MetaSortField;
import com.xgen.mongot.index.query.sort.MetaSortOptions;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.SortSelector;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.mongot.index.definition.AutocompleteFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import java.util.List;
import java.util.Optional;
import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;

public class IndexSortValidatorTest {

  @Test
  public void validateSort_withoutSort_doesNotTraverseMappings() throws BsonParseException {
    // No mappings are needed when sort is absent; accessing them would fail this test.
    IndexSortValidator.validateSort(Optional.empty(), null);
  }

  @Test
  public void validateNoScoreFields_withRegularFields_passes() throws BsonParseException {
    Sort sort =
        new Sort(
            ImmutableList.of(
                new MongotSortField(FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC),
                new MongotSortField(
                    FieldPath.newRoot("field2"), UserFieldSortOptions.DEFAULT_DESC)));

    // Should not throw
    IndexSortValidator.validateNoScoreFields(sort);
  }

  @Test
  public void validateNoScoreFields_withScoreField_throwsException() {
    Sort sort =
        new Sort(
            ImmutableList.of(
                new MongotSortField(FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC),
                new MongotSortField(
                    FieldPath.newRoot("scoreField"),
                    new MetaSortOptions(SortOrder.DESC, MetaSortField.SEARCH_SCORE))));

    try {
      IndexSortValidator.validateNoScoreFields(sort);
      fail("Expected BsonParseException");
    } catch (BsonParseException e) {
      assertEquals("Cannot sort on score for fields: [scoreField]", e.getMessage());
    }
  }

  private static UserFieldSortOptions scopedAsc(String path) {
    BsonDocument operator =
        new BsonDocument("exists", new BsonDocument("path", new BsonString(path + ".price")));
    return new UserFieldSortOptions(
        SortOrder.ASC,
        SortSelector.MIN,
        NullEmptySortPosition.LOWEST,
        Optional.of(new EmbeddedSortScope(FieldPath.parse(path), operator)));
  }

  @Test
  public void validateNoEmbeddedScope_withRegularFields_passes() throws BsonParseException {
    Sort sort =
        new Sort(
            ImmutableList.of(
                new MongotSortField(FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC),
                new MongotSortField(
                    FieldPath.newRoot("field2"), UserFieldSortOptions.DEFAULT_DESC)));

    // Should not throw
    IndexSortValidator.validateNoEmbeddedScope(sort);
  }

  @Test
  public void validateNoEmbeddedScope_withScopedField_throwsException() {
    Sort sort =
        new Sort(
            ImmutableList.of(
                new MongotSortField(FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC),
                new MongotSortField(FieldPath.parse("variants.price"), scopedAsc("variants"))));

    try {
      IndexSortValidator.validateNoEmbeddedScope(sort);
      fail("Expected BsonParseException");
    } catch (BsonParseException e) {
      assertEquals(
          "embeddedDocument is not supported on an index sort for fields: [variants.price]",
          e.getMessage());
    }
  }

  @Test
  public void validateSortFieldsStaticallyDefined_withDefinedFields_passes()
      throws BsonParseException {
    Trie<String, FieldDefinition> staticFields = new PatriciaTrie<>();
    staticFields.put("field1", createTokenFieldDefinition());
    staticFields.put("field2", createTokenFieldDefinition());

    Sort sort =
        new Sort(
            ImmutableList.of(
                new MongotSortField(FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC),
                new MongotSortField(
                    FieldPath.newRoot("field2"), UserFieldSortOptions.DEFAULT_DESC)));

    // Should not throw
    IndexSortValidator.validateSortFieldsStaticallyDefined(sort, staticFields);
  }

  @Test
  public void validateSortFieldsStaticallyDefined_withUndefinedField_throwsException() {
    Trie<String, FieldDefinition> staticFields = new PatriciaTrie<>();
    staticFields.put("field1", createTokenFieldDefinition());
    // field2 is not defined

    Sort sort =
        new Sort(
            ImmutableList.of(
                new MongotSortField(FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC),
                new MongotSortField(
                    FieldPath.newRoot("field2"), UserFieldSortOptions.DEFAULT_DESC)));

    try {
      IndexSortValidator.validateSortFieldsStaticallyDefined(sort, staticFields);
      fail("Expected BsonParseException");
    } catch (BsonParseException e) {
      assertEquals("Sort fields: [field2] are not statically defined", e.getMessage());
    }
  }

  @Test
  public void validateSortFieldsAreSortable_withSingleSortableType_passes()
      throws BsonParseException {
    Trie<String, FieldDefinition> staticFields = new PatriciaTrie<>();
    staticFields.put("field1", createTokenFieldDefinition());
    staticFields.put("field2", createNumberFieldDefinition());

    Sort sort =
        new Sort(
            ImmutableList.of(
                new MongotSortField(FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC),
                new MongotSortField(
                    FieldPath.newRoot("field2"), UserFieldSortOptions.DEFAULT_DESC)));

    // Should not throw
    IndexSortValidator.validateSortFieldsAreSortable(sort, staticFields);
  }

  @Test
  public void validateSortFieldsAreSortable_withMultipleSortableTypes_passes()
      throws BsonParseException {
    Trie<String, FieldDefinition> staticFields = new PatriciaTrie<>();
    staticFields.put("field1", createMultiTypeFieldDefinition());

    Sort sort =
        new Sort(
            ImmutableList.of(
                new MongotSortField(
                    FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC)));

    // Multi-type fields with all sortable types should pass
    IndexSortValidator.validateSortFieldsAreSortable(sort, staticFields);
  }

  @Test
  public void validateSortFieldsAreSortable_withNonSortableField_throwsException() {
    Trie<String, FieldDefinition> staticFields = new PatriciaTrie<>();
    staticFields.put("field1", createStringFieldDefinition()); // String is not sortable

    Sort sort =
        new Sort(
            ImmutableList.of(
                new MongotSortField(
                    FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC)));

    try {
      IndexSortValidator.validateSortFieldsAreSortable(sort, staticFields);
      fail("Expected BsonParseException");
    } catch (BsonParseException e) {
      assertEquals(
          "Sort fields are not sortable: {field1=[STRING]}. Sortable types are: "
              + FieldDefinition.INDEXING_SORTABLE_TYPES,
          e.getMessage());
    }
  }

  @Test
  public void validateSortFieldsAreSortable_withStringAndToken_stringIshDedup_passes()
      throws BsonParseException {
    Trie<String, FieldDefinition> staticFields = new PatriciaTrie<>();
    staticFields.put("field1", createTokenAndStringFieldDefinition());

    Sort sort =
        new Sort(
            ImmutableList.of(
                new MongotSortField(
                    FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC)));

    // TOKEN + STRING should pass: STRING is filtered out when TOKEN is present
    IndexSortValidator.validateSortFieldsAreSortable(sort, staticFields);
  }

  @Test
  public void validateSortFieldsAreSortable_withAutocompleteAndToken_stringIshDedup_passes()
      throws BsonParseException {
    Trie<String, FieldDefinition> staticFields = new PatriciaTrie<>();
    staticFields.put("field1", createTokenAndAutocompleteFieldDefinition());

    Sort sort =
        new Sort(
            ImmutableList.of(
                new MongotSortField(
                    FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC)));

    // TOKEN + AUTOCOMPLETE should pass: AUTOCOMPLETE is filtered out when TOKEN is present
    IndexSortValidator.validateSortFieldsAreSortable(sort, staticFields);
  }

  @Test
  public void filterStringishTypes_removesStringAndAutocomplete_whenTokenPresent() {
    List<FieldTypeDefinition.Type> types =
        List.of(
            FieldTypeDefinition.Type.TOKEN,
            FieldTypeDefinition.Type.STRING,
            FieldTypeDefinition.Type.AUTOCOMPLETE);

    List<FieldTypeDefinition.Type> result = IndexSortValidator.filterStringishTypes(types);

    assertEquals(List.of(FieldTypeDefinition.Type.TOKEN), result);
  }

  @Test
  public void filterStringishTypes_keepsAllTypes_whenTokenAbsent() {
    List<FieldTypeDefinition.Type> types =
        List.of(FieldTypeDefinition.Type.STRING, FieldTypeDefinition.Type.DATE);

    List<FieldTypeDefinition.Type> result = IndexSortValidator.filterStringishTypes(types);

    assertEquals(types, result);
  }

  @Test
  public void validateSortFieldsAreSortable_withAutoEmbedField_throwsException() {
    Trie<String, FieldDefinition> staticFields = new PatriciaTrie<>();
    staticFields.put("field1", createAutoEmbedFieldDefinition());

    Sort sort =
        new Sort(
            ImmutableList.of(
                new MongotSortField(
                    FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC)));

    try {
      IndexSortValidator.validateSortFieldsAreSortable(sort, staticFields);
      fail("Expected BsonParseException");
    } catch (BsonParseException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("{field1=[AUTO_EMBED]}"));
    }
  }

  @Test
  public void validateSortFieldsAreSortable_withStringAndAutoEmbedField_throwsException() {
    Trie<String, FieldDefinition> staticFields = new PatriciaTrie<>();
    staticFields.put("field1", createStringAndAutoEmbedFieldDefinition());

    Sort sort =
        new Sort(
            ImmutableList.of(
                new MongotSortField(
                    FieldPath.newRoot("field1"), UserFieldSortOptions.DEFAULT_ASC)));

    try {
      IndexSortValidator.validateSortFieldsAreSortable(sort, staticFields);
      fail("Expected BsonParseException");
    } catch (BsonParseException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("AUTO_EMBED"));
      assertTrue(e.getMessage(), e.getMessage().contains("STRING"));
    }
  }

  // Helper methods to create field definitions
  private FieldDefinition createTokenFieldDefinition() {
    return FieldDefinitionBuilder.builder()
        .token(TokenFieldDefinitionBuilder.builder().build())
        .build();
  }

  private FieldDefinition createNumberFieldDefinition() {
    return FieldDefinitionBuilder.builder()
        .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
        .build();
  }

  private FieldDefinition createStringFieldDefinition() {
    return FieldDefinitionBuilder.builder()
        .string(StringFieldDefinitionBuilder.builder().build())
        .build();
  }

  private FieldDefinition createMultiTypeFieldDefinition() {
    return FieldDefinitionBuilder.builder()
        .token(TokenFieldDefinitionBuilder.builder().build())
        .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
        .build();
  }

  private FieldDefinition createTokenAndStringFieldDefinition() {
    return FieldDefinitionBuilder.builder()
        .token(TokenFieldDefinitionBuilder.builder().build())
        .string(StringFieldDefinitionBuilder.builder().build())
        .build();
  }

  private FieldDefinition createTokenAndAutocompleteFieldDefinition() {
    return FieldDefinitionBuilder.builder()
        .token(TokenFieldDefinitionBuilder.builder().build())
        .autocomplete(AutocompleteFieldDefinitionBuilder.builder().build())
        .build();
  }

  private FieldDefinition createAutoEmbedFieldDefinition() {
    return FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedDefinition()).build();
  }

  private FieldDefinition createStringAndAutoEmbedFieldDefinition() {
    return FieldDefinitionBuilder.builder()
        .string(StringFieldDefinitionBuilder.builder().build())
        .searchAutoEmbed(autoEmbedDefinition())
        .build();
  }

  private static SearchAutoEmbedFieldDefinition autoEmbedDefinition() {
    return new SearchAutoEmbedFieldDefinition(
        new VectorAutoEmbedFieldSpecification(
            1024,
            VectorSimilarity.DOT_PRODUCT,
            VectorAutoEmbedQuantization.FLOAT,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm(),
            "voyage-3-large",
            "text"));
  }
}
