package com.xgen.mongot.index.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.query.sort.MetaSortField;
import com.xgen.mongot.index.query.sort.MetaSortOptions;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.testing.mongot.index.definition.AutocompleteFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import java.util.List;
import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.junit.Test;

public class IndexSortValidatorTest {

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
}
