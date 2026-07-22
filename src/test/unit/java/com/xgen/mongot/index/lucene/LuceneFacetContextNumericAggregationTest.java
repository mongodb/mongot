package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import java.util.Optional;
import org.junit.Test;

public class LuceneFacetContextNumericAggregationTest {

  private static final String PRICE_FIELD = "price";
  private static final FieldPath PRICE_PATH = FieldPath.parse(PRICE_FIELD);

  @Test
  public void getNumericAggregationLuceneFieldName_int64NumberField_returnsDocValuesField()
      throws InvalidQueryException {
    LuceneFacetContext facetContext =
        createFacetContextWithNumberField(NumericFieldOptions.Representation.INT64);

    String luceneFieldName =
        facetContext.getNumericAggregationLuceneFieldName(PRICE_PATH, Optional.empty());

    assertThat(luceneFieldName)
        .isEqualTo(
            FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(PRICE_PATH, Optional.empty()));
  }

  @Test
  public void getNumericAggregationLuceneFieldName_doubleNumberField_returnsDocValuesField()
      throws InvalidQueryException {
    LuceneFacetContext facetContext =
        createFacetContextWithNumberField(NumericFieldOptions.Representation.DOUBLE);

    String luceneFieldName =
        facetContext.getNumericAggregationLuceneFieldName(PRICE_PATH, Optional.empty());

    assertThat(luceneFieldName)
        .isEqualTo(
            FieldName.TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(PRICE_PATH, Optional.empty()));
  }

  @Test
  public void getNumericAggregationLuceneFieldName_nonNumericField_throws() {
    LuceneFacetContext facetContext = createFacetContextWithTokenField(PRICE_FIELD);

    assertThrows(
        InvalidQueryException.class,
        () -> facetContext.getNumericAggregationLuceneFieldName(PRICE_PATH, Optional.empty()));
  }

  @Test
  public void getNumericAggregationLuceneFieldName_embeddedPath_throws() {
    LuceneFacetContext facetContext =
        createFacetContextWithNumberField(NumericFieldOptions.Representation.INT64);

    assertThrows(
        InvalidQueryException.class,
        () ->
            facetContext.getNumericAggregationLuceneFieldName(
                PRICE_PATH, Optional.of(FieldPath.parse("reviews"))));
  }

  private static LuceneFacetContext createFacetContextWithNumberField(
      NumericFieldOptions.Representation representation) {
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        PRICE_FIELD,
                        FieldDefinitionBuilder.builder()
                            .number(
                                NumericFieldDefinitionBuilder.builder()
                                    .representation(representation)
                                    .buildNumberField())
                            .build())
                    .build())
            .build();

    return new LuceneFacetContext(
        indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
        indexDefinition.getIndexCapabilities(IndexFormatVersion.CURRENT));
  }

  private static LuceneFacetContext createFacetContextWithTokenField(String fieldName) {
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        fieldName,
                        FieldDefinitionBuilder.builder()
                            .token(TokenFieldDefinitionBuilder.builder().build())
                            .build())
                    .build())
            .build();

    return new LuceneFacetContext(
        indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
        indexDefinition.getIndexCapabilities(IndexFormatVersion.CURRENT));
  }
}
