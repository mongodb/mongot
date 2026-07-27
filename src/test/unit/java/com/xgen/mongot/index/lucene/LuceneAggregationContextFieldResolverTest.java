package com.xgen.mongot.index.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.aggregation.ResolvedNumericMetricField;
import com.xgen.mongot.index.query.aggregation.ResolvedTokenField;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import java.util.Optional;
import org.junit.Test;

public class LuceneAggregationContextFieldResolverTest {

  private static final String CITY_FIELD = "city";
  private static final String PRICE_FIELD = "price";
  private static final FieldPath CITY_PATH = FieldPath.parse(CITY_FIELD);
  private static final FieldPath PRICE_PATH = FieldPath.parse(PRICE_FIELD);

  @Test
  public void resolveNumericMetricField_int64NumberField_returnsDocValuesField()
      throws InvalidQueryException {
    LuceneAggregationContextFieldResolver fieldResolver =
        createFieldResolverWithNumberField(NumericFieldOptions.Representation.INT64);

    ResolvedNumericMetricField resolved =
        fieldResolver.resolveNumericMetricField(PRICE_PATH, Optional.empty());

    assertEquals(
        FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(PRICE_PATH, Optional.empty()),
        resolved.luceneFieldName());
    assertEquals(NumericFieldOptions.Representation.INT64, resolved.representation());
  }

  @Test
  public void resolveNumericMetricField_doubleNumberField_returnsDocValuesField()
      throws InvalidQueryException {
    LuceneAggregationContextFieldResolver fieldResolver =
        createFieldResolverWithNumberField(NumericFieldOptions.Representation.DOUBLE);

    ResolvedNumericMetricField resolved =
        fieldResolver.resolveNumericMetricField(PRICE_PATH, Optional.empty());

    assertEquals(
        FieldName.TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(PRICE_PATH, Optional.empty()),
        resolved.luceneFieldName());
    assertEquals(NumericFieldOptions.Representation.DOUBLE, resolved.representation());
  }

  @Test
  public void resolveNumericMetricField_nonNumericField_throws() {
    LuceneAggregationContextFieldResolver fieldResolver =
        createFieldResolverWithTokenField(CITY_FIELD);

    assertThrows(
        InvalidQueryException.class,
        () -> fieldResolver.resolveNumericMetricField(PRICE_PATH, Optional.empty()));
  }

  @Test
  public void resolveNumericMetricField_embeddedPath_throws() {
    LuceneAggregationContextFieldResolver fieldResolver =
        createFieldResolverWithNumberField(NumericFieldOptions.Representation.INT64);

    assertThrows(
        InvalidQueryException.class,
        () ->
            fieldResolver.resolveNumericMetricField(
                PRICE_PATH, Optional.of(FieldPath.parse("reviews"))));
  }

  @Test
  public void resolveTokenMetricField_tokenField_returnsDocValuesField()
      throws InvalidQueryException {
    LuceneAggregationContextFieldResolver fieldResolver =
        createFieldResolverWithTokenField(CITY_FIELD);

    ResolvedTokenField resolved =
        fieldResolver.resolveTokenMetricField(CITY_PATH, Optional.empty());

    assertEquals(
        FieldName.TypeField.TOKEN.getLuceneFieldName(CITY_PATH, Optional.empty()),
        resolved.luceneFieldName());
  }

  @Test
  public void resolveTokenMetricField_nonTokenField_throws() {
    LuceneAggregationContextFieldResolver fieldResolver =
        createFieldResolverWithNumberField(NumericFieldOptions.Representation.INT64);

    assertThrows(
        InvalidQueryException.class,
        () -> fieldResolver.resolveTokenMetricField(CITY_PATH, Optional.empty()));
  }

  private static LuceneAggregationContextFieldResolver createFieldResolverWithNumberField(
      NumericFieldOptions.Representation representation) {
    return new LuceneAggregationContextFieldResolver(
        createFacetContextWithNumberField(representation));
  }

  private static LuceneAggregationContextFieldResolver createFieldResolverWithTokenField(
      String fieldName) {
    return new LuceneAggregationContextFieldResolver(createFacetContextWithTokenField(fieldName));
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
