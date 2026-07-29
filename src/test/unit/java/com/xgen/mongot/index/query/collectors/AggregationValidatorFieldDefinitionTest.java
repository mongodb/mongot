package com.xgen.mongot.index.query.collectors;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.query.collectors.AggregationValidator.AggregationValidationException;
import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.testing.mongot.index.definition.DateFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonInt32;
import org.junit.Test;

/** Tests for {@link AggregationValidator#validateAggregationQuery}. */
public class AggregationValidatorFieldDefinitionTest {

  private static final String PRICE_FIELD = "price";

  /**
   * Legacy {@code numberFacet} stays valid for ordinary facet queries, but search aggregation reads
   * v2 fields only and must reject it as a query error rather than failing deeper in boundary spec
   * building.
   */
  @Test
  public void legacyNumberFacetGroupingField_throws() {
    SearchFieldDefinitionResolver resolver = resolverWithLegacyNumberFacetField();

    assertThrows(
        AggregationValidationException.class,
        () ->
            AggregationValidator.validateAggregationQuery(
                collectorGroupingOnPrice(), resolver, Optional.empty(), true));
  }

  /** A legacy grouping field nested under a valid one is rejected just the same. */
  @Test
  public void legacyNumberFacetNestedGroupingField_throws() {
    SearchFieldDefinitionResolver resolver = resolverWithLegacyNumberFacetField();
    AggregationFacetDefinition nested =
        AggregationFacetDefinition.create(
            new FacetDefinition.StringFacetDefinition("category", 10),
            Map.of(),
            Map.of("byPrice", priceRangeFacet()));

    assertThrows(
        AggregationValidationException.class,
        () ->
            AggregationValidator.validateAggregationQuery(
                AggregationCollector.create(
                    AllDocumentsOperator.INSTANCE, Map.of("byCategory", nested)),
                resolver,
                Optional.empty(),
                true));
  }

  @Test
  public void v2NumberGroupingField_passes() throws AggregationValidationException {
    AggregationValidator.validateAggregationQuery(
        collectorGroupingOnPrice(), resolverWithV2NumberField(), Optional.empty(), true);
  }

  /**
   * A field indexed as neither type is left to facet field resolution, which reports it as not
   * indexed with a message covering both the v2 and legacy types.
   */
  @Test
  public void unindexedGroupingField_passes() throws AggregationValidationException {
    AggregationValidator.validateAggregationQuery(
        collectorGroupingOnPrice(), resolverWithDateField("createdAt"), Optional.empty(), true);
  }

  /**
   * The feature flag is checked before the grouping fields, so a query that is invalid on both
   * counts reports the disabled feature.
   */
  @Test
  public void aggregationDisabled_throws() {
    SearchFieldDefinitionResolver resolver = resolverWithLegacyNumberFacetField();

    AggregationValidationException exception =
        assertThrows(
            AggregationValidationException.class,
            () ->
                AggregationValidator.validateAggregationQuery(
                    collectorGroupingOnPrice(), resolver, Optional.empty(), false));
    assertTrue(exception.getMessage().contains("Aggregation is not enabled"));
  }

  private static AggregationCollector collectorGroupingOnPrice() {
    return AggregationCollector.create(
        AllDocumentsOperator.INSTANCE, Map.of("byPrice", priceRangeFacet()));
  }

  private static AggregationFacetDefinition priceRangeFacet() {
    return AggregationFacetDefinition.create(
        new FacetDefinition.NumericFacetDefinition(
            PRICE_FIELD, Optional.empty(), List.of(new BsonInt32(0), new BsonInt32(10))),
        Map.of(),
        Map.of());
  }

  private static SearchFieldDefinitionResolver resolverWithLegacyNumberFacetField() {
    return resolverFor(
        FieldDefinitionBuilder.builder()
            .numberFacet(
                NumericFieldDefinitionBuilder.builder()
                    .representation(NumericFieldOptions.Representation.INT64)
                    .buildNumberFacetField())
            .build(),
        PRICE_FIELD);
  }

  private static SearchFieldDefinitionResolver resolverWithV2NumberField() {
    return resolverFor(
        FieldDefinitionBuilder.builder()
            .number(
                NumericFieldDefinitionBuilder.builder()
                    .representation(NumericFieldOptions.Representation.INT64)
                    .buildNumberField())
            .build(),
        PRICE_FIELD);
  }

  private static SearchFieldDefinitionResolver resolverWithDateField(String fieldName) {
    return resolverFor(
        FieldDefinitionBuilder.builder().date(DateFieldDefinitionBuilder.builder().build()).build(),
        fieldName);
  }

  private static SearchFieldDefinitionResolver resolverFor(
      FieldDefinition fieldDefinition, String fieldName) {
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
            .mappings(
                DocumentFieldDefinitionBuilder.builder().field(fieldName, fieldDefinition).build())
            .build();
    return indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT);
  }
}
