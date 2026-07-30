package com.xgen.mongot.index.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.aggregation.ResolvedNumericMetricField;
import com.xgen.mongot.index.query.aggregation.ResolvedTokenField;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import java.util.List;
import java.util.Optional;
import org.bson.BsonInt32;
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

  /**
   * Private preview reads token-valued metrics from stored source only, so a covered path resolves
   * to its stored-source path regardless of how it is indexed. The scalar doc-values fast path is
   * deferred to a follow-up.
   */
  @Test
  public void resolveTokenMetricField_pathCoveredByStoredSource_readsStoredSource()
      throws InvalidQueryException {
    LuceneAggregationContextFieldResolver fieldResolver =
        new LuceneAggregationContextFieldResolver(
            createFacetContext(
                tokenMappings(CITY_FIELD), Optional.of(StoredSourceDefinition.createIncludeAll())));

    assertEquals(
        new ResolvedTokenField(CITY_PATH),
        fieldResolver.resolveTokenMetricField(CITY_PATH, Optional.empty()));
  }

  @Test
  public void resolveTokenMetricField_unindexedPathCoveredByStoredSource_readsStoredSource()
      throws InvalidQueryException {
    LuceneAggregationContextFieldResolver fieldResolver =
        new LuceneAggregationContextFieldResolver(
            createFacetContext(
                tokenMappings(CITY_FIELD), Optional.of(StoredSourceDefinition.createIncludeAll())));

    FieldPath unindexed = FieldPath.parse("meta.tags");

    assertEquals(
        new ResolvedTokenField(unindexed),
        fieldResolver.resolveTokenMetricField(unindexed, Optional.empty()));
  }

  /**
   * A path that merely leads to a stored descendant is not itself stored: with only "a.b.c"
   * included, reading "a.b" would return a partial container document, so it must be rejected.
   */
  @Test
  public void resolveTokenMetricField_ancestorOfStoredPath_throws() throws InvalidQueryException {
    LuceneAggregationContextFieldResolver fieldResolver =
        new LuceneAggregationContextFieldResolver(
            createFacetContext(
                tokenMappings(CITY_FIELD),
                Optional.of(
                    StoredSourceDefinition.create(
                        StoredSourceDefinition.Mode.INCLUSION, List.of("a.b.c")))));

    assertThrows(
        InvalidQueryException.class,
        () -> fieldResolver.resolveTokenMetricField(FieldPath.parse("a.b"), Optional.empty()));

    // The included path itself (and anything under it) still resolves.
    assertEquals(
        new ResolvedTokenField(FieldPath.parse("a.b.c")),
        fieldResolver.resolveTokenMetricField(FieldPath.parse("a.b.c"), Optional.empty()));
  }

  @Test
  public void resolveTokenMetricField_pathNotCoveredByStoredSource_throws() {
    LuceneAggregationContextFieldResolver fieldResolver =
        new LuceneAggregationContextFieldResolver(
            createFacetContext(tokenMappings(CITY_FIELD), Optional.empty()));

    assertThrows(
        InvalidQueryException.class,
        () -> fieldResolver.resolveTokenMetricField(CITY_PATH, Optional.empty()));
  }

  @Test
  public void resolveTokenMetricField_embeddedPath_throws() {
    LuceneAggregationContextFieldResolver fieldResolver =
        createFieldResolverWithTokenField(CITY_FIELD);

    assertThrows(
        InvalidQueryException.class,
        () ->
            fieldResolver.resolveTokenMetricField(
                CITY_PATH, Optional.of(FieldPath.parse("reviews"))));
  }

  @Test
  public void getBoundaryFacetFieldType_v2NumberField_resolvesV2Type()
      throws InvalidQueryException {
    LuceneAggregationContextFieldResolver fieldResolver =
        createFieldResolverWithNumberField(NumericFieldOptions.Representation.INT64);

    assertEquals(
        FieldName.TypeField.NUMBER_INT64_V2,
        fieldResolver.getBoundaryFacetFieldType(
            new FacetDefinition.NumericFacetDefinition(
                PRICE_FIELD, Optional.empty(), List.of(new BsonInt32(0), new BsonInt32(10))),
            Optional.empty()));
  }

  private static DocumentFieldDefinition tokenMappings(String fieldName) {
    return DocumentFieldDefinitionBuilder.builder()
        .field(
            fieldName,
            FieldDefinitionBuilder.builder()
                .token(TokenFieldDefinitionBuilder.builder().build())
                .build())
        .build();
  }

  private static LuceneFacetContext createFacetContext(
      DocumentFieldDefinition mappings, Optional<StoredSourceDefinition> storedSource) {
    SearchIndexDefinitionBuilder builder =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
            .mappings(mappings);
    storedSource.ifPresent(builder::storedSource);
    SearchIndexDefinition indexDefinition = builder.build();

    return new LuceneFacetContext(
        indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
        indexDefinition.getIndexCapabilities(IndexFormatVersion.CURRENT));
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
