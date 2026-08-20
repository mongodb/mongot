package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.aggregation.MaxAggregationSpec;
import com.xgen.mongot.index.query.aggregation.MinAggregationSpec;
import com.xgen.mongot.index.query.aggregation.ResolvedNumericMetricField;
import com.xgen.mongot.index.query.aggregation.ResolvedTokenField;
import com.xgen.mongot.index.query.aggregation.TopAggregationSpec;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DateFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
  public void resolveNumericMetricField_dateField_throws() {
    // A date field has a real field definition and a dateV2 doc-values column, so it is the most
    // plausible thing a user reaches for after numbers -- and the one this feature deliberately
    // does not support yet. Without this test only token/unmapped rejection is pinned.
    LuceneAggregationContextFieldResolver fieldResolver =
        new LuceneAggregationContextFieldResolver(
            createFacetContext(dateMappings(PRICE_FIELD), Optional.empty()));

    assertThrows(
        InvalidQueryException.class,
        () -> fieldResolver.resolveNumericMetricField(PRICE_PATH, Optional.empty()));
  }

  @Test
  public void withResolvedFields_minOnDateField_throws() {
    LuceneAggregationContextFieldResolver fieldResolver =
        new LuceneAggregationContextFieldResolver(
            createFacetContext(dateMappings(PRICE_FIELD), Optional.empty()));

    assertThrows(
        InvalidQueryException.class,
        () ->
            fieldResolver.withResolvedFields(
                new TopAggregationSpec(List.of(new MinAggregationSpec("min_price", PRICE_PATH))),
                true));
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
  public void resolveTokenMetricField_returnStoredSourceOnCoveredPath_readsStoredSource()
      throws InvalidQueryException {
    LuceneAggregationContextFieldResolver fieldResolver =
        new LuceneAggregationContextFieldResolver(
            createFacetContext(
                tokenMappings(CITY_FIELD), Optional.of(StoredSourceDefinition.createIncludeAll())));

    assertEquals(
        new ResolvedTokenField.FromStoredSource(CITY_PATH),
        fieldResolver.resolveTokenMetricField(CITY_PATH, Optional.empty(), true));
  }

  /** Stored source can serve a path no doc-values column exists for, so it is preferred for one. */
  @Test
  public void resolveTokenMetricField_returnStoredSourceOnUnindexedCoveredPath_readsStoredSource()
      throws InvalidQueryException {
    LuceneAggregationContextFieldResolver fieldResolver =
        new LuceneAggregationContextFieldResolver(
            createFacetContext(
                tokenMappings(CITY_FIELD), Optional.of(StoredSourceDefinition.createIncludeAll())));

    FieldPath unindexed = FieldPath.parse("meta.tags");

    assertEquals(
        new ResolvedTokenField.FromStoredSource(unindexed),
        fieldResolver.resolveTokenMetricField(unindexed, Optional.empty(), true));
  }

  /** Without returnStoredSource the value comes from doc values even on a covered path. */
  @Test
  public void resolveTokenMetricField_withoutReturnStoredSource_readsDocValues()
      throws InvalidQueryException {
    LuceneAggregationContextFieldResolver fieldResolver =
        new LuceneAggregationContextFieldResolver(
            createFacetContext(
                tokenMappings(CITY_FIELD), Optional.of(StoredSourceDefinition.createIncludeAll())));

    assertEquals(
        new ResolvedTokenField.FromDocValues(
            CITY_PATH, Set.of(FieldName.TypeField.TOKEN, FieldName.TypeField.NULL)),
        fieldResolver.resolveTokenMetricField(CITY_PATH, Optional.empty(), false));
  }

  /**
   * returnStoredSource asks for stored source but cannot conjure it: an uncovered path that doc
   * values can serve falls back rather than being rejected.
   */
  @Test
  public void resolveTokenMetricField_returnStoredSourceOnUncoveredPath_fallsBackToDocValues()
      throws InvalidQueryException {
    LuceneAggregationContextFieldResolver fieldResolver =
        new LuceneAggregationContextFieldResolver(
            createFacetContext(tokenMappings(CITY_FIELD), Optional.empty()));

    assertEquals(
        new ResolvedTokenField.FromDocValues(
            CITY_PATH, Set.of(FieldName.TypeField.TOKEN, FieldName.TypeField.NULL)),
        fieldResolver.resolveTokenMetricField(CITY_PATH, Optional.empty(), true));
  }

  @Test
  public void resolveTokenMetricField_dateField_readsTheDateColumn() throws InvalidQueryException {
    LuceneAggregationContextFieldResolver fieldResolver =
        new LuceneAggregationContextFieldResolver(
            createFacetContext(dateMappings(CITY_FIELD), Optional.empty()));

    assertEquals(
        new ResolvedTokenField.FromDocValues(
            CITY_PATH, Set.of(FieldName.TypeField.DATE_V2, FieldName.TypeField.NULL)),
        fieldResolver.resolveTokenMetricField(CITY_PATH, Optional.empty(), false));
  }

  /** A path indexed as several types is read back from every one of their columns. */
  @Test
  public void resolveTokenMetricField_multiTypeField_readsEveryTypeColumn()
      throws InvalidQueryException {
    LuceneAggregationContextFieldResolver fieldResolver =
        new LuceneAggregationContextFieldResolver(
            createFacetContext(tokenAndDateMappings(CITY_FIELD), Optional.empty()));

    assertEquals(
        new ResolvedTokenField.FromDocValues(
            CITY_PATH,
            Set.of(
                FieldName.TypeField.TOKEN, FieldName.TypeField.DATE_V2, FieldName.TypeField.NULL)),
        fieldResolver.resolveTokenMetricField(CITY_PATH, Optional.empty(), false));
  }

  /**
   * A path that merely leads to a stored descendant is not itself stored: with only "a.b.c"
   * included, reading "a.b" would return a partial container document. It is not indexed either, so
   * neither source can serve it.
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
        () ->
            fieldResolver.resolveTokenMetricField(FieldPath.parse("a.b"), Optional.empty(), true));

    // The included path itself (and anything under it) still resolves.
    assertEquals(
        new ResolvedTokenField.FromStoredSource(FieldPath.parse("a.b.c")),
        fieldResolver.resolveTokenMetricField(FieldPath.parse("a.b.c"), Optional.empty(), true));
  }

  /** Neither source can serve an unmapped path on an index with no stored source. */
  @Test
  public void resolveTokenMetricField_pathNeitherStoredNorIndexed_throws() {
    LuceneAggregationContextFieldResolver fieldResolver =
        new LuceneAggregationContextFieldResolver(
            createFacetContext(tokenMappings(CITY_FIELD), Optional.empty()));

    FieldPath unindexed = FieldPath.parse("meta.tags");

    assertThrows(
        InvalidQueryException.class,
        () -> fieldResolver.resolveTokenMetricField(unindexed, Optional.empty(), false));
  }

  /**
   * A stored path with no readable column names returnStoredSource as the remedy, since setting it
   * is all the user has to do.
   */
  @Test
  public void resolveTokenMetricField_storedButUnreadablePath_namesReturnStoredSource() {
    LuceneAggregationContextFieldResolver fieldResolver =
        new LuceneAggregationContextFieldResolver(
            createFacetContext(
                tokenMappings(CITY_FIELD), Optional.of(StoredSourceDefinition.createIncludeAll())));

    FieldPath unindexed = FieldPath.parse("meta.tags");

    InvalidQueryException thrown =
        assertThrows(
            InvalidQueryException.class,
            () -> fieldResolver.resolveTokenMetricField(unindexed, Optional.empty(), false));

    assertThat(thrown).hasMessageThat().contains("returnStoredSource");
  }

  @Test
  public void resolveTokenMetricField_embeddedPath_throws() {
    LuceneAggregationContextFieldResolver fieldResolver =
        createFieldResolverWithTokenField(CITY_FIELD);

    assertThrows(
        InvalidQueryException.class,
        () ->
            fieldResolver.resolveTokenMetricField(
                CITY_PATH, Optional.of(FieldPath.parse("reviews")), true));
  }

  @Test
  public void withResolvedFields_minMaxOnNumberField_resolvesNumericMetric()
      throws InvalidQueryException {
    LuceneAggregationContextFieldResolver fieldResolver =
        createFieldResolverWithNumberField(NumericFieldOptions.Representation.INT64);

    LuceneAggregationContextFieldResolver resolved =
        fieldResolver.withResolvedFields(
            new TopAggregationSpec(
                List.of(
                    new MinAggregationSpec("min_price", PRICE_PATH),
                    new MaxAggregationSpec("max_price", PRICE_PATH))),
            true);

    assertEquals(
        FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(PRICE_PATH, Optional.empty()),
        resolved.resolveNumeric(PRICE_PATH).luceneFieldName());
  }

  @Test
  public void withResolvedFields_minOnNonNumberField_throws() {
    // min/max read NUMBER_*_V2 doc values, so a non-number path must be rejected before execution
    // -- this is the only validation gate for the operator.
    LuceneAggregationContextFieldResolver fieldResolver =
        createFieldResolverWithTokenField(CITY_FIELD);

    assertThrows(
        InvalidQueryException.class,
        () ->
            fieldResolver.withResolvedFields(
                new TopAggregationSpec(List.of(new MinAggregationSpec("min_city", CITY_PATH))),
                true));
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

  private static DocumentFieldDefinition dateMappings(String fieldName) {
    return DocumentFieldDefinitionBuilder.builder()
        .field(
            fieldName,
            FieldDefinitionBuilder.builder()
                .date(DateFieldDefinitionBuilder.builder().build())
                .build())
        .build();
  }

  private static DocumentFieldDefinition tokenAndDateMappings(String fieldName) {
    return DocumentFieldDefinitionBuilder.builder()
        .field(
            fieldName,
            FieldDefinitionBuilder.builder()
                .token(TokenFieldDefinitionBuilder.builder().build())
                .date(DateFieldDefinitionBuilder.builder().build())
                .build())
        .build();
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
