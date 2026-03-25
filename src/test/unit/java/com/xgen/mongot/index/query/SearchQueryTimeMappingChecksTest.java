package com.xgen.mongot.index.query;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.GeoFieldDefinition;
import com.xgen.mongot.index.definition.KnnVectorFieldDefinition;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.path.string.StringFieldPath;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.BooleanFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DateFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.GeoFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.ObjectIdFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

@RunWith(Theories.class)
public class SearchQueryTimeMappingChecksTest {

  @DataPoints
  public static IndexFormatVersion[] indexFormatVersions =
      new IndexFormatVersion[] {
        IndexFormatVersion.MIN_SUPPORTED_VERSION, IndexFormatVersion.CURRENT
      };

  @Theory
  public void rootMappingIsDynamicIsNotIndexedAsGeoPoint(IndexFormatVersion indexFormatVersion) {
    var validations =
        new SearchQueryTimeMappingChecks(
            SearchIndexDefinitionBuilder.builder()
                .defaultMetadata()
                .dynamicMapping()
                .build()
                .createFieldDefinitionResolver(indexFormatVersion));
    Assert.assertFalse(validations.indexedAsGeoPoint(path("any_path"), Optional.empty()));
    Assert.assertFalse(validations.indexedAsGeoPoint(path("any.nested_path"), Optional.empty()));
    Assert.assertFalse(
        validations.indexedAsGeoPoint(path("any.very.nested_path"), Optional.empty()));
  }

  @Theory
  public void rootMappingIsDynamicIsNotIndexedAsGeoShapes(IndexFormatVersion indexFormatVersion) {
    var validations =
        new SearchQueryTimeMappingChecks(
            SearchIndexDefinitionBuilder.builder()
                .defaultMetadata()
                .dynamicMapping()
                .build()
                .createFieldDefinitionResolver(indexFormatVersion));
    Assert.assertFalse(validations.indexedAsGeoShape(path("any_path"), Optional.empty()));
    Assert.assertFalse(validations.indexedAsGeoShape(path("any.nested_path"), Optional.empty()));
    Assert.assertFalse(
        validations.indexedAsGeoShape(path("any.very.nested_path"), Optional.empty()));
  }

  @Theory
  public void testIndexedAsGeoPoint(IndexFormatVersion indexFormatVersion) {
    var validations = geoMappingValidations(indexFormatVersion);

    Assert.assertTrue(validations.indexedAsGeoPoint(path("geo_with_shapes"), Optional.empty()));
    Assert.assertTrue(validations.indexedAsGeoPoint(path("geo_no_shapes"), Optional.empty()));
    Assert.assertTrue(validations.indexedAsGeoPoint(path("nested_static.geo"), Optional.empty()));
    Assert.assertTrue(validations.indexedAsGeoPoint(path("geo_and_document"), Optional.empty()));
    Assert.assertTrue(
        validations.indexedAsGeoPoint(path("geo_and_document_and_string"), Optional.empty()));

    Assert.assertFalse(
        validations.indexedAsGeoPoint(path("geo_with_shapes.does_not_exist"), Optional.empty()));
    Assert.assertFalse(validations.indexedAsGeoPoint(path("dynamic"), Optional.empty()));
    Assert.assertFalse(
        validations.indexedAsGeoPoint(path("dynamic.any_nested_under_dynamic"), Optional.empty()));
    Assert.assertFalse(validations.indexedAsGeoPoint(path("not_a_path"), Optional.empty()));
    Assert.assertFalse(validations.indexedAsGeoPoint(path("not_a.nested_path"), Optional.empty()));
  }

  @Theory
  public void testIndexedAsGeoShape(IndexFormatVersion indexFormatVersion) {
    var validations = geoMappingValidations(indexFormatVersion);

    Assert.assertTrue(validations.indexedAsGeoShape(path("geo_with_shapes"), Optional.empty()));
    Assert.assertTrue(validations.indexedAsGeoShape(path("nested_static.geo"), Optional.empty()));
    Assert.assertTrue(validations.indexedAsGeoShape(path("geo_and_document"), Optional.empty()));
    Assert.assertTrue(
        validations.indexedAsGeoShape(path("geo_and_document_and_string"), Optional.empty()));

    // as geo but with shape=false
    Assert.assertFalse(validations.indexedAsGeoShape(path("geo_no_shapes"), Optional.empty()));
    Assert.assertFalse(
        validations.indexedAsGeoShape(path("geo_and_document_no_shapes"), Optional.empty()));

    // not indexed as geo
    Assert.assertFalse(
        validations.indexedAsGeoPoint(path("geo_with_shapes.does_not_exist"), Optional.empty()));
    Assert.assertFalse(validations.indexedAsGeoShape(path("dynamic"), Optional.empty()));
    Assert.assertFalse(
        validations.indexedAsGeoShape(path("dynamic.any_nested_under_dynamic"), Optional.empty()));
    Assert.assertFalse(validations.indexedAsGeoShape(path("not_a_path"), Optional.empty()));
    Assert.assertFalse(validations.indexedAsGeoShape(path("not_a.nested_path"), Optional.empty()));
  }

  @Test
  public void validKnnVectorFieldShouldPassValidation() throws InvalidQueryException {
    var resolver = Mockito.mock(SearchFieldDefinitionResolver.class);
    var checks = new SearchQueryTimeMappingChecks(resolver);
    var path = FieldPath.newRoot("foo");
    var embeddedRoot = FieldPath.newRoot("bar");
    var definition = new KnnVectorFieldDefinition(100, VectorSimilarity.EUCLIDEAN);
    var fieldDefinition =
        new FieldDefinition(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(definition),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    Mockito.when(resolver.getFieldDefinition(path, Optional.of(embeddedRoot)))
        .thenReturn(Optional.of(fieldDefinition));

    checks.validateVectorField(path, Optional.of(embeddedRoot), 100);
  }

  @Test(expected = InvalidQueryException.class)
  public void shouldFailKnnVectorValidationOnDimensionMismatch() throws InvalidQueryException {
    var resolver = Mockito.mock(SearchFieldDefinitionResolver.class);
    var checks = new SearchQueryTimeMappingChecks(resolver);
    var path = FieldPath.newRoot("foo");
    var embeddedRoot = FieldPath.newRoot("bar");
    var definition = new KnnVectorFieldDefinition(100, VectorSimilarity.EUCLIDEAN);
    var fieldDefinition =
        new FieldDefinition(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(definition),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    Mockito.when(resolver.getFieldDefinition(path, Optional.of(embeddedRoot)))
        .thenReturn(Optional.of(fieldDefinition));

    TestUtils.assertThrows(
        "vector field is indexed with 100 dimensions but queried with 999",
        InvalidQueryException.class,
        () -> checks.validateVectorField(path, Optional.of(embeddedRoot), 999));

    checks.validateVectorField(path, Optional.of(embeddedRoot), 999);
  }

  @Test(expected = InvalidQueryException.class)
  public void shouldFailKnnVectorValidationWhenPathIsNotIndexedAsVector()
      throws InvalidQueryException {
    var resolver = Mockito.mock(SearchFieldDefinitionResolver.class);
    var checks = new SearchQueryTimeMappingChecks(resolver);
    var path = FieldPath.newRoot("foo");
    var embeddedRoot = FieldPath.newRoot("bar");

    Mockito.when(
            resolver
                .getFieldDefinition(path, Optional.of(embeddedRoot))
                .flatMap(FieldDefinition::knnVectorFieldDefinition))
        .thenReturn(Optional.empty());

    TestUtils.assertThrows(
        String.format("%s must be indexed as \"vector\".", path),
        InvalidQueryException.class,
        () -> checks.validateVectorField(path, Optional.of(embeddedRoot), 100));

    checks.validateVectorField(path, Optional.of(embeddedRoot), 100);
  }

  @Theory
  public void testHighlightInvalidStringFieldNotStored(IndexFormatVersion indexFormatVersion) {
    SearchQueryTimeMappingChecks validations =
        new SearchQueryTimeMappingChecks(
            getIndexDefinition(highlightStoreFalseMapping())
                .createFieldDefinitionResolver(indexFormatVersion));

    InvalidQueryException e =
        Assert.assertThrows(
            InvalidQueryException.class,
            () ->
                validations.validatePathStringStorage(
                    new StringFieldPath(FieldPath.newRoot("a")), Optional.empty()));
    Assert.assertTrue(e.getMessage().contains("Index definition specifies store:false"));
  }

  @Theory
  public void testHighlightInvalidStringFieldNotIndexed(IndexFormatVersion indexFormatVersion) {
    SearchQueryTimeMappingChecks validations =
        new SearchQueryTimeMappingChecks(
            getIndexDefinition(emptyDynamicFalseMapping())
                .createFieldDefinitionResolver(indexFormatVersion));

    TestUtils.assertThrows(
        "Highlights cannot be generated. Path: \"a\" is not stored statically or "
            + "dynamically indexed",
        InvalidQueryException.class,
        () ->
            validations.validatePathStringStorage(
                new StringFieldPath(FieldPath.newRoot("a")), Optional.empty()));
  }

  @Theory
  public void testIsHighlightableStringField(IndexFormatVersion indexFormatVersion) {
    SearchQueryTimeMappingChecks withDynamicMapping =
        new SearchQueryTimeMappingChecks(
            getIndexDefinition(dynamic()).createFieldDefinitionResolver(indexFormatVersion));
    SearchQueryTimeMappingChecks withUnstoredStringField =
        new SearchQueryTimeMappingChecks(
            getIndexDefinition(highlightStoreFalseMapping())
                .createFieldDefinitionResolver(indexFormatVersion));

    StringPath path = new StringFieldPath(FieldPath.newRoot("a"));

    Assert.assertTrue(withDynamicMapping.isHighlightableStringField(path, Optional.empty()));
    Assert.assertFalse(withUnstoredStringField.isHighlightableStringField(path, Optional.empty()));
  }

  @Test
  public void testIsIndexedAsDate() {
    SearchQueryTimeMappingChecks withDynamicMapping =
        new SearchQueryTimeMappingChecks(
            getIndexDefinition(dynamic())
                .createFieldDefinitionResolver(IndexFormatVersion.MAX_SUPPORTED_VERSION));

    SearchQueryTimeMappingChecks withExplicitlyDefinedField =
        new SearchQueryTimeMappingChecks(
            getIndexDefinition(
                    DocumentFieldDefinitionBuilder.builder()
                        .dynamic(false)
                        .field(
                            "myDate",
                            field().date(DateFieldDefinitionBuilder.builder().build()).build())
                        .build())
                .createFieldDefinitionResolver(IndexFormatVersion.MAX_SUPPORTED_VERSION));

    FieldPath path = FieldPath.newRoot("myDate");

    Assert.assertTrue(withDynamicMapping.indexedAsDate(path, Optional.empty()));
    Assert.assertTrue(withExplicitlyDefinedField.indexedAsDate(path, Optional.empty()));
  }

  @Test
  public void testIsIndexedAsNumber() {
    SearchQueryTimeMappingChecks withDynamicMapping =
        new SearchQueryTimeMappingChecks(
            getIndexDefinition(dynamic())
                .createFieldDefinitionResolver(IndexFormatVersion.MAX_SUPPORTED_VERSION));

    SearchQueryTimeMappingChecks withExplicitlyDefinedField =
        new SearchQueryTimeMappingChecks(
            getIndexDefinition(
                    DocumentFieldDefinitionBuilder.builder()
                        .dynamic(false)
                        .field(
                            "myNumber",
                            field()
                                .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
                                .build())
                        .build())
                .createFieldDefinitionResolver(IndexFormatVersion.MAX_SUPPORTED_VERSION));

    FieldPath path = FieldPath.newRoot("myNumber");

    Assert.assertTrue(withDynamicMapping.indexedAsNumber(path, Optional.empty()));
    Assert.assertTrue(withExplicitlyDefinedField.indexedAsNumber(path, Optional.empty()));
  }

  @Test
  public void testIsIndexedAsToken() {
    SearchQueryTimeMappingChecks withDynamicMapping =
        new SearchQueryTimeMappingChecks(
            getIndexDefinition(dynamic())
                .createFieldDefinitionResolver(IndexFormatVersion.MAX_SUPPORTED_VERSION));

    SearchQueryTimeMappingChecks withExplicitlyDefinedField =
        new SearchQueryTimeMappingChecks(
            getIndexDefinition(
                    DocumentFieldDefinitionBuilder.builder()
                        .dynamic(false)
                        .field(
                            "myToken",
                            field().token(TokenFieldDefinitionBuilder.builder().build()).build())
                        .build())
                .createFieldDefinitionResolver(IndexFormatVersion.MAX_SUPPORTED_VERSION));

    FieldPath path = FieldPath.newRoot("myToken");

    Assert.assertFalse(withDynamicMapping.indexedAsToken(path, Optional.empty()));
    Assert.assertTrue(withExplicitlyDefinedField.indexedAsToken(path, Optional.empty()));
  }

  @Test
  public void testIsIndexedAsBoolean() {
    SearchQueryTimeMappingChecks withDynamicMapping =
        new SearchQueryTimeMappingChecks(
            getIndexDefinition(dynamic())
                .createFieldDefinitionResolver(IndexFormatVersion.MAX_SUPPORTED_VERSION));

    SearchQueryTimeMappingChecks withExplicitlyDefinedField =
        new SearchQueryTimeMappingChecks(
            getIndexDefinition(
                    DocumentFieldDefinitionBuilder.builder()
                        .dynamic(false)
                        .field(
                            "myBoolean",
                            field().bool(BooleanFieldDefinitionBuilder.builder().build()).build())
                        .build())
                .createFieldDefinitionResolver(IndexFormatVersion.MAX_SUPPORTED_VERSION));

    FieldPath path = FieldPath.newRoot("myBoolean");

    Assert.assertTrue(withDynamicMapping.indexedAsBoolean(path, Optional.empty()));
    Assert.assertTrue(withExplicitlyDefinedField.indexedAsBoolean(path, Optional.empty()));
  }

  @Test
  public void testIsIndexedAsObjectId() {
    SearchQueryTimeMappingChecks withDynamicMapping =
        new SearchQueryTimeMappingChecks(
            getIndexDefinition(dynamic())
                .createFieldDefinitionResolver(IndexFormatVersion.MAX_SUPPORTED_VERSION));

    SearchQueryTimeMappingChecks withExplicitlyDefinedField =
        new SearchQueryTimeMappingChecks(
            getIndexDefinition(
                    DocumentFieldDefinitionBuilder.builder()
                        .dynamic(false)
                        .field(
                            "myObjectId",
                            field()
                                .objectid(ObjectIdFieldDefinitionBuilder.builder().build())
                                .build())
                        .build())
                .createFieldDefinitionResolver(IndexFormatVersion.MAX_SUPPORTED_VERSION));

    FieldPath path = FieldPath.newRoot("myObjectId");

    Assert.assertTrue(withDynamicMapping.indexedAsObjectId(path, Optional.empty()));
    Assert.assertTrue(withExplicitlyDefinedField.indexedAsObjectId(path, Optional.empty()));
  }

  private SearchQueryTimeMappingChecks geoMappingValidations(
      IndexFormatVersion indexFormatVersion) {
    return new SearchQueryTimeMappingChecks(
        getIndexDefinition(geoMappings()).createFieldDefinitionResolver(indexFormatVersion));
  }

  private SearchIndexDefinition getIndexDefinition(DocumentFieldDefinition mappings) {
    return SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
  }

  private DocumentFieldDefinition highlightStoreFalseMapping() {
    return DocumentFieldDefinitionBuilder.builder()
        .dynamic(false)
        .field(
            "a",
            field().string(StringFieldDefinitionBuilder.builder().store(false).build()).build())
        .build();
  }

  private DocumentFieldDefinition geoMappings() {
    return DocumentFieldDefinitionBuilder.builder()
        .dynamic(false)
        .field("dynamic", field().document(dynamic()).build())
        .field("geo_no_shapes", field().geo(geo(false)).build())
        .field("geo_with_shapes", field().geo(geo(true)).build())
        .field("geo_and_document", field().geo(geo(true)).document(dynamic()).build())
        .field("geo_and_document_no_shapes", field().geo(geo(false)).document(dynamic()).build())
        .field(
            "geo_and_document_and_string",
            field()
                .geo(geo(true))
                .document(dynamic())
                .string(StringFieldDefinitionBuilder.builder().build())
                .build())
        .field(
            "nested_static",
            field()
                .document(
                    DocumentFieldDefinitionBuilder.builder()
                        .field("geo", field().geo(geo(true)).build())
                        .build())
                .build())
        .build();
  }

  private DocumentFieldDefinition dynamic() {
    return DocumentFieldDefinitionBuilder.builder().dynamic(true).build();
  }

  private DocumentFieldDefinition emptyDynamicFalseMapping() {
    return DocumentFieldDefinitionBuilder.builder().dynamic(false).build();
  }

  private FieldDefinitionBuilder field() {
    return FieldDefinitionBuilder.builder();
  }

  private GeoFieldDefinition geo(boolean indexShapes) {
    return GeoFieldDefinitionBuilder.builder().indexShapes(indexShapes).build();
  }

  private FieldPath path(String name) {
    return FieldPath.parse(name);
  }
}
