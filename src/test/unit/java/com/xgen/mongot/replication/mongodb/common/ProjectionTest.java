package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static org.junit.Assert.assertEquals;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.BooleanFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DateFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.junit.Assert;
import org.junit.Test;

public class ProjectionTest {

  private enum ProjectionType {
    INCLUDE(1),
    EXCLUDE(0);

    private final int value;

    ProjectionType(int value) {
      this.value = value;
    }
  }

  @Test
  public void testVectorDefinitionForFindQuery() {

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("a1.a2.a3")
            .withFilterPath("b")
            .withFilterPath("c1.c2")
            .withCosineVectorField("d1.d2.d3.d4", 3)
            .withCosineVectorField("a1.a2", 3)
            .withCosineVectorField("c1", 3)
            .withCosineVectorField("operationType", 3)
            .build();

    Optional<Bson> projection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        projection,
        getDefaultFindQueryPaths(definition),
        "a1.a2",
        "b",
        "c1",
        "d1.d2.d3.d4",
        "operationType");
  }

  @Test
  public void testVectorDefinitionForChangeStreamQuery() {

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("a1.a2.a3")
            .withFilterPath("b")
            .withFilterPath("c1.c2")
            .withCosineVectorField("d1.d2.d3.d4", 3)
            .withCosineVectorField("a1.a2", 3)
            .withCosineVectorField("c1", 3)
            .withCosineVectorField("operationType", 3)
            .build();

    Optional<Bson> projection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        projection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.a1.a2",
        "fullDocument.b",
        "fullDocument.c1",
        "fullDocument.d1.d2.d3.d4",
        "fullDocument.operationType");
  }

  @Test
  public void testVectorDefinitionTopLevelFieldsWithDollarSignFilter() {
    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("$bad_filter")
            .withCosineVectorField("good_filter.field", 3)
            .build();

    Assert.assertTrue(Projection.forRegularQuery(definition).isEmpty());
    Assert.assertTrue(Projection.forChangeStream(definition).isEmpty());
  }

  @Test
  public void testVectorDefinitionTopLevelFieldsWithDollarSignVector() {

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("good_filter")
            .withCosineVectorField("$bad_data", 3)
            .build();

    Assert.assertTrue(Projection.forRegularQuery(definition).isEmpty());
    Assert.assertTrue(Projection.forChangeStream(definition).isEmpty());
  }

  @Test
  public void testVectorDefinitionNestedFieldsWithDollarSign() {
    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("a.b.c")
            .withFilterPath("parent.$bad_filter")
            .withCosineVectorField("parent.$bad_data", 3)
            .withCosineVectorField("another_parent.$bad_child.$bad_grandchild", 3)
            .withCosineVectorField("another_parent.good_child", 3)
            .build();
    Optional<Bson> queryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        queryProjection,
        getDefaultFindQueryPaths(definition),
        "parent",
        "another_parent",
        "a.b.c");

    Optional<Bson> changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.parent",
        "fullDocument.another_parent",
        "fullDocument.a.b.c");
  }

  @Test
  public void testVectorDefinitionWithIndexNamespaceForFindQuery() {

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("b")
            .withCustomEngineVectorField("d1.d2", 3)
            .indexNamespace("meta.tenant")
            .build();

    Optional<Bson> projection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        projection,
        getDefaultFindQueryPaths(definition),
        "b",
        "d1.d2",
        "meta.tenant");
  }

  @Test
  public void testVectorDefinitionWithIndexNamespaceForChangeStreamQuery() {

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("b")
            .withCustomEngineVectorField("d1.d2", 3)
            .indexNamespace("meta.tenant")
            .build();

    Optional<Bson> projection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        projection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.b",
        "fullDocument.d1.d2",
        "fullDocument.meta.tenant");
  }

  @Test
  public void testVectorDefinitionWithDollarSignIndexNamespace() {
    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withCustomEngineVectorField("good_vector", 3)
            .indexNamespace("$bad_namespace")
            .build();

    Assert.assertTrue(Projection.forRegularQuery(definition).isEmpty());
    Assert.assertTrue(Projection.forChangeStream(definition).isEmpty());
  }

  @Test
  public void testFlatFieldsAreProjectedIndependently() {

    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "firstName",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .field(
                "lastName",
                FieldDefinitionBuilder.builder()
                    .date(DateFieldDefinitionBuilder.builder().build())
                    .build())
            .build();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "firstName",
        "lastName");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.firstName",
        "fullDocument.lastName");
  }

  @Test
  public void testSearchAutoEmbedProjectsSourceFieldNotDeclaredName() {
    // CLOUDP-353553 (A2 shape): the autoEmbed field name ("embedding") differs from its sourceField
    // ("body"). Embedding reads the text from the sourceField, so the projection must include
    // "body"; otherwise the source text is stripped from the scanned document and no vector is
    // generated.
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("body")))
                    .build())
            .build();
    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .autoEmbeddingDefinitionVersion(0L)
            .build();

    assertThat(getDocument(Projection.forRegularQuery(definition)).keySet()).contains("body");
    assertThat(getDocument(Projection.forChangeStream(definition)).keySet())
        .contains("fullDocument.body");
  }

  @Test
  public void testNestedSearchAutoEmbedProjectsSourceField() {
    // B1 shape: nested-only autoEmbed. The sourceField "product.description" must be projected.
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "product",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic(false)
                            .field(
                                "desc_embedding",
                                FieldDefinitionBuilder.builder()
                                    .searchAutoEmbed(
                                        new SearchAutoEmbedFieldDefinition(
                                            "voyage-3-large",
                                            FieldPath.parse("product.description")))
                                    .build())
                            .build())
                    .build())
            .build();
    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .autoEmbeddingDefinitionVersion(0L)
            .build();

    assertThat(getDocument(Projection.forRegularQuery(definition)).keySet())
        .contains("product.description");
    assertThat(getDocument(Projection.forChangeStream(definition)).keySet())
        .contains("fullDocument.product.description");
  }

  @Test
  public void testSearchAutoEmbedWithScalarAlternativeProjectsBothPaths() {
    // D2 shape: one field carries both an autoEmbed (sourceField "body") and a scalar string
    // alternative. The declared field name ("title", scalar data) AND the sourceField ("body",
    // embed text) must both be projected.
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "title",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("body")))
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();
    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .autoEmbeddingDefinitionVersion(0L)
            .build();

    var keys = getDocument(Projection.forRegularQuery(definition)).keySet();
    assertThat(keys).contains("title");
    assertThat(keys).contains("body");
  }

  @Test
  public void testRootLevelEmptyFieldsReturnsEmptyProjection() {

    var mappings = DocumentFieldDefinitionBuilder.builder().dynamic(false).build();
    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    Assert.assertFalse(Projection.forRegularQuery(definition).isPresent());
    Assert.assertFalse(Projection.forChangeStream(definition).isPresent());
  }

  @Test
  public void testNestedLevelEmptyFieldsReturnsParentField() {

    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "data",
                FieldDefinitionBuilder.builder()
                    .document(DocumentFieldDefinitionBuilder.builder().dynamic(false).build())
                    .build())
            .build();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    Assert.assertFalse(Projection.forRegularQuery(definition).isPresent());
    Assert.assertFalse(Projection.forChangeStream(definition).isPresent());
  }

  @Test
  public void testEmptyMappingForIdFieldStillProjectsIt() {

    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("_id", FieldDefinitionBuilder.builder().build())
            .build();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE, findQueryProjection, getDefaultFindQueryPaths(definition));

    var doc = getDocument(Projection.forChangeStream(definition));
    assertEquals(getDefaultChangeStreamPaths(definition).size(), doc.size());
    Assert.assertTrue(doc.containsKey("fullDocument._id"));
    getDefaultChangeStreamPaths(definition)
        .forEach(field -> Assert.assertTrue(doc.containsKey(field)));
  }

  @Test
  public void testNestedLevelEmptyStaticMappingIsProjected() {

    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "data",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic(false)
                            .field("public", FieldDefinitionBuilder.builder().build())
                            .build())
                    .build())
            .build();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "data.public");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.data.public");
  }

  @Test
  public void testNestedLevelEmptyDynamicMappingIsProjected() {

    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "data",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic(true)
                            .field("public", FieldDefinitionBuilder.builder().build())
                            .build())
                    .build())
            .build();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE, findQueryProjection, getDefaultFindQueryPaths(definition), "data");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.data");
  }

  @Test
  public void testEmbeddedDocumentWithStaticMappingIsProjectedIndependentlyForEachField() {
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "data",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic(false)
                            .field(
                                "firstName",
                                FieldDefinitionBuilder.builder()
                                    .string(StringFieldDefinitionBuilder.builder().build())
                                    .build())
                            .field(
                                "lastName",
                                FieldDefinitionBuilder.builder()
                                    .date(DateFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build())
            .build();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "data.firstName",
        "data.lastName");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.data.firstName",
        "fullDocument.data.lastName");
  }

  @Test
  public void testEmbeddedDocumentWithDynamicMappingProjectsParentField() {

    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "patient",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic(false)
                            .field(
                                "data",
                                FieldDefinitionBuilder.builder()
                                    .document(
                                        DocumentFieldDefinitionBuilder.builder()
                                            .dynamic(false)
                                            .field(
                                                "personal",
                                                FieldDefinitionBuilder.builder()
                                                    .document(
                                                        DocumentFieldDefinitionBuilder.builder()
                                                            .dynamic(true)
                                                            .field(
                                                                "name",
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
                    .build())
            .build();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "patient.data.personal");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.patient.data.personal");
  }

  @Test
  public void testFlatFieldNameWithDotsReturnsEmptyProject() {

    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "first.name",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .field(
                "last.name",
                FieldDefinitionBuilder.builder()
                    .date(DateFieldDefinitionBuilder.builder().build())
                    .build())
            .build();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    var projection = Projection.forRegularQuery(definition);
    Assert.assertFalse(projection.isPresent());
  }

  @Test
  public void testTopLevelFieldWithDollarSign() {
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "$bad_field",
                FieldDefinitionBuilder.builder()
                    .date(DateFieldDefinitionBuilder.builder().build())
                    .build())
            .field(
                "parent",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic(false)
                            .field(
                                "nested",
                                FieldDefinitionBuilder.builder()
                                    .date(DateFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build())
            .build();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    Assert.assertTrue(Projection.forRegularQuery(definition).isEmpty());
    Assert.assertTrue(Projection.forChangeStream(definition).isEmpty());
  }

  @Test
  public void testNestedFieldsWithDollarSign() {
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "bad_parent",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic(false)
                            .field(
                                "$last",
                                FieldDefinitionBuilder.builder()
                                    .date(DateFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build())
            .field(
                "good_parent",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic(false)
                            .field(
                                "la$t",
                                FieldDefinitionBuilder.builder()
                                    .date(DateFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build())
            .build();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "bad_parent",
        "good_parent.la$t");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.bad_parent",
        "fullDocument.good_parent.la$t");
  }

  @Test
  public void testNestedScalarFieldNameWithDotsProjectsParentField() {
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "data",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic(false)
                            .field(
                                "first.name",
                                FieldDefinitionBuilder.builder()
                                    .string(StringFieldDefinitionBuilder.builder().build())
                                    .build())
                            .field(
                                "last.name",
                                FieldDefinitionBuilder.builder()
                                    .date(DateFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build())
            .build();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE, findQueryProjection, getDefaultFindQueryPaths(definition), "data");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.data");
  }

  @Test
  public void testStoredSourcePathsContainingFieldNamesWithDotsProjectsFullDocument() {

    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "z",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();

    var storedSource =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.INCLUSION, List.of("a..b"));

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    // expect empty projection (request full document)
    Assert.assertTrue(Projection.forRegularQuery(definition).isEmpty());
    Assert.assertTrue(Projection.forChangeStream(definition).isEmpty());
  }

  @Test
  public void testStoredSourcePathsWhichStartWithEmptyFieldsProjectsFullDocument() {

    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "z",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();

    var storedSource =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.INCLUSION, List.of(".a"));

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    // expect empty projection (request full document)
    Assert.assertTrue(Projection.forRegularQuery(definition).isEmpty());
    Assert.assertTrue(Projection.forChangeStream(definition).isEmpty());
  }

  @Test
  public void testStoredSourcePathsWhichEndWithEmptyFieldsProjectsFullDocument() {

    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "z",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();

    var storedSource =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.INCLUSION, List.of("a."));

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    // expect empty projection (request full document)
    Assert.assertTrue(Projection.forRegularQuery(definition).isEmpty());
    Assert.assertTrue(Projection.forChangeStream(definition).isEmpty());
  }

  @Test
  public void testNestedDocumentFieldNameWithDotsProjectsParentField() {
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "data",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic(false)
                            .field(
                                "personal.data",
                                FieldDefinitionBuilder.builder()
                                    .document(
                                        DocumentFieldDefinitionBuilder.builder()
                                            .dynamic(false)
                                            .field(
                                                "name",
                                                FieldDefinitionBuilder.builder()
                                                    .string(
                                                        StringFieldDefinitionBuilder.builder()
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE, findQueryProjection, getDefaultFindQueryPaths(definition), "data");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.data");
  }

  @Test
  public void testFlatEmptyFieldNameReturnsEmptyProject() {
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .field(
                "name",
                FieldDefinitionBuilder.builder()
                    .date(DateFieldDefinitionBuilder.builder().build())
                    .build())
            .build();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    var projection = Projection.forRegularQuery(definition);
    Assert.assertFalse(projection.isPresent());
  }

  @Test
  public void testNestedDocumentEmptyFieldNameProjectsParentField() {
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "patient",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic(false)
                            .field(
                                "",
                                FieldDefinitionBuilder.builder()
                                    .document(
                                        DocumentFieldDefinitionBuilder.builder()
                                            .field(
                                                "first.name",
                                                FieldDefinitionBuilder.builder()
                                                    .string(
                                                        StringFieldDefinitionBuilder.builder()
                                                            .build())
                                                    .build())
                                            .field(
                                                "last.name",
                                                FieldDefinitionBuilder.builder()
                                                    .date(
                                                        DateFieldDefinitionBuilder.builder()
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "patient");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.patient");
  }

  @Test
  public void testNestedTerminalEmptyFieldNameProjectsParentField() {
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "patient",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .dynamic(false)
                            .field(
                                "",
                                FieldDefinitionBuilder.builder()
                                    .string(StringFieldDefinitionBuilder.builder().build())
                                    .build())
                            .field(
                                "lastName",
                                FieldDefinitionBuilder.builder()
                                    .date(DateFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build())
            .build();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "patient");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.patient");
  }

  @Test
  public void testMixedScalarAndDocumentDefinitionsOnTheSameLevelProjectsEntireField() {

    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "patient",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "data",
                                FieldDefinitionBuilder.builder()
                                    .document(
                                        DocumentFieldDefinitionBuilder.builder()
                                            .field(
                                                "nested",
                                                FieldDefinitionBuilder.builder()
                                                    .string(
                                                        StringFieldDefinitionBuilder.builder()
                                                            .build())
                                                    .build())
                                            .build())
                                    .string(StringFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build())
            .build();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "patient.data");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.patient.data");
  }

  @Test
  public void testRootDynamicMappingReturnsEmptyProjection() {
    var mappings = DocumentFieldDefinitionBuilder.builder().dynamic(true).build();
    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings).build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    Assert.assertFalse(findQueryProjection.isPresent());

    var changeStreamProjection = Projection.forChangeStream(definition);
    Assert.assertFalse(changeStreamProjection.isPresent());
  }

  @Test
  public void testDeterministicProjectPipelineSortRegardlessOfTheMappingInput() {

    var mappings1 =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .field(
                "1",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();

    var mappings2 =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "1",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();

    var projection1 =
        Projection.forRegularQuery(
            SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings1).build());

    var projection2 =
        Projection.forRegularQuery(
            SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION).mappings(mappings2).build());

    assertEquals(projection1, projection2);
  }

  @Test
  public void testAllIncludedStoredSource() {

    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "c",
                                FieldDefinitionBuilder.builder()
                                    .string(StringFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build())
            .build();

    var storedSource = StoredSourceDefinition.createIncludeAll();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    // expect empty projection (request full document)
    Assert.assertTrue(Projection.forRegularQuery(definition).isEmpty());
    Assert.assertTrue(Projection.forChangeStream(definition).isEmpty());
  }

  @Test
  public void testAllExcludedStoredSourceProjectsIndexedFieldsOnly() {

    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "c",
                                FieldDefinitionBuilder.builder()
                                    .string(StringFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build())
            .build();

    var storedSource = StoredSourceDefinition.createExcludeAll();

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    // expect projection to contain only indexed fields
    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE, findQueryProjection, getDefaultFindQueryPaths(definition), "a.c");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.a.c");
  }

  @Test
  public void testChangeStreamInclusionProjectionIncludesRenameDestinationNamespace() {
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();

    var storedSource =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.INCLUSION, List.of("a"));

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.a");
  }

  @Test
  public void testPartiallyIncludedStoredSource() {

    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "c",
                                FieldDefinitionBuilder.builder()
                                    .string(StringFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build())
            .build();

    var storedSource =
        StoredSourceDefinition.create(
            StoredSourceDefinition.Mode.INCLUSION, List.of("a.d", "b.e.f"));

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    // expect empty projection (request full document)
    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "a.c",
        "a.d",
        "b.e.f");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.a.c",
        "fullDocument.a.d",
        "fullDocument.b.e.f");
  }

  @Test
  public void testPartiallyExcludedStoredSourceWithNoIndexingPathsOverlap() {
    /*
     * [a.b] is excluded in Stored Source config and {a.c.e}, {a.c.d} are indexed:
     *
     *   a
     *  / \
     *[b]  c
     *    / \
     * {d}  {e}
     */
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "c",
                                FieldDefinitionBuilder.builder()
                                    .document(
                                        DocumentFieldDefinitionBuilder.builder()
                                            .field(
                                                "e",
                                                FieldDefinitionBuilder.builder()
                                                    .string(
                                                        StringFieldDefinitionBuilder.builder()
                                                            .build())
                                                    .build())
                                            .field(
                                                "d",
                                                FieldDefinitionBuilder.builder()
                                                    .bool(
                                                        BooleanFieldDefinitionBuilder.builder()
                                                            .build())
                                                    .build())
                                            .build())
                                    .string(StringFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build())
            .build();

    var storedSource =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.EXCLUSION, List.of("a.b", "_id"));

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    // expect exclusive projection based on stored source, since it will include the indexed fields
    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.EXCLUDE, findQueryProjection, getDefaultFindQueryPaths(definition), "a.b");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.EXCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.a.b");
  }

  @Test
  public void partiallyExcludedMappingWithDisjointPath() {
    /*
     * [a, f.g] is excluded in Stored Source config, everything under {a.c} is dynamically indexed:
     *
     *   [a]       f
     *   / \      / \
     * [b] [c]   [g]  h
     *     / \
     * [{d}] [{e}]
     *
     * Therefore, we can only exclude f.g in $project
     */
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "c",
                                FieldDefinitionBuilder.builder()
                                    .document(
                                        DocumentFieldDefinitionBuilder.builder()
                                            .dynamic(true)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    var storedSource =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.EXCLUSION, List.of("a", "f.g"));

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.EXCLUDE, findQueryProjection, getDefaultFindQueryPaths(definition), "f.g");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.EXCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.f.g");
  }

  @Test
  public void partialDynamicMappingWithIncludeNone() {
    /*
     * [] is included in Stored Source config, everything under {a.c} is dynamically indexed:
     *
     *   a        f
     *  / \      / \
     * b    c   g   h
     *     / \
     *  {d}   {e}
     *
     * Therefore, we only need to project a.c
     */
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "c",
                                FieldDefinitionBuilder.builder()
                                    .document(
                                        DocumentFieldDefinitionBuilder.builder()
                                            .dynamic(true)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    var storedSource =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.INCLUSION, List.of());

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE, findQueryProjection, getDefaultFindQueryPaths(definition), "a.c");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.a.c");
  }

  @Test
  public void partialDynamicMappingWithExcludeNone() {
    /*
     * [] is excluded in Stored Source config, everything under {a.c} is dynamically indexed:
     *
     *   a        f
     *  / \      / \
     * b    c   g   h
     *     / \
     *  {d}   {e}
     *
     * Therefore, we can exclude nothing
     */
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "c",
                                FieldDefinitionBuilder.builder()
                                    .document(
                                        DocumentFieldDefinitionBuilder.builder()
                                            .dynamic(true)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    var storedSource =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.EXCLUSION, List.of());

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertThat(findQueryProjection).isEmpty();
    var changeStreamProjection = Projection.forChangeStream(definition);
    assertThat(changeStreamProjection).isEmpty();
  }

  @Test
  public void partiallyExcludedStoredSourceIsChildOfIndexedPath() {
    /*
     * [a.c.e] is excluded in Stored Source config, everything under {a.c} is dynamically indexed:
     *
     *   a
     *  / \
     * b   c
     *   /  \
     * {d} [{e}]
     */
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "c",
                                FieldDefinitionBuilder.builder()
                                    .document(
                                        DocumentFieldDefinitionBuilder.builder()
                                            .dynamic(true)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    var storedSource =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.EXCLUSION, List.of("a.c.e"));

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    // expect empty projection (request full document) because of include and exclude overlap
    Assert.assertTrue(Projection.forRegularQuery(definition).isEmpty());
    Assert.assertTrue(Projection.forChangeStream(definition).isEmpty());
  }

  @Test
  public void partiallyExcludedStoredSourceIsParentOfDynamicIndex() {
    /*
     * Everything under [a.c] is excluded in Stored Source config and {a.c.e} is indexed:
     *
     *   a
     *  / \
     * b  [c]
     *   /  \
     * [d] [{e}]
     */
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "c",
                                FieldDefinitionBuilder.builder()
                                    .document(
                                        DocumentFieldDefinitionBuilder.builder()
                                            .field(
                                                "e",
                                                FieldDefinitionBuilder.builder()
                                                    .string(
                                                        StringFieldDefinitionBuilder.builder()
                                                            .build())
                                                    .build())
                                            .build())
                                    .string(StringFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build())
            .build();

    var storedSource =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.EXCLUSION, List.of("a.c"));

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    // expect empty projection (request full document) because of include and exclude overlap
    Assert.assertTrue(Projection.forRegularQuery(definition).isEmpty());
    Assert.assertTrue(Projection.forChangeStream(definition).isEmpty());
  }

  @Test
  public void testPartiallyExcludedStoredSourceWithDynamicIndexingPathsOverlapAtTheSameNode() {
    /*
     * Everything under [a.c] is excluded in Stored Source config and {a.c} is dynamically indexed:
     *
     *   a
     *  / \
     * b  [c]
     *   /  \
     * [{d}] [{e}]
     */
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "c",
                                FieldDefinitionBuilder.builder()
                                    .document(
                                        DocumentFieldDefinitionBuilder.builder()
                                            .dynamic(true)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    var storedSource =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.EXCLUSION, List.of("a.c"));

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    // expect empty projection (request full document) because of include and exclude overlap
    Assert.assertTrue(Projection.forRegularQuery(definition).isEmpty());
    Assert.assertTrue(Projection.forChangeStream(definition).isEmpty());
  }

  @Test
  public void testPartiallyExcludedStoredSourceWithStaticIndexingPathsOverlapAtTheSameNode() {
    /*
     * Everything under [a.c] is excluded in Stored Source config and {a.c} is indexed:
     *
     *   a
     *  / \
     * b  [{c}]
     *   /  \
     * [d] [e]
     */
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "c",
                                FieldDefinitionBuilder.builder()
                                    .string(StringFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build())
            .build();

    var storedSource =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.EXCLUSION, List.of("a.c"));

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    // expect empty projection (request full document) because of include and exclude overlap
    Assert.assertTrue(Projection.forRegularQuery(definition).isEmpty());
    Assert.assertTrue(Projection.forChangeStream(definition).isEmpty());
  }

  @Test
  public void testStoredSourceAndIndexedFieldsDoNotCollideWhenStoredSourcePathIsBroader() {
    /*
     * Everything under [a] is included in Stored Source config and {a.c} is indexed:
     *
     *  [a]
     *  / \
     *[b]  [{c}]
     *   /  \
     * [d] [e]
     */
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "c",
                                FieldDefinitionBuilder.builder()
                                    .string(StringFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build())
            .build();

    var storedSource =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.INCLUSION, List.of("a"));

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    // expect only the broader path to be included
    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE, findQueryProjection, getDefaultFindQueryPaths(definition), "a");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.a");
  }

  @Test
  public void testStoredSourceAndIndexedFieldsDoNotCollideWhenIndexedPathIsBroader() {
    /*
     * Everything under [a.c] is included in Stored Source config and everything
     * under {a} is dynamically indexed:
     *
     *     a
     *    / \
     * {b}  [{c}]
     *      /  \
     *   [{d}] [{e}]
     */
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .document(DocumentFieldDefinitionBuilder.builder().dynamic(true).build())
                    .build())
            .build();

    var storedSource =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.INCLUSION, List.of("a.c"));

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    // expect only the broader path to be included
    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE, findQueryProjection, getDefaultFindQueryPaths(definition), "a");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.a");
  }

  @Test
  public void testPartiallyExcludedStoredSourceFullOverlapWithIndexingPaths() {
    /*
     * Everything [a.c] is indexed and everything except b is stored
     *
     *  {a}
     *  / \
     * b  [{c}]
     *    /  \
     * [{d}] [{e}]
     */
    var mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                "a",
                FieldDefinitionBuilder.builder()
                    .document(
                        DocumentFieldDefinitionBuilder.builder()
                            .field(
                                "c",
                                FieldDefinitionBuilder.builder()
                                    .string(StringFieldDefinitionBuilder.builder().build())
                                    .build())
                            .build())
                    .build())
            .build();

    var storedSource =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.EXCLUSION, List.of("b"));

    var definition =
        SearchIndexDefinitionBuilder.from(MOCK_INDEX_DEFINITION)
            .mappings(mappings)
            .storedSource(storedSource)
            .build();

    // expect to include everything except b
    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.EXCLUDE, findQueryProjection, getDefaultFindQueryPaths(definition), "b");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.EXCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.b");
  }

  @Test
  public void testVectorStoredSourceInclusion() {
    var ssd =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.INCLUSION, List.of("c", "d"));

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("a")
            .withCosineVectorField("b", 3)
            .storedSource(ssd)
            .build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "a",
        "b",
        "c",
        "d");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.a",
        "fullDocument.b",
        "fullDocument.c",
        "fullDocument.d");
  }

  @Test
  public void testVectorStoredSourceExclusion() {
    var ssd =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.EXCLUSION, List.of("c", "d"));

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("a")
            .withCosineVectorField("b", 3)
            .storedSource(ssd)
            .build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.EXCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "c",
        "d");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.EXCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.c",
        "fullDocument.d");
  }

  @Test
  public void testVectorStoredSourceInclusionOverlap() {
    var ssd =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.INCLUSION, List.of("b", "c"));

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("a")
            .withCosineVectorField("b", 3)
            .storedSource(ssd)
            .build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "a",
        "b",
        "c");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.a",
        "fullDocument.b",
        "fullDocument.c");
  }

  @Test
  public void testVectorStoredSourceExclusionOverlap() {
    var ssd =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.EXCLUSION, List.of("b", "c"));

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("a")
            .withCosineVectorField("b", 3)
            .storedSource(ssd)
            .build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.EXCLUDE, findQueryProjection, getDefaultFindQueryPaths(definition), "c");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.EXCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.c");
  }

  @Test
  public void testVectorStoredSourceInclusionParent() {
    var ssd =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.INCLUSION, List.of("b", "c"));

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("a")
            .withCosineVectorField("b.b.b", 3)
            .storedSource(ssd)
            .build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "a",
        "b",
        "c");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.a",
        "fullDocument.b",
        "fullDocument.c");
  }

  @Test
  public void testVectorStoredSourceExclusionParent() {
    var ssd =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.EXCLUSION, List.of("b", "c"));

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("a")
            .withCosineVectorField("b.b.b", 3)
            .storedSource(ssd)
            .build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.EXCLUDE, findQueryProjection, getDefaultFindQueryPaths(definition), "c");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.EXCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.c");
  }

  @Test
  public void testVectorStoredSourceIncludeAll() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> {
          VectorIndexDefinitionBuilder.builder()
              .withFilterPath("a")
              .withCosineVectorField("b", 3)
              .storedSource(StoredSourceDefinition.createIncludeAll())
              .build();
        });
  }

  @Test
  public void testVectorStoredSourceExcludeAll() {
    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("a")
            .withCosineVectorField("b", 3)
            .storedSource(StoredSourceDefinition.createExcludeAll())
            .build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "a",
        "b");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.a",
        "fullDocument.b");
  }

  @Test
  public void testVectorStoredSourceWithMultipleVectorFields() {
    var ssd =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.INCLUSION, List.of("d", "e"));

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("a")
            .withCosineVectorField("b", 3)
            .withCosineVectorField("c", 3)
            .withCosineVectorField("d", 3)
            .storedSource(ssd)
            .build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "a",
        "b",
        "c",
        "d",
        "e");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.a",
        "fullDocument.b",
        "fullDocument.c",
        "fullDocument.d",
        "fullDocument.e");
  }

  @Test
  public void testVectorStoredSourceExclusionWithFilterPaths() {
    var ssd =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.EXCLUSION, List.of("f", "g"));

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("a.b.c")
            .withFilterPath("d")
            .withCosineVectorField("e", 3)
            .storedSource(ssd)
            .build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.EXCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "f",
        "g");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.EXCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.f",
        "fullDocument.g");
  }

  @Test
  public void testVectorStoredSourceExclusionOfFilterField() {
    var ssd =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.EXCLUSION, List.of("a", "e"));

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withFilterPath("a")
            .withFilterPath("d")
            .withCosineVectorField("b", 3)
            .storedSource(ssd)
            .build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.EXCLUDE, findQueryProjection, getDefaultFindQueryPaths(definition), "e");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.EXCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.e");
  }

  @Test
  public void testVectorStoredSourceInclusionWithChildVectorField() {
    var ssd =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.INCLUSION, List.of("a", "b"));

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withCosineVectorField("a.a.a", 3)
            .storedSource(ssd)
            .build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "a",
        "b");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.a",
        "fullDocument.b");
  }

  @Test
  public void testVectorStoredSourceInclusionWithParentVectorField() {
    var ssd =
        StoredSourceDefinition.create(StoredSourceDefinition.Mode.INCLUSION, List.of("a.a.a", "b"));

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .withCosineVectorField("a", 3)
            .storedSource(ssd)
            .build();

    var findQueryProjection = Projection.forRegularQuery(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        findQueryProjection,
        getDefaultFindQueryPaths(definition),
        "a",
        "b");

    var changeStreamProjection = Projection.forChangeStream(definition);
    assertProjection(
        ProjectionType.INCLUDE,
        changeStreamProjection,
        getDefaultChangeStreamPaths(definition),
        "fullDocument.a",
        "fullDocument.b");
  }

  private void assertProjection(
      ProjectionType projectionType,
      Optional<Bson> projection,
      Set<String> defaultPaths,
      String... expectedPaths) {
    var doc = getDocument(projection);

    // check projection paths
    switch (projectionType) {
      case INCLUDE:
        assertThat(doc.keySet()).containsAtLeastElementsIn(expectedPaths);
        assertThat(doc.keySet()).containsAtLeastElementsIn(defaultPaths);
        assertEquals(expectedPaths.length + defaultPaths.size(), doc.size());
        break;
      case EXCLUDE:
        assertThat(doc.keySet()).containsExactlyElementsIn(expectedPaths);
        assertThat(doc.keySet()).containsNoneIn(defaultPaths);
        assertEquals(expectedPaths.length, doc.size());
    }

    // check expected paths have correct value
    Arrays.stream(expectedPaths)
        .forEach(
            field -> {
              assertThat(doc).containsKey(field);
              assertEquals(projectionType.value, doc.get(field).asInt32().getValue());
            });
  }

  private BsonDocument getDocument(Optional<Bson> projection) {
    return projection.orElseThrow().toBsonDocument(null, null);
  }

  private Set<String> getDefaultFindQueryPaths(IndexDefinition definition) {
    return Set.of("_id", definition.getIndexId().toString());
  }

  private Set<String> getDefaultChangeStreamPaths(IndexDefinition definition) {
    return Set.of(
        "fullDocument._id",
        "fullDocument." + definition.getIndexId(),
        "operationType",
        "resumeToken",
        "ns",
        "documentKey",
        "clusterTime",
        "updateDescription",
        "to");
  }
}
