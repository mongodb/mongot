package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.VectorDataFieldDefinitionBuilder;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class VectorIndexFieldMappingTest {

  @Test
  public void testSimple() {
    FieldPath path = FieldPath.parse("path");
    VectorDataFieldDefinition dataField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(path)
            .numDimensions(1)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    List<VectorIndexFieldDefinition> fields = List.of(dataField);
    VectorIndexFieldMapping mapping = VectorIndexFieldMapping.create(fields, Optional.empty());
    Assert.assertEquals(mapping.getFieldDefinition(path), Optional.of(dataField));
    Assert.assertFalse(mapping.subDocumentExists(path));
    Assert.assertFalse(mapping.isNestedRoot(path));
    Assert.assertFalse(mapping.hasNestedRoot());
  }

  @Test
  public void testSingleFilter() {
    FieldPath dataPath = FieldPath.parse("data");
    VectorDataFieldDefinition dataField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(dataPath)
            .numDimensions(1)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    FieldPath filterPath = FieldPath.parse("filter");
    VectorIndexFilterFieldDefinition filterField =
        VectorIndexFilterFieldDefinition.create(filterPath);
    List<VectorIndexFieldDefinition> fields = List.of(dataField, filterField);
    VectorIndexFieldMapping mapping = VectorIndexFieldMapping.create(fields, Optional.empty());
    Assert.assertEquals(mapping.getFieldDefinition(dataPath), Optional.of(dataField));
    Assert.assertEquals(mapping.getFieldDefinition(filterPath), Optional.of(filterField));
    Assert.assertFalse(mapping.subDocumentExists(filterPath));
  }

  @Test
  public void testMultipleFiltersSameParentPath() {
    FieldPath dataPath = FieldPath.parse("data");
    VectorDataFieldDefinition dataField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(dataPath)
            .numDimensions(1)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    FieldPath filterParent = FieldPath.parse("filter");
    FieldPath filterPath1 = FieldPath.parse("filter.date");
    VectorIndexFilterFieldDefinition filterField1 =
        VectorIndexFilterFieldDefinition.create(filterPath1);
    FieldPath filterPath2 = FieldPath.parse("filter.number");
    VectorIndexFilterFieldDefinition filterField2 =
        VectorIndexFilterFieldDefinition.create(filterPath2);
    List<VectorIndexFieldDefinition> fields = List.of(dataField, filterField1, filterField2);
    VectorIndexFieldMapping mapping = VectorIndexFieldMapping.create(fields, Optional.empty());
    Assert.assertEquals(mapping.getFieldDefinition(dataPath), Optional.of(dataField));
    Assert.assertEquals(mapping.getFieldDefinition(filterPath1), Optional.of(filterField1));
    Assert.assertEquals(mapping.getFieldDefinition(filterPath2), Optional.of(filterField2));
    Assert.assertTrue(mapping.subDocumentExists(filterParent));
    Assert.assertFalse(mapping.subDocumentExists(filterPath1));
    Assert.assertFalse(mapping.subDocumentExists(filterPath2));
  }

  @Test
  public void testTwoFieldsSamePath() {
    FieldPath path = FieldPath.parse("path");
    VectorDataFieldDefinition dataField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(path)
            .numDimensions(1)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    VectorIndexFilterFieldDefinition filterField = VectorIndexFilterFieldDefinition.create(path);
    List<VectorIndexFieldDefinition> fields = List.of(dataField, filterField);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> VectorIndexFieldMapping.create(fields, Optional.empty()));
  }

  @Test
  public void testPathIsBothDocumentAndField() {
    FieldPath dataPath = FieldPath.parse("data");
    VectorDataFieldDefinition dataField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(dataPath)
            .numDimensions(1)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    FieldPath filterParentPath = FieldPath.parse("filter");
    FieldPath filterPath = FieldPath.parse("filter.date");
    VectorIndexFilterFieldDefinition filterParentField =
        VectorIndexFilterFieldDefinition.create(filterParentPath);
    VectorIndexFilterFieldDefinition filterField =
        VectorIndexFilterFieldDefinition.create(filterPath);
    List<VectorIndexFieldDefinition> fields = List.of(dataField, filterParentField, filterField);
    VectorIndexFieldMapping mapping = VectorIndexFieldMapping.create(fields, Optional.empty());
    Assert.assertEquals(mapping.getFieldDefinition(dataPath), Optional.of(dataField));
    Assert.assertEquals(
        mapping.getFieldDefinition(filterParentPath), Optional.of(filterParentField));
    Assert.assertEquals(mapping.getFieldDefinition(filterPath), Optional.of(filterField));
    Assert.assertTrue(mapping.subDocumentExists(filterParentPath));
    Assert.assertFalse(mapping.subDocumentExists(filterPath));
  }

  // --- nestedRoot tests ---

  @Test
  public void testNestedRootSimpleCase() {
    FieldPath nestedRoot = FieldPath.parse("sections");
    FieldPath vectorPath = FieldPath.parse("sections.embedding");
    VectorDataFieldDefinition vectorField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(vectorPath)
            .numDimensions(512)
            .similarity(VectorSimilarity.COSINE)
            .quantization(VectorQuantization.NONE)
            .build();
    List<VectorIndexFieldDefinition> fields = List.of(vectorField);
    VectorIndexFieldMapping mapping =
        VectorIndexFieldMapping.create(fields, Optional.of(nestedRoot));

    Assert.assertTrue("Should have nestedRoot", mapping.hasNestedRoot());
    Assert.assertTrue(
        "sections should be the nested root", mapping.isNestedRoot(FieldPath.parse("sections")));
    Assert.assertFalse(
        "other path should not be nested root", mapping.isNestedRoot(FieldPath.parse("other")));
    Assert.assertEquals(mapping.getFieldDefinition(vectorPath), Optional.of(vectorField));
    Assert.assertTrue(mapping.subDocumentExists(nestedRoot));
    Assert.assertTrue(mapping.childPathExists(nestedRoot));
  }

  @Test
  public void testNestedRootMultipleVectors() {
    FieldPath nestedRoot = FieldPath.parse("sections");
    FieldPath vectorPath1 = FieldPath.parse("sections.embedding1");
    FieldPath vectorPath2 = FieldPath.parse("sections.embedding2");
    VectorDataFieldDefinition vectorField1 =
        VectorDataFieldDefinitionBuilder.builder()
            .path(vectorPath1)
            .numDimensions(256)
            .similarity(VectorSimilarity.COSINE)
            .quantization(VectorQuantization.NONE)
            .build();
    VectorDataFieldDefinition vectorField2 =
        VectorDataFieldDefinitionBuilder.builder()
            .path(vectorPath2)
            .numDimensions(512)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    List<VectorIndexFieldDefinition> fields = List.of(vectorField1, vectorField2);
    VectorIndexFieldMapping mapping =
        VectorIndexFieldMapping.create(fields, Optional.of(nestedRoot));

    Assert.assertTrue(mapping.hasNestedRoot());
    Assert.assertTrue(mapping.isNestedRoot(nestedRoot));
    Assert.assertEquals(mapping.getFieldDefinition(vectorPath1), Optional.of(vectorField1));
    Assert.assertEquals(mapping.getFieldDefinition(vectorPath2), Optional.of(vectorField2));
    Assert.assertTrue(mapping.subDocumentExists(nestedRoot));
    Assert.assertFalse(mapping.getFieldDefinition(nestedRoot).isPresent());
  }

  @Test
  public void testNestedRootMixedSimpleAndNestedVectorFields() {
    FieldPath nestedRoot = FieldPath.parse("sections");
    FieldPath simpleVectorPath = FieldPath.parse("topLevelVector");
    FieldPath nestedVectorPath = FieldPath.parse("sections.embedding");
    VectorDataFieldDefinition simpleVectorField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(simpleVectorPath)
            .numDimensions(128)
            .similarity(VectorSimilarity.COSINE)
            .quantization(VectorQuantization.NONE)
            .build();
    VectorDataFieldDefinition nestedVectorField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(nestedVectorPath)
            .numDimensions(512)
            .similarity(VectorSimilarity.COSINE)
            .quantization(VectorQuantization.NONE)
            .build();
    List<VectorIndexFieldDefinition> fields = List.of(simpleVectorField, nestedVectorField);
    VectorIndexFieldMapping mapping =
        VectorIndexFieldMapping.create(fields, Optional.of(nestedRoot));

    Assert.assertTrue(mapping.hasNestedRoot());
    Assert.assertTrue(mapping.isNestedRoot(nestedRoot));
    Assert.assertFalse(mapping.isNestedRoot(simpleVectorPath));
    Assert.assertEquals(mapping.getFieldDefinition(simpleVectorPath),
        Optional.of(simpleVectorField));
    Assert.assertEquals(mapping.getFieldDefinition(nestedVectorPath),
        Optional.of(nestedVectorField));
    Assert.assertTrue(mapping.childPathExists(simpleVectorPath));
    Assert.assertTrue(mapping.childPathExists(nestedRoot));
    Assert.assertTrue(mapping.childPathExists(nestedVectorPath));
  }

  @Test
  public void testNestedRootWithVectorAndFilterFields() {
    FieldPath nestedRoot = FieldPath.parse("sections");
    FieldPath vectorPath = FieldPath.parse("sections.embedding");
    FieldPath filterPathUnderNested = FieldPath.parse("sections.name");
    FieldPath filterPathAtRoot = FieldPath.parse("title");
    VectorDataFieldDefinition vectorField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(vectorPath)
            .numDimensions(512)
            .similarity(VectorSimilarity.COSINE)
            .quantization(VectorQuantization.NONE)
            .build();
    VectorIndexFilterFieldDefinition filterUnderNested =
        VectorIndexFilterFieldDefinition.create(filterPathUnderNested);
    VectorIndexFilterFieldDefinition filterAtRoot =
        VectorIndexFilterFieldDefinition.create(filterPathAtRoot);
    List<VectorIndexFieldDefinition> fields =
        List.of(vectorField, filterUnderNested, filterAtRoot);
    VectorIndexFieldMapping mapping =
        VectorIndexFieldMapping.create(fields, Optional.of(nestedRoot));

    Assert.assertTrue(mapping.hasNestedRoot());
    Assert.assertTrue(mapping.isNestedRoot(nestedRoot));
    Assert.assertEquals(mapping.getFieldDefinition(vectorPath), Optional.of(vectorField));
    Assert.assertEquals(mapping.getFieldDefinition(filterPathUnderNested),
        Optional.of(filterUnderNested));
    Assert.assertEquals(mapping.getFieldDefinition(filterPathAtRoot), Optional.of(filterAtRoot));
    Assert.assertTrue(mapping.subDocumentExists(nestedRoot));
  }

  @Test
  public void testNestedRootPresentButNoFieldUnderItFails() {
    // Invariant: when nestedRoot is present, at least one field path must be under it, so
    // subDocumentExists(nestedRoot) and childPathExists(nestedRoot) are true for downstream code.
    FieldPath nestedRoot = FieldPath.parse("sections");
    FieldPath topLevelOnly = FieldPath.parse("topLevelVector");
    VectorDataFieldDefinition topLevelField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(topLevelOnly)
            .numDimensions(128)
            .similarity(VectorSimilarity.COSINE)
            .quantization(VectorQuantization.NONE)
            .build();
    List<VectorIndexFieldDefinition> fields = List.of(topLevelField);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> VectorIndexFieldMapping.create(fields, Optional.of(nestedRoot)));
  }

  @Test
  public void testNoNestedRootWithNestedPaths() {
    // Fields under "sections" but no index-level nestedRoot: isNestedRoot should be false
    FieldPath sectionVectorPath = FieldPath.parse("sections.embedding");
    VectorDataFieldDefinition vectorField =
        VectorDataFieldDefinitionBuilder.builder()
            .path(sectionVectorPath)
            .numDimensions(512)
            .similarity(VectorSimilarity.COSINE)
            .quantization(VectorQuantization.NONE)
            .build();
    List<VectorIndexFieldDefinition> fields = List.of(vectorField);
    VectorIndexFieldMapping mapping = VectorIndexFieldMapping.create(fields, Optional.empty());

    Assert.assertFalse(mapping.hasNestedRoot());
    Assert.assertFalse(
        "sections is not the nested root when nestedRoot is empty",
        mapping.isNestedRoot(FieldPath.parse("sections")));
    Assert.assertEquals(mapping.getFieldDefinition(sectionVectorPath), Optional.of(vectorField));
    Assert.assertTrue(mapping.subDocumentExists(FieldPath.parse("sections")));
  }
}
