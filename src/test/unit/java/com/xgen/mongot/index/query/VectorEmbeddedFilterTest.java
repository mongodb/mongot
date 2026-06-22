package com.xgen.mongot.index.query;

import static com.xgen.mongot.index.definition.VectorSimilarity.DOT_PRODUCT;

import com.xgen.mongot.index.definition.VectorDataFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldDefinitionResolver;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.VectorDataFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for nested vector filter validation. This test verifies that filter field validation works
 * correctly with nested vectors (using nestedRoot at index level).
 */
public class VectorEmbeddedFilterTest {

  @Test
  public void testNestedVectorFilterValidation() {
    // Create index definition matching the user's scenario:
    // - root_vector (non-nested)
    // - sections.section_vector (nested with nestedRoot="sections" at index level)
    // - name (filter field)
    // - sections.section_name (filter field)

    var definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("test_index")
            .database("test_db")
            .lastObservedCollectionName("test_collection")
            .collectionUuid(UUID.randomUUID())
            .numPartitions(1)
            .nestedRoot("sections")
            .setFields(
                List.of(
                    // Root vector field (non-nested)
                    VectorDataFieldDefinitionBuilder.builder()
                        .path(FieldPath.parse("root_vector"))
                        .numDimensions(5)
                        .similarity(DOT_PRODUCT)
                        .quantization(VectorQuantization.NONE)
                        .build(),
                    // Nested vector field (path is a child of nestedRoot)
                    new VectorDataFieldDefinition(
                        FieldPath.parse("sections.section_vector"),
                        new VectorFieldSpecification(
                            5,
                            DOT_PRODUCT,
                            VectorQuantization.NONE,
                            new VectorIndexingAlgorithm.HnswIndexingAlgorithm())),
                    // Filter fields
                    VectorIndexFilterFieldDefinition.create(FieldPath.parse("name")),
                    VectorIndexFilterFieldDefinition.create(
                        FieldPath.parse("sections.section_name"))))
            .build();

    var resolver = new VectorFieldDefinitionResolver(definition, IndexFormatVersion.CURRENT);
    var checks = new VectorQueryTimeMappingChecks(resolver);

    // Test 1: Validate root vector field (non-embedded)
    try {
      checks.validateVectorField(FieldPath.parse("root_vector"), Optional.empty(), 5);
      // Should succeed
    } catch (InvalidQueryException e) {
      Assert.fail("Root vector validation should succeed: " + e.getMessage());
    }

    // Test 2: Validate embedded vector field
    try {
      checks.validateVectorField(
          FieldPath.parse("sections.section_vector"), Optional.of(FieldPath.parse("sections")), 5);
      // Should succeed
    } catch (InvalidQueryException e) {
      Assert.fail("Embedded vector validation should succeed: " + e.getMessage());
    }

    // Test 3: Validate root filter field (no embedded context)
    Assert.assertTrue(
        "Root filter field 'name' should be indexed",
        checks.indexedAsToken(FieldPath.parse("name"), Optional.empty()));

    // Test 4: Validate embedded filter field with embedded context
    // When querying with embeddedRoot="sections", the filter path must still be the full path
    // "sections.section_name" as defined in the index, not the relative path "section_name"
    Assert.assertTrue(
        "Embedded filter field 'sections.section_name' should be indexed "
            + "when embeddedRoot='sections'",
        checks.indexedAsToken(
            FieldPath.parse("sections.section_name"), Optional.of(FieldPath.parse("sections"))));

    // Test 5: Validate that relative path does NOT work with embedded context
    Assert.assertFalse(
        "Relative path 'section_name' should NOT be indexed when embeddedRoot='sections'",
        checks.indexedAsToken(
            FieldPath.parse("section_name"), Optional.of(FieldPath.parse("sections"))));

    // Test 6: Validate that full path also works without embedded context
    Assert.assertTrue(
        "Full path 'sections.section_name' should be indexed without embedded context",
        checks.indexedAsToken(FieldPath.parse("sections.section_name"), Optional.empty()));
  }

  @Test
  public void testNestedVectorFieldSpecification() {
    // Test that getVectorFieldSpecification works for nested vectors
    var definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("test_index")
            .database("test_db")
            .lastObservedCollectionName("test_collection")
            .collectionUuid(UUID.randomUUID())
            .numPartitions(1)
            .nestedRoot("sections")
            .setFields(
                List.of(
                    new VectorDataFieldDefinition(
                        FieldPath.parse("sections.section_vector"),
                        new VectorFieldSpecification(
                            5,
                            DOT_PRODUCT,
                            VectorQuantization.NONE,
                            new VectorIndexingAlgorithm.HnswIndexingAlgorithm()))))
            .build();

    var resolver = new VectorFieldDefinitionResolver(definition, IndexFormatVersion.CURRENT);

    // Should find the vector field specification
    Optional<VectorFieldSpecification> spec =
        resolver.getVectorFieldSpecification(FieldPath.parse("sections.section_vector"));
    Assert.assertTrue("Vector field specification should be found", spec.isPresent());
    Assert.assertEquals("Dimensions should match", 5, spec.get().numDimensions());
    Assert.assertEquals("Similarity should match", DOT_PRODUCT, spec.get().similarity());
  }

  @Test
  public void testNonEmbeddedFilterValidation() {
    // Test that non-embedded filter validation still works
    var definition =
        VectorIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("test_index")
            .database("test_db")
            .lastObservedCollectionName("test_collection")
            .collectionUuid(UUID.randomUUID())
            .numPartitions(1)
            .setFields(
                List.of(
                    VectorDataFieldDefinitionBuilder.builder()
                        .path(FieldPath.parse("vector"))
                        .numDimensions(5)
                        .similarity(DOT_PRODUCT)
                        .quantization(VectorQuantization.NONE)
                        .build(),
                    VectorIndexFilterFieldDefinition.create(FieldPath.parse("name")),
                    VectorIndexFilterFieldDefinition.create(FieldPath.parse("nested.field"))))
            .build();

    var resolver = new VectorFieldDefinitionResolver(definition, IndexFormatVersion.CURRENT);
    var checks = new VectorQueryTimeMappingChecks(resolver);

    // Test filter validation without embedded context
    Assert.assertTrue(
        "Filter field 'name' should be indexed",
        checks.indexedAsToken(FieldPath.parse("name"), Optional.empty()));

    Assert.assertTrue(
        "Filter field 'nested.field' should be indexed",
        checks.indexedAsToken(FieldPath.parse("nested.field"), Optional.empty()));

    Assert.assertFalse(
        "Non-indexed field should return false",
        checks.indexedAsToken(FieldPath.parse("nonexistent"), Optional.empty()));
  }
}
