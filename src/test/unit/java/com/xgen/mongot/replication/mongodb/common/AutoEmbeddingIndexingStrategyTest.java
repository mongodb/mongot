package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration.MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests verifying that the auto-embedding indexing strategy is correctly determined based on
 * the field types in the index definition.
 *
 * <p>Strategy determination:
 *
 * <ul>
 *   <li>TEXT fields (parsedAutoEmbeddingFeatureVersion = 1) → EMBEDDING strategy (old path)
 *   <li>AUTO_EMBED fields (parsedAutoEmbeddingFeatureVersion = 2) → EMBEDDING_MATERIALIZED_VIEW
 *       strategy (new path)
 * </ul>
 *
 * <p>Filter-only updates should ONLY be created for EMBEDDING_MATERIALIZED_VIEW strategy because:
 *
 * <ul>
 *   <li>Materialized view path supports partial updates via MongoDB $set
 *   <li>Old EMBEDDING path writes to Lucene directly, which doesn't support partial updates
 *   <li>Creating filter-only updates for EMBEDDING causes Lucene to replace documents WITHOUT
 *       vectors (data corruption)
 * </ul>
 */
@RunWith(JUnit4.class)
public class AutoEmbeddingIndexingStrategyTest {

  @Test
  public void testTextFieldType_UsesEmbeddingStrategy() {
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withTextField("description")
            .withFilterPath("category")
            .build();

    assertEquals(1, indexDef.getParsedAutoEmbeddingFeatureVersion());
    assertFalse(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));
    assertTrue(
        indexDef.getParsedAutoEmbeddingFeatureVersion()
            < MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING);
  }

  @Test
  public void testAutoEmbedFieldType_UsesMaterializedViewStrategy() {
    VectorIndexDefinition indexDef =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();

    assertEquals(2, indexDef.getParsedAutoEmbeddingFeatureVersion());
    assertTrue(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(indexDef));
    assertTrue(
        indexDef.getParsedAutoEmbeddingFeatureVersion()
            >= MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING);
  }

  @Test
  public void testStrategySelection_VersionBoundary() {
    // TEXT field → version 1 → below threshold → EMBEDDING strategy
    VectorIndexDefinition textIndex =
        VectorIndexDefinitionBuilder.builder().withTextField("text").build();

    assertEquals(1, textIndex.getParsedAutoEmbeddingFeatureVersion());
    assertFalse(MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(textIndex));

    // AUTO_EMBED field → version 2 → meets threshold → EMBEDDING_MATERIALIZED_VIEW strategy
    VectorIndexDefinition autoEmbedIndex =
        VectorIndexDefinitionBuilder.builder().withAutoEmbedField("text").build();

    assertEquals(2, autoEmbedIndex.getParsedAutoEmbeddingFeatureVersion());
    assertTrue(
        MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(autoEmbedIndex));
  }

  @Test
  public void testFilterOnlyUpdates_OnlyForMaterializedViewIndexes() {
    // Old path (TEXT field, version 1) - filter-only updates NOT applicable
    VectorIndexDefinition oldPathIndex =
        VectorIndexDefinitionBuilder.builder()
            .withTextField("description")
            .withFilterPath("category")
            .build();

    assertFalse(
        MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(oldPathIndex));

    // New path (AUTO_EMBED field, version 2) - filter-only updates ARE applicable
    VectorIndexDefinition newPathIndex =
        VectorIndexDefinitionBuilder.builder()
            .withAutoEmbedField("description")
            .withFilterPath("category")
            .build();

    assertTrue(
        MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(newPathIndex));
  }

  @Test
  public void testNonAutoEmbeddingIndex_VersionZero() {
    VectorIndexDefinition nonAutoEmbedIndex =
        VectorIndexDefinitionBuilder.builder()
            .withCosineVectorField("embedding", 1024)
            .withFilterPath("category")
            .build();

    assertEquals(0, nonAutoEmbedIndex.getParsedAutoEmbeddingFeatureVersion());
    assertFalse(nonAutoEmbedIndex.isAutoEmbeddingIndex());
    assertFalse(
        MaterializedViewIndexDefinitionGeneration.isMaterializedViewBasedIndex(nonAutoEmbedIndex));
  }
}
