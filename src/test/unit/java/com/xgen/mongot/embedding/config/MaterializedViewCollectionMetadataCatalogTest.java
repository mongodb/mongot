package com.xgen.mongot.embedding.config;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.MaterializedViewGeneration;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import java.util.Map;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link MaterializedViewCollectionMetadataCatalog}. */
public class MaterializedViewCollectionMetadataCatalogTest {

  private MaterializedViewCollectionMetadataCatalog catalog;

  @Before
  public void setUp() {
    this.catalog = new MaterializedViewCollectionMetadataCatalog();
  }

  /**
   * Confirms the canonical key approach: add with one type (MaterializedViewGenerationId, as the
   * resolver does) and get with the other (GenerationId, as the lifecycle manager does) both
   * resolve to the same entry.
   */
  @Test
  public void addWithMaterializedViewGenerationId_getWithGenerationId_returnsSameMetadata() {
    ObjectId indexId = new ObjectId();
    Generation generation = Generation.CURRENT;
    MaterializedViewGenerationId matViewId =
        new MaterializedViewGenerationId(indexId, new MaterializedViewGeneration(generation));
    GenerationId rawId = new GenerationId(indexId, generation);

    MaterializedViewCollectionMetadata metadata =
        new MaterializedViewCollectionMetadata(
            new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of()),
            UUID.randomUUID(),
            "testColl");

    this.catalog.addMetadata(matViewId, metadata);

    MaterializedViewCollectionMetadata found = this.catalog.getMetadata(rawId);
    assertThat(found).isNotNull();
    assertThat(found.collectionUuid()).isEqualTo(metadata.collectionUuid());
    assertThat(found.collectionName()).isEqualTo(metadata.collectionName());
  }

  /**
   * Same in reverse: add with GenerationId, get with MaterializedViewGenerationId. Both types
   * produce the same canonical key.
   */
  @Test
  public void addWithGenerationId_getWithMaterializedViewGenerationId_returnsSameMetadata() {
    ObjectId indexId = new ObjectId();
    Generation generation = Generation.CURRENT;
    GenerationId rawId = new GenerationId(indexId, generation);
    MaterializedViewGenerationId matViewId =
        new MaterializedViewGenerationId(indexId, new MaterializedViewGeneration(generation));

    MaterializedViewCollectionMetadata metadata =
        new MaterializedViewCollectionMetadata(
            new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of()),
            UUID.randomUUID(),
            "testColl");

    this.catalog.addMetadata(rawId, metadata);

    MaterializedViewCollectionMetadata found = this.catalog.getMetadata(matViewId);
    assertThat(found).isNotNull();
    assertThat(found.collectionUuid()).isEqualTo(metadata.collectionUuid());
    assertThat(found.collectionName()).isEqualTo(metadata.collectionName());
  }

  /**
   * removeMetadata with the other type still removes the entry (e.g. dropIndex passes
   * GenerationId).
   */
  @Test
  public void addWithMaterializedViewGenerationId_removeWithGenerationId_removesEntry() {
    ObjectId indexId = new ObjectId();
    Generation generation = Generation.CURRENT;
    MaterializedViewGenerationId matViewId =
        new MaterializedViewGenerationId(indexId, new MaterializedViewGeneration(generation));
    GenerationId rawId = new GenerationId(indexId, generation);

    MaterializedViewCollectionMetadata metadata =
        new MaterializedViewCollectionMetadata(
            new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of()),
            UUID.randomUUID(),
            "testColl");

    this.catalog.addMetadata(matViewId, metadata);
    assertThat(this.catalog.getMetadata(rawId)).isNotNull();

    this.catalog.removeMetadata(rawId);
    assertThrows(IllegalStateException.class, () -> this.catalog.getMetadata(matViewId));
    assertThat(this.catalog.getMetadataIfPresent(rawId)).isEmpty();
  }
}
