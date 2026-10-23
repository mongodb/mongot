package com.xgen.testing.mongot.index.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import java.util.Set;
import org.junit.Test;

/**
 * Pins the snapshot/restore contract used by tests that mutate the process-wide model catalog:
 * {@link EmbeddingModelCatalog#clear()} drops the compatible query models too, so a restore that
 * only re-registers model configs would leave the rest of the JVM fork without its query-model
 * compatibility.
 */
public class EmbeddingModelCatalogTestUtilsTest {

  @Test
  public void restoreCatalogRestoresCompatibleQueryModels() {
    var originalCatalog = EmbeddingModelCatalogTestUtils.snapshotCatalog();
    try {
      EmbeddingModelCatalogTestUtils.registerAutoEmbedModel("snapshot-round-trip-index-model");
      EmbeddingModelCatalog.registerCompatibleModels(
          "snapshot-round-trip-index-model", Set.of("snapshot-round-trip-query-model"));

      var mutatedCatalog = EmbeddingModelCatalogTestUtils.snapshotCatalog();
      EmbeddingModelCatalog.clear();
      assertFalse(EmbeddingModelCatalog.isQueryModelAllowed("snapshot-round-trip-query-model"));

      EmbeddingModelCatalogTestUtils.restoreCatalog(mutatedCatalog);

      assertTrue(EmbeddingModelCatalog.isModelRegistered("snapshot-round-trip-index-model"));
      assertTrue(EmbeddingModelCatalog.isQueryModelAllowed("snapshot-round-trip-query-model"));
      assertEquals(
          Set.of("snapshot-round-trip-index-model"),
          EmbeddingModelCatalog.getCompatibleIndexModels("snapshot-round-trip-query-model"));
    } finally {
      EmbeddingModelCatalogTestUtils.restoreCatalog(originalCatalog);
    }
  }
}
