package com.xgen.mongot.embedding.config;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.index.version.GenerationId;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory cache for MaterializedViewCollectionMetadata, keyed by GenerationId. This cache is
 * populated at index creation time and used for both indexing and query. The methods can take in
 * either a GenerationId or a MaterializedViewGenerationId that belongs to the same index
 * generation.
 */
public class MaterializedViewCollectionMetadataCatalog {

  private final Map<String, MaterializedViewCollectionMetadata> metadataMap;

  public MaterializedViewCollectionMetadataCatalog() {
    this.metadataMap = new ConcurrentHashMap<>();
  }

  public void addMetadata(GenerationId generationId, MaterializedViewCollectionMetadata metadata) {
    this.metadataMap.put(canonicalKey(generationId), metadata);
  }

  public MaterializedViewCollectionMetadata getMetadata(GenerationId generationId) {
    checkState(
        this.metadataMap.containsKey(canonicalKey(generationId)),
        "Mat view metadata not found for generationId: %s. This likely indicates a bug.",
        generationId);
    return this.metadataMap.get(canonicalKey(generationId));
  }

  public Optional<MaterializedViewCollectionMetadata> getMetadataIfPresent(
      GenerationId generationId) {
    return Optional.ofNullable(this.metadataMap.get(canonicalKey(generationId)));
  }

  public void removeMetadata(GenerationId generationId) {
    this.metadataMap.remove(canonicalKey(generationId));
  }

  /**
   * Canonical key for mat view metadata: indexId + userVersion (should be same for both
   * GenerationId and MaterializedViewGenerationId types).
   */
  private static String canonicalKey(GenerationId generationId) {
    return generationId.indexId.toHexString()
        + "-u"
        + generationId.generation.userIndexVersion.versionNumber;
  }
}
