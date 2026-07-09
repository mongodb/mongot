package com.xgen.mongot.index.autoembedding;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.embedding.SearchIndexAutoEmbedFieldMapping;
import com.xgen.mongot.embedding.mongodb.MaterializedViewCollectionResolver;
import com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for {@link MaterializedViewIndexGeneration}, including detection of when two index
 * definitions differ only by Lucene-only parameters (e.g. hnswOptions, analyzers). When Lucene-only
 * params change, replaceGenerator updates the lease in place (persistLeaseForGeneration) and
 * preserves the resume token (doInitialSync=false). Other changes trigger full replace with
 * doInitialSync=true.
 */
public final class MaterializedViewIndexGenerationUtil {

  private static final Logger LOG =
      LoggerFactory.getLogger(MaterializedViewIndexGenerationUtil.class);

  /**
   * Wire keys dropped from {@link VectorAutoEmbedFieldDefinition#toBson()} before comparison. New
   * Lucene-only keys should be added here; any other new keys in {@code toBson()} participate
   * automatically.
   */
  private static final Set<String> AUTO_EMBED_BSON_KEYS_IGNORED_FOR_NON_LUCENE_COMPARE =
      Set.of(
          VectorFieldSpecification.Fields.SIMILARITY.getName(),
          VectorFieldSpecification.Fields.HNSW_OPTIONS.getName(),
          VectorFieldSpecification.Fields.INDEXING_ALGORITHM.getName());

  /**
   * True if the new definition could skip resync, e.g. differ only by Lucene params (hnswOptions,
   * numPartitions, indexFeatureVersion, analyzers). When false, resync is needed.
   */
  public static boolean skipInitialSync(IndexDefinition oldDef, IndexDefinition newDef) {
    return switch (oldDef) {
      case VectorIndexDefinition oldVector -> {
        if (!(newDef instanceof VectorIndexDefinition newVector)) {
          throw new IllegalArgumentException(
              "Cannot compare definitions of different types: "
                  + oldDef.getClass().getSimpleName()
                  + " vs "
                  + newDef.getClass().getSimpleName());
        }
        yield skipInitialSyncVector(oldVector, newVector);
      }
      case SearchIndexDefinition oldSearch -> {
        if (!(newDef instanceof SearchIndexDefinition newSearch)) {
          throw new IllegalArgumentException(
              "Cannot compare definitions of different types: "
                  + oldDef.getClass().getSimpleName()
                  + " vs "
                  + newDef.getClass().getSimpleName());
        }
        yield skipInitialSyncSearch(oldSearch, newSearch);
      }
    };
  }

  // ---- Vector index ----

  private static boolean skipInitialSyncVector(
      VectorIndexDefinition oldDef, VectorIndexDefinition newDef) {
    // Conditions that REQUIRE resync (return false):

    // 1. AUTO_EMBED fields changed (path, model, modality, dimensions). This should never happen
    // because such changes will create a new MV and not enter the replaceGenerator path where
    // method skipInitialSync is called. Returning false as a defensive move.
    if (autoEmbedFieldChanged(oldDef, newDef)) {
      return false;
    }

    // 2. Filter fields changed
    if (filterFieldsChanged(oldDef, newDef)) {
      return false;
    }

    // 3. Lucene-only params changed (indexingMethod, hnswOptions, similarity, etc.) → reuse
    if (vectorLuceneParamsChanged(oldDef, newDef)) {
      return true;
    }

    // 4. Same definition version (e.g. Lucene rebuild, fell off oplog). No MV schema change,
    // so we preserve the existing commit and resume steady-state replication.
    if (oldDef.getDefinitionVersion().equals(newDef.getDefinitionVersion())) {
      LOG.atInfo().log("Definition version did not change");
      return true;
    }

    // 5. Only definitionVersion differs → resync
    return false;
  }

  /**
   * Checks if only KNOWN Lucene parameters changed.
   *
   * <p>Per AUTO_EMBED field: {@link VectorAutoEmbedFieldDefinition#toBson()} with Lucene keys
   * removed (see {@link #AUTO_EMBED_BSON_KEYS_IGNORED_FOR_NON_LUCENE_COMPARE}), then {@link
   * BsonDocument#equals(Object)} on key-sorted copies. Top-level: numPartitions,
   * indexFeatureVersion.
   */
  private static boolean vectorLuceneParamsChanged(
      VectorIndexDefinition oldDef, VectorIndexDefinition newDef) {
    var oldAutoEmbedFields =
        oldDef.getFields().stream()
            .filter(f -> f.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED)
            .sorted(Comparator.comparing(f -> f.getPath().toString()))
            .toList();
    var newAutoEmbedFields =
        newDef.getFields().stream()
            .filter(f -> f.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED)
            .sorted(Comparator.comparing(f -> f.getPath().toString()))
            .toList();

    // Fields must have same count
    if (oldAutoEmbedFields.size() != newAutoEmbedFields.size()) {
      return false;
    }

    // Compare each pair with Lucene fields (similarity, indexingAlgorithm) ignored: if the
    // remainder differs, not a pure Lucene bump. If remainders match but full fields differ, only
    // Lucene params changed for that pair.
    @Var boolean autoEmbedLuceneParamChange = false;
    for (int i = 0; i < oldAutoEmbedFields.size(); i++) {
      var oldAutoEmbedField = oldAutoEmbedFields.get(i);
      var newAutoEmbedField = newAutoEmbedFields.get(i);
      if (!autoEmbedFieldsEqualIgnoringLuceneParams(oldAutoEmbedField, newAutoEmbedField)) {
        LOG.atInfo()
            .addKeyValue("oldAutoEmbedField", oldAutoEmbedField)
            .addKeyValue("newAutoEmbedField", newAutoEmbedField)
            .log("AUTO_EMBED field differs outside Lucene params");
        return false;
      }
      if (!oldAutoEmbedField.equals(newAutoEmbedField)) {
        autoEmbedLuceneParamChange = true;
      }
    }

    if (autoEmbedLuceneParamChange) {
      LOG.atInfo().log("At least one AUTO_EMBED field differs by Lucene params");
      return true;
    }

    // Top-level Lucene params
    // TODO(CLOUDP-391893): assess other top-level VectorIndexDefinition field changes
    if (oldDef.getNumPartitions() != newDef.getNumPartitions()) {
      LOG.atInfo()
          .addKeyValue("oldNumPartitions", oldDef.getNumPartitions())
          .addKeyValue("newNumPartitions", newDef.getNumPartitions())
          .log("numPartitions changed");
      return true;
    }
    if (oldDef.getParsedIndexFeatureVersion() != newDef.getParsedIndexFeatureVersion()) {
      LOG.atInfo()
          .addKeyValue("oldIndexFeatureVersion", oldDef.getParsedIndexFeatureVersion())
          .addKeyValue("newIndexFeatureVersion", newDef.getParsedIndexFeatureVersion())
          .log("indexFeatureVersion changed");
      return true;
    }

    return false;
  }

  /**
   * Path match and same auto-embed field shape in BSON after removing Lucene-only keys ({@link
   * BsonDocument#equals(Object)} on key-sorted documents so order does not matter).
   */
  private static boolean autoEmbedFieldsEqualIgnoringLuceneParams(
      VectorIndexFieldDefinition oldField, VectorIndexFieldDefinition newField) {
    var oldAutoEmbed = (VectorAutoEmbedFieldDefinition) oldField;
    var newAutoEmbed = (VectorAutoEmbedFieldDefinition) newField;
    if (!oldAutoEmbed.getPath().equals(newAutoEmbed.getPath())) {
      return false;
    }
    return nonLuceneAutoEmbedFieldBsonNormalized(oldAutoEmbed)
        .equals(nonLuceneAutoEmbedFieldBsonNormalized(newAutoEmbed));
  }

  private static BsonDocument nonLuceneAutoEmbedFieldBsonNormalized(
      VectorAutoEmbedFieldDefinition f) {
    BsonDocument raw = f.toBson();
    BsonDocument sansLucene = new BsonDocument();
    for (Map.Entry<String, BsonValue> e : raw.entrySet()) {
      if (!AUTO_EMBED_BSON_KEYS_IGNORED_FOR_NON_LUCENE_COMPARE.contains(e.getKey())) {
        sansLucene.append(e.getKey(), e.getValue());
      }
    }
    TreeMap<String, BsonValue> sorted = new TreeMap<>();
    for (String k : sansLucene.keySet()) {
      sorted.put(k, sansLucene.get(k));
    }
    BsonDocument ordered = new BsonDocument();
    sorted.forEach(ordered::append);
    return ordered;
  }

  /** Filter fields have changed (order-independent comparison). */
  private static boolean filterFieldsChanged(
      VectorIndexDefinition oldDef, VectorIndexDefinition newDef) {
    var oldFilterFields =
        oldDef.getFields().stream()
            .filter(f -> f.getType() == VectorIndexFieldDefinition.Type.FILTER)
            .sorted(Comparator.comparing(f -> f.getPath().toString()))
            .toList();
    var newFilterFields =
        newDef.getFields().stream()
            .filter(f -> f.getType() == VectorIndexFieldDefinition.Type.FILTER)
            .sorted(Comparator.comparing(f -> f.getPath().toString()))
            .toList();
    if (!oldFilterFields.equals(newFilterFields)) {
      LOG.atInfo().log("Filter fields changed");
      return true;
    }
    return false;
  }

  /** AUTO_EMBED field hash changed (path, model, numDimensions, quantization). */
  private static boolean autoEmbedFieldChanged(
      VectorIndexDefinition oldDef, VectorIndexDefinition newDef) {
    String oldHash = MaterializedViewCollectionResolver.computeHash(oldDef);
    String newHash = MaterializedViewCollectionResolver.computeHash(newDef);
    if (!oldHash.equals(newHash)) {
      LOG.atError()
          .log("AUTO_EMBED fields changed, which should NOT happen in replaceGenerator path");
      return true;
    }
    return false;
  }

  // ---- Search index ----

  private static boolean skipInitialSyncSearch(
      SearchIndexDefinition oldDef, SearchIndexDefinition newDef) {
    // Resync (re-embed) is needed only when the materialized-view content changes. The MV
    // holds the auto-embed fields' embeddings plus the passed-through values of the
    // non-auto-embed fields, which is exactly what the AutoEmbedFieldMapping captures
    // (dynamic mode, embed fields with their specs, and passthrough paths). Any other
    // difference -- analyzers, synonyms, typeSets, storedSource, sort, numPartitions,
    // indexFeatureVersion -- leaves the embeddings valid, so the generation is reused and
    // the Lucene index is rebuilt over the preserved MV data.
    // The comparison is by MV content, not strict equality: in dynamic mode the explicit
    // passthrough paths don't affect what reaches the MV.

    // The MV materializes the view's output, so a view change invalidates every materialized
    // document regardless of the mappings.
    if (!oldDef.getView().equals(newDef.getView())) {
      LOG.atInfo()
          .addKeyValue("indexId", newDef.getIndexId())
          .addKeyValue("oldDefinitionVersion", oldDef.getDefinitionVersion())
          .addKeyValue("newDefinitionVersion", newDef.getDefinitionVersion())
          .log("Source view changed; resync required");
      return false;
    }

    SearchIndexAutoEmbedFieldMapping oldMapping =
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(oldDef);
    SearchIndexAutoEmbedFieldMapping newMapping =
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(newDef);
    if (!oldMapping.producesSameMatViewContent(newMapping)) {
      LOG.atInfo()
          .addKeyValue("indexId", newDef.getIndexId())
          .addKeyValue("oldDefinitionVersion", oldDef.getDefinitionVersion())
          .addKeyValue("newDefinitionVersion", newDef.getDefinitionVersion())
          .log("Search materialized-view content changed; resync required");
      return false;
    }

    // A bump of definitionVersion with no other change forces a resync: the operator escape
    // hatch for recovering from transient errors, mirroring the vector arm.
    if (!oldDef.getDefinitionVersion().equals(newDef.getDefinitionVersion())
        && oldDef.equalsIgnoringDefinitionVersion(newDef)) {
      LOG.atInfo()
          .addKeyValue("indexId", newDef.getIndexId())
          .addKeyValue("oldDefinitionVersion", oldDef.getDefinitionVersion())
          .addKeyValue("newDefinitionVersion", newDef.getDefinitionVersion())
          .log("Only definitionVersion changed; resync forced");
      return false;
    }
    return true;
  }
}
