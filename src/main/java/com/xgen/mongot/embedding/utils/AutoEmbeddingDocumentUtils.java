package com.xgen.mongot.embedding.utils;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import static com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator.getHashFieldPath;
import static com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator.getMatViewFieldPath;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.errorprone.annotations.Var;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping.AutoEmbedField;
import com.xgen.mongot.embedding.SearchIndexAutoEmbedFieldMapping;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.ingestion.BsonDocumentProcessor;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.BsonVectorParser;
import com.xgen.mongot.util.bson.Vector;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.collections4.map.CompositeMap;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoEmbeddingDocumentUtils {

  public static final String HASH_FIELD_SUFFIX = "_hash";
  private static final Logger LOG = LoggerFactory.getLogger(AutoEmbeddingDocumentUtils.class);

  // Mirrors {@code MaterializedViewCollectionResolver.MV_COLLECTION_SCHEMA_NAMESPACE}. Inlined to
  // avoid a utils → mongodb package dependency.
  private static final String MV_NAMESPACE = "_autoEmbed";

  /**
   * Metadata fields present in materialized view documents that are not part of the index field
   * mappings. These fields must be excluded when comparing MV documents to source documents to
   * avoid spurious re-indexing. When adding a new metadata field to the MV document (e.g., in
   * {@link com.xgen.mongot.index.mongodb.MaterializedViewWriter}), it must also be added here.
   */
  // Package-private so tests within the same package can validate it. The cross-package enforcer
  // test in MaterializedViewWriterTest validates it via the full writer → compareDocuments path.
  static final ImmutableSet<String> MV_METADATA_FIELDS =
      ImmutableSet.of("_id", "_autoEmbed._leaseVersion");

  /**
   * Extracts string field values from rawBsonDocument based on VectorIndexFieldMappings (from index
   * definition), only processes string field paths defined in VectorTextFieldDefinition. Text
   * strings are mapped by field path to support multi-model index. Returns a FieldPath to Strings
   * mapping for auto-embedding calls.
   */
  public static ImmutableMap<FieldPath, Set<String>> getVectorTextPathMap(
      RawBsonDocument rawBsonDocument, AutoEmbedFieldMapping fieldMapping) throws IOException {
    CollectVectorTextStringsDocumentHandler handler =
        CollectVectorTextStringsDocumentHandler.create(fieldMapping, Optional.empty());
    BsonDocumentProcessor.process(rawBsonDocument, handler);
    return handler.getVectorTextPathMap();
  }

  /**
   * Constructs a new DocumentEvent by adding per document embeddings from a large embeddings
   * inputs. Only embeddings with matched field path and string text value will be added into
   * DocumentEvent.
   */
  public static DocumentEvent buildAutoEmbeddingDocumentEvent(
      DocumentEvent rawDocumentEvent,
      AutoEmbedFieldMapping fieldMapping,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> allVectorsFromBatchResponse)
      throws IOException {
    if (rawDocumentEvent.getDocument().isEmpty()) {
      return rawDocumentEvent;
    }
    ImmutableMap<FieldPath, Set<String>> textPerFieldInDoc =
        getVectorTextPathMap(rawDocumentEvent.getDocument().get(), fieldMapping);
    // Output vector map builder to load filtered vectors from allVectorsFromBatchRequest
    ImmutableMap.Builder<FieldPath, ImmutableMap<String, Vector>> filteredVectorMapBuilder =
        ImmutableMap.builder();
    for (var fieldAndTexts : textPerFieldInDoc.entrySet()) {
      if (!allVectorsFromBatchResponse.containsKey(fieldAndTexts.getKey())) {
        continue;
      }
      ImmutableMap<String, Vector> sourceEmbeddings =
          allVectorsFromBatchResponse.get(fieldAndTexts.getKey());
      ImmutableMap.Builder<String, Vector> filteredEmbeddingBuilder = ImmutableMap.builder();
      for (String text : fieldAndTexts.getValue()) {
        if (sourceEmbeddings.containsKey(text)) {
          filteredEmbeddingBuilder.put(text, sourceEmbeddings.get(text));
        }
        // missing embedding won't be handled here, but will have
        // LuceneVectorIndexFieldValueHandler::handleString to skip building those auto-embedding
        // text field in DocumentBuilder
      }
      filteredVectorMapBuilder.put(fieldAndTexts.getKey(), filteredEmbeddingBuilder.build());
    }
    return DocumentEvent.createFromDocumentEventAndVectors(
        rawDocumentEvent, filteredVectorMapBuilder.build());
  }

  /**
   * Constructs a new document by cloning filter fields and stored source fields from original
   * document, skips auto embedding fields which will be added separately.
   */
  public static DocumentEvent buildMaterializedViewDocumentEvent(
      DocumentEvent rawDocumentEvent,
      AutoEmbedFieldMapping fieldMapping,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddingsPerField,
      MaterializedViewSchemaMetadata schemaMetadata)
      throws IOException {
    return buildMaterializedViewDocumentEvent(
        rawDocumentEvent, fieldMapping, embeddingsPerField, schemaMetadata, Optional.empty());
  }

  /** Builds the auto-embed MV doc: per-element children when nested, else per-field. */
  public static DocumentEvent buildMaterializedViewDocumentEvent(
      DocumentEvent rawDocumentEvent,
      AutoEmbedFieldMapping fieldMapping,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddingsPerField,
      MaterializedViewSchemaMetadata schemaMetadata,
      Optional<FieldPath> sourceNestedRoot)
      throws IOException {
    if (rawDocumentEvent.getDocument().isEmpty()) {
      return rawDocumentEvent;
    }
    Map<FieldPath, Map<String, Vector>> consolidatedEmbeddingMap = new HashMap<>();
    for (FieldPath fieldPath :
        Sets.union(embeddingsPerField.keySet(), rawDocumentEvent.getAutoEmbeddings().keySet())) {
      var newEmbeddings = embeddingsPerField.getOrDefault(fieldPath, ImmutableMap.of());
      var oldEmbeddings =
          rawDocumentEvent
              .getAutoEmbeddings()
              .getOrDefault(fieldPath, ImmutableMap.of())
              .entrySet()
              .stream()
              .filter(entry -> !newEmbeddings.containsKey(entry.getKey()))
              .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
      if (newEmbeddings.isEmpty() && oldEmbeddings.isEmpty()) {
        continue;
      }
      consolidatedEmbeddingMap.put(fieldPath, new CompositeMap<>(newEmbeddings, oldEmbeddings));
    }
    BsonDocument bsonDoc = new BsonDocument();
    MaterializedViewDocumentHandler handler =
        MaterializedViewDocumentHandler.create(fieldMapping, Optional.empty(), bsonDoc);
    BsonDocumentProcessor.process(rawDocumentEvent.getDocument().get(), handler);
    // buildEmbeddingsPerMatViewPath excludes nestedRoot paths when present (returns only the mixed
    // non-nested embeds for a nested index), else returns all embeds.
    ImmutableMap<FieldPath, BsonValue> embeddingsPerMatViewPath =
        buildEmbeddingsPerMatViewPath(
            rawDocumentEvent.getDocument().get(),
            fieldMapping,
            consolidatedEmbeddingMap,
            schemaMetadata,
            sourceNestedRoot);
    for (var entry : embeddingsPerMatViewPath.entrySet()) {
      addBsonValueToBsonDocument(bsonDoc, entry.getKey(), entry.getValue());
    }
    if (sourceNestedRoot.isPresent()) {
      // Nested auto-embed: one MV child per source array element (vector + co-located filters) plus
      // a parallel per-element hash array.
      addNestedAutoEmbedArrays(
          bsonDoc,
          rawDocumentEvent.getDocument().get(),
          fieldMapping,
          consolidatedEmbeddingMap,
          schemaMetadata,
          sourceNestedRoot.get());
    } else {
      // CLOUDP-402522: co-locate filters whose source path is a strict descendant of the source
      // nestedRoot under the MV nestedRoot; a no-op for top-level filters (no schema entry).
      ImmutableMap<FieldPath, BsonValue> remappedFilterValuesPerMatViewPath =
          buildRemappedFilterValuesPerMatViewPath(
              rawDocumentEvent.getDocument().get(),
              fieldMapping,
              consolidatedEmbeddingMap,
              schemaMetadata);
      for (var filterEntry : remappedFilterValuesPerMatViewPath.entrySet()) {
        addBsonValueToBsonDocument(bsonDoc, filterEntry.getKey(), filterEntry.getValue());
      }
    }

    stripIdForMaterializedViewWrite(bsonDoc);

    return DocumentEvent.createFromDocumentEvent(
        rawDocumentEvent, new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC));
  }

  /**
   * Removes the top-level {@code _id} field from a materialized-view-shaped BSON document before it
   * is handed to {@link com.xgen.mongot.index.mongodb.MaterializedViewWriter}.
   *
   * <p>The MV row's {@code _id} is owned by the upsert filter in {@code MaterializedViewWriter}
   * ({@code {_id: event.getDocumentId()}}), which carries the full source {@code _id} value —
   * including any subdocument structure. Letting an MV-bound document carry its own {@code _id} is
   * unsafe whenever the index declares a filter on a subfield of {@code _id} (e.g. {@code
   * _id.ProfileId}): {@link MaterializedViewDocumentHandler} treats {@code _id} as a passthrough
   * ancestor and copies only the declared subfield, dropping siblings. The resulting write then
   * either targets the immutable {@code _id} field directly (filter-only {@code $set} path,
   * CLOUDP-406702) or presents a truncated {@code _id} that no longer matches the upsert filter
   * (full-replace / initial-sync path, CLOUDP-412237). Both are non-retryable errors that FAIL the
   * index.
   *
   * <p>Call this on every MV-shaped document produced by this utility before it leaves the
   * embedding pipeline.
   */
  static void stripIdForMaterializedViewWrite(BsonDocument bsonDoc) {
    bsonDoc.remove("_id");
  }

  // Collects filter-field values whose source path is a strict descendant of the source nestedRoot,
  // keyed by MV path (sections.section_name -> _autoEmbed.sections.section_name). Value comes from
  // the same child element as the embedded vector (else nested $vectorSearch colocates unrelated
  // elements), mirroring buildEmbeddingsPerMatViewPath's first-non-empty-with-vector choice.
  // No-op for search indexes: their schema mapping holds only embed sources, all skipped below.
  // Applies to vector nestedRoot filters.
  // TODO(CLOUDP-441374): search autoEmbed under embeddedDocuments would reach this path.
  private static ImmutableMap<FieldPath, BsonValue> buildRemappedFilterValuesPerMatViewPath(
      RawBsonDocument rawBsonDocument,
      AutoEmbedFieldMapping mappings,
      Map<FieldPath, Map<String, Vector>> consolidatedEmbeddingMap,
      MaterializedViewSchemaMetadata schemaMetadata)
      throws IOException {
    Map<FieldPath, FieldPath> schemaFieldsMapping = schemaMetadata.autoEmbeddingFieldsMapping();
    if (schemaFieldsMapping.isEmpty()) {
      return ImmutableMap.of();
    }
    // Single pass collects both the embed text values (for index selection) and the remapped
    // filter values. AUTO_EMBED writes are still owned by buildEmbeddingsPerMatViewPath; the
    // embed branch here is read-only and used only to pick the array index.
    CollectFieldValueDocumentHandler collectValuesHandler =
        CollectFieldValueDocumentHandler.create(
            mappings,
            Optional.empty(),
            path ->
                schemaFieldsMapping.containsKey(path)
                    && (mappings.isPassthrough(path)
                        || (mappings.isEmbed(path) && consolidatedEmbeddingMap.containsKey(path))),
            true);
    BsonDocumentProcessor.process(rawBsonDocument, collectValuesHandler);
    ImmutableMap<FieldPath, List<BsonValue>> collected = collectValuesHandler.getCollectedValues();

    // For each remapped embed source path, find the document-order index of the value
    // buildEmbeddingsPerMatViewPath will pick (first non-empty BsonString with a vector). If no
    // such index exists, the embed will write nothing for that path and we must skip its filters
    // too — otherwise the MV would carry filter values pointing at no vector.
    Map<FieldPath, Integer> chosenIndexPerEmbedSource = new HashMap<>();
    for (var entry : collected.entrySet()) {
      FieldPath sourcePath = entry.getKey();
      if (!mappings.isEmbed(sourcePath)) {
        continue;
      }
      Map<String, Vector> vectors = consolidatedEmbeddingMap.get(sourcePath);
      List<BsonValue> values = entry.getValue();
      for (int i = 0; i < values.size(); i++) {
        BsonValue v = values.get(i);
        if (v.isString()
            && !v.asString().getValue().isEmpty()
            // A dual-role source is collected as passthrough even when its embedding failed, so
            // the map entry can be absent; skip instead of dereferencing a missing vector map.
            && vectors != null
            && vectors.containsKey(v.asString().getValue())) {
          chosenIndexPerEmbedSource.put(sourcePath, i);
          break;
        }
      }
    }

    ImmutableMap.Builder<FieldPath, BsonValue> resultBuilder = ImmutableMap.builder();
    for (var entry : collected.entrySet()) {
      FieldPath sourcePath = entry.getKey();
      // Only plain filter fields are copied here. An auto-embed source keeps its generated vector
      // at the MV path (_autoEmbed.<path>), and a dual-role field is an embed source and a filter
      // at the same time, so copying its raw value here would overwrite that vector with the raw
      // text.
      if (mappings.isEmbed(sourcePath) || !mappings.isPassthrough(sourcePath)) {
        continue;
      }
      List<BsonValue> values = entry.getValue();
      if (values.isEmpty()) {
        continue;
      }
      Optional<FieldPath> governingEmbed =
          findGoverningEmbedSource(sourcePath, chosenIndexPerEmbedSource.keySet());
      if (governingEmbed.isEmpty()) {
        // No embed in the same nestedRoot subtree produced a vector — skip so we don't write a
        // filter value without a corresponding vector on the same MV child document.
        continue;
      }
      @Var int index = chosenIndexPerEmbedSource.get(governingEmbed.get());
      if (index >= values.size()) {
        // Defensive: the embed and filter lists have diverged (missing values in some array
        // elements shifted the indices). Fall back to the first value rather than dropping the
        // filter entirely — this preserves pre-CLOUDP-402522 behavior for the divergent case.
        LOG.warn(
            "Filter list shorter than embed-chosen index for {}; falling back to first value."
                + " embedSource={} chosenIndex={} filterSize={}",
            sourcePath,
            governingEmbed.get(),
            index,
            values.size());
        index = 0;
      }
      resultBuilder.put(schemaFieldsMapping.get(sourcePath), values.get(index));
    }
    return resultBuilder.build();
  }

  /**
   * Returns the embed source governing {@code filterSource}'s row-index choice: the one sharing the
   * deepest common ancestor with it — in practice the auto-embed under the same nestedRoot.
   */
  private static Optional<FieldPath> findGoverningEmbedSource(
      FieldPath filterSource, Set<FieldPath> embedSources) {
    @Var FieldPath best = null;
    @Var int bestDepth = -1;
    for (FieldPath embedSource : embedSources) {
      Optional<FieldPath> commonAncestor = deepestCommonAncestor(filterSource, embedSource);
      if (commonAncestor.isEmpty()) {
        continue;
      }
      int depth = commonAncestor.get().getPathHierarchy().size();
      if (depth > bestDepth) {
        bestDepth = depth;
        best = embedSource;
      }
    }
    return Optional.ofNullable(best);
  }

  private static Optional<FieldPath> deepestCommonAncestor(FieldPath a, FieldPath b) {
    ImmutableSet<FieldPath> ancestorsOfA = a.ancestorPaths().collect(ImmutableSet.toImmutableSet());
    return b.ancestorPaths().filter(ancestorsOfA::contains).findFirst();
  }

  /**
   * Compares a source collection document with a materialized view document to determine if it
   * needs to be re-indexed. The comparison is a multi-step process:
   *
   * <ul>
   *   <li>1. Compare filter fields. If any filter field values don't match, the document needs to
   *       be re-indexed.
   *   <li>2. Compare auto-embedding fields. If any auto-embedding fields don't match, the document
   *       needs to be re-indexed. For this, we rely on the hashes of the auto-embedding fields.
   *   <li>3. Check if the materialized view document has any extra fields that are no longer in the
   *       index definition. If so, the document needs to be re-indexed.
   * </ul>
   *
   * @return A DocumentComparisonResult object containing a boolean indicating whether the document
   *     needs to be re-indexed, and a map of reusable embeddings.
   */
  public static DocumentComparisonResult compareDocuments(
      RawBsonDocument sourceDoc,
      RawBsonDocument matViewDoc,
      AutoEmbedFieldMapping mappings,
      AutoEmbedFieldMapping matViewMappings,
      MaterializedViewSchemaMetadata schemaMetadata) {
    return compareDocuments(
        sourceDoc, matViewDoc, mappings, matViewMappings, schemaMetadata, Optional.empty());
  }

  /** As above; a present {@code sourceNestedRoot} compares embed fields under it element-wise. */
  public static DocumentComparisonResult compareDocuments(
      RawBsonDocument sourceDoc,
      RawBsonDocument matViewDoc,
      AutoEmbedFieldMapping mappings,
      AutoEmbedFieldMapping matViewMappings,
      MaterializedViewSchemaMetadata schemaMetadata,
      Optional<FieldPath> sourceNestedRoot) {
    // Collect all autoEmbed and filter fields from source doc.
    var sourceFilterValuesCollector =
        CollectFieldValueDocumentHandler.create(mappings, Optional.empty(), (path) -> true, true);

    // Collect all fields from mat view doc, even if they are not in the index definition.
    var matViewFilterValuesCollector =
        CollectFieldValueDocumentHandler.create(
            matViewMappings, Optional.empty(), (path) -> true, false);

    @Var boolean needsReIndexing = false;
    var reusableEmbeddingsBuilder = ImmutableMap.<FieldPath, ImmutableMap<String, Vector>>builder();

    // Search mappings compare passthrough content by projection equality instead of the per-path
    // loop below: dynamic-mode passthroughs (root dynamic and wholesale dynamic documents) are
    // open-ended, so fieldMap() cannot enumerate them for per-path comparison (CLOUDP-384026).
    boolean passthroughComparedByProjection = mappings instanceof SearchIndexAutoEmbedFieldMapping;

    // CLOUDP-428208: the "extra fields" scan below only looks at MV fields the current mapping can
    // still reach. It misses a removed filter that sits inside a subdocument the mapping no longer
    // touches.
    //
    // Example: index on autoEmbed "awards.text" + filter "awards.wins". The MV row looks like
    //   { awards: { wins: 1 }, _autoEmbed: { awards: { text: <vector> } } }
    // The embedding is stored under _autoEmbed.awards.text, NOT awards.text. So when the user drops
    // the "awards.wins" filter, nothing the mapping knows about lives under the top-level "awards"
    // subdocument anymore, and the scan never looks inside it -- the stale "awards.wins" is left
    // behind. (A top-level filter like "year" has no such parent, so the scan sees it fine.)
    //
    // To catch this, also rebuild what the MV row *should* contain from the source doc and compare
    // whole subtrees.
    //
    // This is safe for indexes that declare a nestedRoot too, so it is not excluded here: filters
    // beneath the nestedRoot are stored remapped under _autoEmbed (e.g.
    // _autoEmbed.sections.section_name), and the comparison strips the whole _autoEmbed namespace,
    // so those never take part in it. Filters outside the nestedRoot -- e.g. "metadata.category"
    // with nestedRoot "sections" -- are stored source-shaped exactly like the non-nested case and
    // need the same protection. Only search mappings are skipped, because they already ran this
    // same comparison above.
    boolean detectOrphanedFieldsByProjection = !passthroughComparedByProjection;

    try {
      BsonDocumentProcessor.process(sourceDoc, sourceFilterValuesCollector);
      BsonDocumentProcessor.process(matViewDoc, matViewFilterValuesCollector);

      var sourceDocValues = sourceFilterValuesCollector.getCollectedValues();
      var matViewValues = matViewFilterValuesCollector.getCollectedValues();

      if (passthroughComparedByProjection
          && !passthroughProjectionMatches(sourceDoc, matViewDoc, mappings, schemaMetadata)) {
        needsReIndexing = true;
      }

      for (Map.Entry<FieldPath, AutoEmbedField> entry : mappings.fieldMap().entrySet()) {
        FieldPath autoEmbedPath = entry.getKey();
        FieldPath matViewFieldPath =
            getMatViewFieldPath(autoEmbedPath, schemaMetadata.autoEmbeddingFieldsMapping());
        FieldPath matViewFieldHashPath =
            getHashFieldPath(autoEmbedPath, schemaMetadata.materializedViewSchemaVersion());
        AutoEmbedField field = entry.getValue();

        // Filter values: full ordered-list equality. Top-level filters compare source-vs-source;
        // nested filters (CLOUDP-402522) are co-located at the remapped MV path, one value per
        // index-aligned array element, so ordered-list equality detects any per-element change.
        // Search mappings skip this: their passthrough staleness is already decided by
        // projection equality above.
        if (field instanceof AutoEmbedField.PassthroughField && !passthroughComparedByProjection) {
          if (!Objects.equals(
              sourceDocValues.get(autoEmbedPath), matViewValues.get(matViewFieldPath))) {
            needsReIndexing = true;
          }
        }

        var reusableEmbeddingsForFieldBuilder = ImmutableMap.<String, Vector>builder();

        // TODO(CLOUDP-380567): Only checks when modelConfig hash matches when resyncing across
        // redefinitions.
        // check embeddings against their hashes. Collect ones that match for re-use.
        if (field instanceof AutoEmbedField.EmbedField) {
          boolean isNestedEmbed =
              sourceNestedRoot.isPresent() && autoEmbedPath.isChildOf(sourceNestedRoot.get());
          if (isNestedEmbed) {
            // Element-wise comparison for nested auto-embed: dropping the "" per-element hashes
            // yields the non-empty hashes in child order, aligned 1:1 with collected MV vectors.
            // Re-index only when the ordered source hashes diverge; otherwise reuse every vector.
            var sourceTexts =
                sourceDocValues.getOrDefault(autoEmbedPath, List.of()).stream()
                    .filter(
                        bsonVal -> bsonVal.isString() && !bsonVal.asString().getValue().isEmpty())
                    .map(bsonVal -> bsonVal.asString().getValue())
                    .toList();
            var sourceHashes =
                sourceTexts.stream().map(AutoEmbeddingDocumentUtils::computeTextHash).toList();
            var matViewNonEmptyHashes =
                matViewValues.getOrDefault(matViewFieldHashPath, List.of()).stream()
                    .map(value -> value.asString().getValue())
                    .filter(hash -> !hash.isEmpty())
                    .toList();
            var matViewVectors =
                matViewValues.getOrDefault(matViewFieldPath, List.of()).stream()
                    .map(BsonValue::asBinary)
                    .toList();
            if (!sourceHashes.equals(matViewNonEmptyHashes)
                || matViewVectors.size() != matViewNonEmptyHashes.size()) {
              needsReIndexing = true;
            } else {
              // Distinct elements may repeat text mapping to the same embedding; dedupe so the
              // builder doesn't throw on a duplicate key and force a spurious re-index.
              Set<String> seenTexts = new HashSet<>();
              for (int k = 0; k < sourceTexts.size(); k++) {
                if (seenTexts.add(sourceTexts.get(k))) {
                  reusableEmbeddingsForFieldBuilder.put(
                      sourceTexts.get(k), BsonVectorParser.parse(matViewVectors.get(k)));
                }
              }
            }
          } else if (sourceDocValues.containsKey(autoEmbedPath)) {
            var stringValuesWithHashes =
                sourceDocValues.get(autoEmbedPath).stream()
                    .filter(
                        bsonVal -> bsonVal.isString() && !bsonVal.asString().getValue().isEmpty())
                    .collect(
                        ImmutableListMultimap.toImmutableListMultimap(
                            key -> key.asString().getValue(),
                            value -> computeTextHash(value.asString().getValue())));

            // Empty strings are not embedded, so skip checking the field if all values are empty.
            // The scenario where we have an array field and some values in the array are empty is
            // handled separately later below.
            if (stringValuesWithHashes.isEmpty()) {
              // Case where mat view has embeddings for this field, but source doc has an empty
              // string value.
              if (matViewValues.containsKey(matViewFieldPath)) {
                needsReIndexing = true;
              }
              continue;
            }
            if (matViewValues.containsKey(matViewFieldPath)) {
              var matViewHashes =
                  matViewValues.getOrDefault(matViewFieldHashPath, List.of()).stream()
                      .map(value -> value.asString().getValue())
                      .toList();
              var matViewVectors =
                  matViewValues.get(matViewFieldPath).stream().map(BsonValue::asBinary).toList();
              // TODO(CLOUDP-384026): Support multiple embeddings per field.
              // No need to check the rest as we don't support multiple embeddings per field yet.
              var stringHashEntry = stringValuesWithHashes.entries().stream().iterator().next();
              var index = matViewHashes.indexOf(stringHashEntry.getValue());
              if (index == -1) {
                needsReIndexing = true;
              } else {
                reusableEmbeddingsForFieldBuilder.put(
                    stringHashEntry.getKey(), BsonVectorParser.parse(matViewVectors.get(index)));
              }
            } else {
              needsReIndexing = true;
            }
          } else {
            // Case where mat view has embeddings for this field, but source doc does not have this
            // field at all.
            if (matViewValues.containsKey(matViewFieldPath)) {
              needsReIndexing = true;
            }
          }
          var reusableEmbeddingsForField = reusableEmbeddingsForFieldBuilder.build();
          if (!reusableEmbeddingsForField.isEmpty()) {
            reusableEmbeddingsBuilder.put(autoEmbedPath, reusableEmbeddingsForField);
          }
        }
      }

      // check if mat view has extra fields which are no longer in the index definition.
      // an example of this is when a filter field is removed.
      for (FieldPath fieldPath : matViewValues.keySet()) {
        if (!MV_METADATA_FIELDS.contains(fieldPath.toString())
            && !matViewMappings.isPassthrough(fieldPath)
            && !matViewMappings.fieldMap().containsKey(fieldPath)) {
          needsReIndexing = true;
          break;
        }
      }

      // The scan above can miss a stale filter value buried in a subdocument the mapping no longer
      // touches (CLOUDP-428208; see comment where detectOrphanedFieldsByProjection is set). Rebuild
      // the expected passthrough content from the source doc and compare; a mismatch means the MV
      // row still holds a removed nested filter, so re-index to prune it.
      if (!needsReIndexing
          && detectOrphanedFieldsByProjection
          && !passthroughProjectionMatches(sourceDoc, matViewDoc, mappings, schemaMetadata)) {
        needsReIndexing = true;
      }
    } catch (Exception e) {
      LOG.warn("Caught exception comparing documents, re-indexing.", e);
      needsReIndexing = true;
    }
    return new DocumentComparisonResult(needsReIndexing, reusableEmbeddingsBuilder.build());
  }

  public record DocumentComparisonResult(
      boolean needsReIndexing,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> reusableEmbeddings) {}

  /**
   * Compares the writer's expected passthrough projection of {@code sourceDoc} against {@code
   * matViewDoc} stripped of embed vectors, hashes, and MV metadata. Both sides are shaped by the
   * same {@link MaterializedViewDocumentHandler} traversal that produces MV documents, so equality
   * holds exactly when the MV's passthrough content is current — including open-ended dynamic
   * regions (root dynamic and wholesale dynamic documents) that {@code fieldMap()} cannot enumerate
   * for per-path comparison.
   */
  private static boolean passthroughProjectionMatches(
      RawBsonDocument sourceDoc,
      RawBsonDocument matViewDoc,
      AutoEmbedFieldMapping mappings,
      MaterializedViewSchemaMetadata schemaMetadata)
      throws IOException {
    BsonDocument expected = new BsonDocument();
    BsonDocumentProcessor.process(
        sourceDoc, MaterializedViewDocumentHandler.create(mappings, Optional.empty(), expected));
    stripIdForMaterializedViewWrite(expected);

    // On MV schema v1+ the vectors, hashes, and lease metadata all live under the _autoEmbed
    // namespace, so building the passthrough view by skipping that key (plus top-level metadata
    // fields) avoids decoding the embedding binaries the strips below would just discard. Schema
    // v0 spread them across source-relative paths and needs the decode-and-strip fallback.
    BsonDocument actual;
    if (schemaMetadata.materializedViewSchemaVersion() >= 1) {
      actual = new BsonDocument();
      for (String key : matViewDoc.keySet()) {
        if (!key.equals(MV_NAMESPACE) && !MV_METADATA_FIELDS.contains(key)) {
          actual.put(key, matViewDoc.get(key));
        }
      }
    } else {
      actual = matViewDoc.decode(BsonUtils.BSON_DOCUMENT_CODEC);
      for (String metadataField : MV_METADATA_FIELDS) {
        removePathPruningEmptyAncestors(actual, FieldPath.parse(metadataField));
      }
    }
    for (FieldPath sourceEmbedPath : mappings.embedFields().keySet()) {
      removePathPruningEmptyAncestors(
          actual,
          getMatViewFieldPath(sourceEmbedPath, schemaMetadata.autoEmbeddingFieldsMapping()));
      removePathPruningEmptyAncestors(
          actual,
          getHashFieldPath(sourceEmbedPath, schemaMetadata.materializedViewSchemaVersion()));
    }
    return expected.equals(actual);
  }

  /**
   * Removes the value at {@code path} and prunes ancestor documents the removal leaves empty. Bails
   * out when a segment is absent or traverses a non-document value (e.g. the array shapes {@link
   * #addBsonValueToBsonDocument} can produce for overlapping embed declarations); the leftover
   * value then surfaces as a projection mismatch and the document is re-indexed.
   */
  private static void removePathPruningEmptyAncestors(BsonDocument doc, FieldPath path) {
    ImmutableList<String> segments = ImmutableList.copyOf(path.getSegments());
    BsonDocument[] parents = new BsonDocument[segments.size()];
    @Var BsonDocument current = doc;
    for (int i = 0; i < segments.size() - 1; i++) {
      parents[i] = current;
      BsonValue child = current.get(segments.get(i));
      if (child == null || !child.isDocument()) {
        return;
      }
      current = child.asDocument();
    }
    parents[segments.size() - 1] = current;
    current.remove(segments.get(segments.size() - 1));
    for (int i = segments.size() - 2; i >= 0; i--) {
      if (!parents[i + 1].isEmpty()) {
        break;
      }
      parents[i].remove(segments.get(i));
    }
  }

  /**
   * Decides whether a change-stream update can be applied as a partial {@code $set} on the MV (vs a
   * full document replacement) for an auto-embed index. Returns a {@link
   * PartialEmbedClassification}: an {@code applied} result carrying the {@code path -> chosen
   * source text} map of embed fields to re-embed, or a {@code fellBack} result carrying the {@link
   * PartialEmbedFallbackReason} when the caller must fall back to a full replacement.
   *
   * <p>Should only be called when {@link #requiresEmbeddingGeneration} already returned true. The
   * result is {@code applied} only when all of these hold:
   *
   * <ul>
   *   <li>The mapping enumerates every indexed source path. Vector mappings always do; static
   *       Search mappings do when no dynamic subtree is enabled.
   *   <li>No removed or truncated field touches any declared embed path. A partial {@code $set} has
   *       no way to clear an MV vector or hash that the source removed.
   *   <li>Every updated field is either an exact embed match, an exact passthrough, or unrelated to
   *       any embed declaration. Indirect touches (e.g. updating an ancestor or descendant of an
   *       embed path) force a fallback because we can't precisely scope them.
   *   <li>No changed embed source path traverses an array in the source doc — MongoDB rejects
   *       dotted {@code $set} keys (e.g. {@code _autoEmbed.items.title}) when an ancestor is an
   *       array.
   *   <li>Every changed embed source path resolves to a non-empty source string. An empty new value
   *       can't be expressed by partial {@code $set}; full replace handles it by dropping the MV
   *       vector.
   * </ul>
   *
   * <p>Texts in the returned map are document-order first non-empty strings — the same selection
   * the full-replace path uses in {@link #buildMaterializedViewDocumentEvent} — so the embedding
   * scheduler and {@link #buildPartialEmbedUpdateSetBody} can consume them without walking the
   * source document again.
   */
  public static PartialEmbedClassification computePartialEmbedChangedTexts(
      UpdateDescription updateDescription,
      RawBsonDocument fullDocument,
      AutoEmbedFieldMapping fieldMapping) {
    if (updateDescription == null) {
      // Mirror requiresEmbeddingGeneration's null handling: when the update description is
      // unavailable we can't reason about what changed, so we can't safely emit a partial-embed
      // update — caller must fall back to a full document replacement.
      return PartialEmbedClassification.fellBack(
          PartialEmbedFallbackReason.NULL_UPDATE_DESCRIPTION);
    }
    if (fullDocument == null) {
      // Defensive: mongot configures change streams with fullDocument lookup, so this should not
      // happen in production. Match requiresEmbeddingGeneration's conservatism if the assumption
      // ever breaks — we can't extract embed-field text without the document.
      return PartialEmbedClassification.fellBack(PartialEmbedFallbackReason.NULL_FULL_DOCUMENT);
    }
    if (affectedFieldPaths(updateDescription).isEmpty()) {
      // Mirror requiresEmbeddingGeneration's "no channels reported anything" branch — that gate
      // returns true (be safe) for this shape, so a correctly-gated caller can still dispatch
      // here. Fall back to a full replace rather than reaching the post-loop precondition.
      return PartialEmbedClassification.fellBack(PartialEmbedFallbackReason.NO_CHANNELS_REPORTED);
    }
    if (!fieldMapping.enumeratesAllIndexedPaths()) {
      // The destructive-touch check below requires fieldMap() to enumerate every indexed path.
      // Dynamic Search mappings intentionally under-enumerate, which would let a destructive
      // update misclassify as partial-safe and leave stale values on the MV.
      return PartialEmbedClassification.fellBack(
          PartialEmbedFallbackReason.UNSUPPORTED_FIELD_MAPPING);
    }

    Set<FieldPath> declaredEmbedPaths = fieldMapping.embedFields().keySet();
    // All declared indexed paths — embeds plus passthrough fields. The completeness precondition
    // above is what makes destructive-touch analysis safe for both Vector and static Search.
    Set<FieldPath> declaredIndexedPaths = fieldMapping.fieldMap().keySet();

    // Destructive channels (removedFields, truncatedArrays): partial $set can't express a
    // remove or shorten on the MV, for embeds (vector + hash must be dropped) and for filter
    // fields alike (the $set body is built from the post-image, which no longer carries the
    // removed value, so the stale MV value would survive). Any indexed-field touch here forces
    // a full replace.
    // TODO(CLOUDP-412618): extend partial-update body with $unset so these cases stay partial.
    for (String touched : destructivelyAffectedFieldPaths(updateDescription)) {
      if (mayAffectDeclaredField(FieldPath.parse(touched), declaredIndexedPaths)) {
        return PartialEmbedClassification.fellBack(
            PartialEmbedFallbackReason.DESTRUCTIVE_INDEXED_TOUCH);
      }
    }

    // Updates: collect exact-match embed paths as re-embed candidates. Bail on indirect touches
    // (an updated field that's an ancestor/descendant/array-indexed form of an embed path) —
    // we can't precisely scope what changed. Filter (passthrough) paths are handled by the same
    // mayAffectDeclaredField check the gate (requiresEmbeddingGeneration) uses, so a filter that
    // shares an ancestor/descendant relation with an embed (e.g. filter "b", embed "b.c") falls
    // back here rather than slipping through and producing an empty changedEmbedPaths set.
    ImmutableSet.Builder<FieldPath> changedEmbedPathsBuilder = ImmutableSet.builder();
    for (String updated : updatedFieldKeys(updateDescription)) {
      FieldPath path = FieldPath.parse(updated);
      // A $set on a strict ancestor of any declared indexed path is a deletion in disguise: a
      // scalar overwrite (e.g. $set {meta: 5} over filter "meta.category", or $set {a: "y"}
      // where embed "a" is the ancestor of filter "a.b") can remove the declared value from the
      // post-image, and the partial $set body — built from the post-image — would then carry
      // nothing to overwrite the stale MV value. Checked before the embed branch so it covers
      // exact-embed paths that are also ancestors of filters.
      if (isStrictAncestorOfAny(path, declaredIndexedPaths)) {
        return PartialEmbedClassification.fellBack(PartialEmbedFallbackReason.ANCESTOR_OVERWRITE);
      }
      if (fieldMapping.isEmbed(path)) {
        // Overlapping embed declarations (e.g. "a" and "a.b" — definition validation only rejects
        // exact duplicates): a write to one can reshape what the other extracts, and a partial
        // $set can't drop the other's now-stale vector + hash. Fall back to a full replace.
        for (FieldPath other : declaredEmbedPaths) {
          if (!other.equals(path) && path.isDirectRelation(other)) {
            return PartialEmbedClassification.fellBack(
                PartialEmbedFallbackReason.OVERLAPPING_EMBEDS);
          }
        }
        changedEmbedPathsBuilder.add(path);
        continue;
      }
      if (mayAffectDeclaredField(path, declaredEmbedPaths)) {
        return PartialEmbedClassification.fellBack(PartialEmbedFallbackReason.INDIRECT_EMBED_TOUCH);
      }
    }

    return extractChangedEmbedTexts(changedEmbedPathsBuilder.build(), fullDocument, fieldMapping);
  }

  /**
   * Reads the new text for each changed embed path from the (view-resolved) full document, applying
   * the guards a partial {@code $set} can't express: array traversal, extraction failure, and empty
   * new value. Shared by the non-view classifier and the view-based partial-embed path; the caller
   * supplies the changed embed paths (non-view by direct match, view by feeder lookup).
   */
  public static PartialEmbedClassification extractChangedEmbedTexts(
      ImmutableSet<FieldPath> changedEmbedPaths,
      RawBsonDocument fullDocument,
      AutoEmbedFieldMapping fieldMapping) {
    Verify.verify(
        !changedEmbedPaths.isEmpty(),
        "extractChangedEmbedTexts called with no changed embed paths");

    for (FieldPath embedPath : changedEmbedPaths) {
      if (sourcePathTraversesArray(fullDocument, embedPath)) {
        // TODO(CLOUDP-412619): investigate partial-embed support for nested embeddings.
        return PartialEmbedClassification.fellBack(PartialEmbedFallbackReason.ARRAY_TRAVERSAL);
      }
    }

    // Collect first-non-empty-string in document order for each changed embed path. Uses the same
    // CollectFieldValueDocumentHandler the full-replace path uses, so partial-embed and full-
    // replace agree on which text wins for paths with multiple candidate strings.
    CollectFieldValueDocumentHandler textCollector =
        CollectFieldValueDocumentHandler.create(
            fieldMapping, Optional.empty(), changedEmbedPaths::contains, true);
    try {
      BsonDocumentProcessor.process(fullDocument, textCollector);
    } catch (IOException e) {
      LOG.warn(
          "Failed to extract embed-field text while classifying partial-embed safety; falling"
              + " back to full replace",
          e);
      return PartialEmbedClassification.fellBack(PartialEmbedFallbackReason.TEXT_EXTRACTION_FAILED);
    }
    ImmutableMap<FieldPath, List<BsonValue>> collectedValues = textCollector.getCollectedValues();
    ImmutableMap.Builder<FieldPath, String> textsBuilder = ImmutableMap.builder();
    for (FieldPath embedPath : changedEmbedPaths) {
      Optional<String> textOpt =
          collectedValues.getOrDefault(embedPath, List.of()).stream()
              .filter(bv -> bv.isString() && !bv.asString().getValue().isEmpty())
              .findFirst()
              .map(bv -> bv.asString().getValue());
      if (textOpt.isEmpty()) {
        // No non-empty text for this changed embed path. A partial $set has no way to $unset the
        // existing MV vector + hash for the field; fall back to a full replace which clears them
        // via the ReplaceOne.
        // TODO(CLOUDP-412618): extend partial-update body with $unset so this stays partial.
        return PartialEmbedClassification.fellBack(PartialEmbedFallbackReason.EMPTY_NEW_VALUE);
      }
      textsBuilder.put(embedPath, textOpt.get());
    }
    return PartialEmbedClassification.applied(textsBuilder.build());
  }

  /**
   * Returns the field paths that could have affected source content for this update, drawn from
   * every {@link UpdateDescription} channel that mongot's change-stream consumer cares about:
   * {@link UpdateDescription#getUpdatedFields()}, {@link UpdateDescription#getRemovedFields()}, and
   * {@link UpdateDescription#getTruncatedArrays()}. Returned as a flat list of dotted paths; the
   * channel each entry came from is intentionally discarded — callers that need it should use
   * {@link #updatedFieldKeys} / {@link #destructivelyAffectedFieldPaths} instead.
   */
  private static List<String> affectedFieldPaths(UpdateDescription updateDescription) {
    List<String> out = new ArrayList<>();
    out.addAll(updatedFieldKeys(updateDescription));
    out.addAll(destructivelyAffectedFieldPaths(updateDescription));
    return out;
  }

  /** The {@code updatedFields} channel only. Empty when the update set nothing. */
  private static Set<String> updatedFieldKeys(UpdateDescription updateDescription) {
    return Optional.ofNullable(updateDescription.getUpdatedFields())
        .map(BsonDocument::keySet)
        .orElseGet(Collections::emptySet);
  }

  /**
   * The destructive change channels — {@code removedFields} ($unset) and {@code truncatedArrays}
   * ($pop / $slice) — combined. A partial {@code $set} body cannot express any of these on the MV
   * side; if any destructive touch lands on an embed path the partial-embed optimization must fall
   * back to a full ReplaceOne so the MV vector and hash are correctly cleared.
   */
  private static List<String> destructivelyAffectedFieldPaths(UpdateDescription updateDescription) {
    List<String> out = new ArrayList<>();
    Optional.ofNullable(updateDescription.getRemovedFields()).ifPresent(out::addAll);
    Optional.ofNullable(updateDescription.getTruncatedArrays())
        .ifPresent(arrays -> arrays.forEach(t -> out.add(t.getField())));
    return out;
  }

  /**
   * True if {@code path} is a strict (non-equal) ancestor of any path in {@code declaredPaths},
   * either literally or after stripping array-index segments. A {@code $set} on such a path can
   * implicitly delete the declared descendant's value from the post-image (e.g. a scalar overwrite
   * of a subdocument), which a partial {@code $set} built from the post-image cannot mirror on the
   * MV.
   */
  private static boolean isStrictAncestorOfAny(FieldPath path, Set<FieldPath> declaredPaths) {
    if (isLiteralStrictAncestorOfAny(path, declaredPaths)) {
      return true;
    }
    Optional<FieldPath> stripped = stripArrayIndexSegments(path);
    return stripped.isPresent() && isLiteralStrictAncestorOfAny(stripped.get(), declaredPaths);
  }

  private static boolean isLiteralStrictAncestorOfAny(
      FieldPath path, Set<FieldPath> declaredPaths) {
    for (FieldPath declared : declaredPaths) {
      if (declared.isChildOf(path)) {
        return true;
      }
    }
    return false;
  }

  /**
   * True if {@code candidate} might affect the source content of any path in {@code declaredPaths}.
   * Recognizes three forms of contact, in order of cost:
   *
   * <ol>
   *   <li>Exact match (the candidate IS a declared path).
   *   <li>Literal-segment relation — ancestor or descendant under straight segment comparison.
   *   <li>Array-index relation — the same check after stripping numeric segments. Catches
   *       change-stream paths like {@code "items.0.title"} that target a declared path {@code
   *       "items.title"}; {@link FieldPath#isDirectRelation} compares literally and would otherwise
   *       miss them.
   * </ol>
   *
   * <p>Callers choose the declared set: embed paths only (re-embed routing) or all indexed paths
   * (destructive-touch safety, where filter staleness matters too).
   */
  private static boolean mayAffectDeclaredField(FieldPath candidate, Set<FieldPath> declaredPaths) {
    if (declaredPaths.contains(candidate)) {
      return true;
    }
    for (FieldPath declared : declaredPaths) {
      if (candidate.isDirectRelation(declared)) {
        return true;
      }
    }
    Optional<FieldPath> stripped = stripArrayIndexSegments(candidate);
    if (stripped.isEmpty()) {
      return false;
    }
    for (FieldPath declared : declaredPaths) {
      if (stripped.get().isDirectRelation(declared)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns {@code path} with numeric segments removed. Empty when {@code path} had no numeric
   * segments (or had only numeric segments) — i.e. there's nothing array-index-related to check.
   * Numeric segments are how change-stream events address array elements (e.g. {@code
   * "items.0.title"} for the first element's {@code title} subfield).
   */
  private static Optional<FieldPath> stripArrayIndexSegments(FieldPath path) {
    List<String> kept = new ArrayList<>();
    @Var boolean strippedAny = false;
    for (String segment : path.getSegments()) {
      if (isNumeric(segment)) {
        strippedAny = true;
      } else {
        kept.add(segment);
      }
    }
    if (!strippedAny || kept.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(FieldPath.fromParts(kept.toArray(new String[0])));
  }

  private static boolean isNumeric(String s) {
    return !s.isEmpty() && s.chars().allMatch(Character::isDigit);
  }

  private static boolean sourcePathTraversesArray(RawBsonDocument document, FieldPath path) {
    @Var BsonValue current = document;
    Iterator<String> segments = path.getSegments().iterator();
    while (segments.hasNext()) {
      String segment = segments.next();
      if (current == null || !current.isDocument()) {
        return false;
      }
      BsonValue child = current.asDocument().get(segment);
      if (segments.hasNext() && child != null && child.isArray()) {
        return true;
      }
      current = child;
    }
    return false;
  }

  /**
   * Determines if an update requires embedding generation (i.e., any AUTO_EMBED or TEXT field
   * changed). When this returns false, we can skip the embedding service call and use a partial
   * update for filter-only changes.
   *
   * <p>This method uses positive assertion (checking if embedding IS required) rather than negative
   * assertion (checking if it's NOT required) for fail-safe behavior: if there's a bug, we make
   * extra embedding calls (performance issue) rather than skip necessary ones (data correctness
   * issue).
   *
   * <p>Note: Fields not in the index definition (non-indexed fields) are treated the same as filter
   * fields - they don't require embedding generation. These fields are simply not propagated to the
   * materialized view.
   *
   * @param updateDescription the update description from the change stream event
   * @param fieldMapping the vector index field mapping
   * @return true if any AUTO_EMBED/TEXT fields changed and embedding is required, false if only
   *     filter fields or non-indexed fields were updated
   */
  public static boolean requiresEmbeddingGeneration(
      UpdateDescription updateDescription, AutoEmbedFieldMapping fieldMapping) {
    if (updateDescription == null) {
      // No update description means we can't determine what changed - require embedding to be safe
      return true;
    }

    List<String> affected = affectedFieldPaths(updateDescription);
    // No fields reported by any channel → can't determine what changed; require embedding to be
    // safe.
    if (affected.isEmpty()) {
      return true;
    }

    Set<FieldPath> declaredEmbedPaths = fieldMapping.embedFields().keySet();
    for (String f : affected) {
      if (mayAffectDeclaredField(FieldPath.parse(f), declaredEmbedPaths)) {
        return true;
      }
    }
    return false;
  }

  // True when a filter under sourceNestedRoot is co-located with the vector (CLOUDP-402522).
  public static boolean hasFilterUnderNestedRoot(
      AutoEmbedFieldMapping fieldMapping, Optional<FieldPath> sourceNestedRoot) {
    if (sourceNestedRoot.isEmpty()) {
      return false;
    }
    return fieldMapping.fieldMap().values().stream()
        .filter(field -> field instanceof AutoEmbedField.PassthroughField)
        .anyMatch(
            field ->
                AutoEmbedFieldMappingCreator.isFilterUnderSourceNestedRoot(
                    field.path(), sourceNestedRoot));
  }

  /**
   * Returns a fallback reason when a filter-only partial $set would leave a stale MV filter value
   * it can't clear (else empty): a filter $unset/$pop/$slice'd away, or a sub-field dropped when a
   * $set overwrites its parent object. Covers co-located nested filters and top-level filters.
   *
   * <p>TODO(CLOUDP-412618): $unset support in the partial body would keep these updates partial.
   */
  public static Optional<PartialEmbedFallbackReason> filterOnlyPartialUpdateFallbackReason(
      UpdateDescription updateDescription,
      AutoEmbedFieldMapping mapping,
      Optional<FieldPath> sourceNestedRoot) {
    if (updateDescription == null) {
      return Optional.empty();
    }
    Set<FieldPath> nestedFilters = new HashSet<>();
    Set<FieldPath> topLevelFilters = new HashSet<>();
    for (var entry : mapping.fieldMap().entrySet()) {
      if (!(entry.getValue() instanceof AutoEmbedField.PassthroughField)) {
        continue;
      }
      FieldPath path = entry.getKey();
      if (sourceNestedRoot.isPresent() && path.isChildOf(sourceNestedRoot.get())) {
        nestedFilters.add(path);
      } else {
        topLevelFilters.add(path);
      }
    }
    if (nestedFilters.isEmpty() && topLevelFilters.isEmpty()) {
      return Optional.empty();
    }

    // Destructive channels ($unset / $pop|$slice) that remove or shorten a declared filter.
    for (String touched : destructivelyAffectedFieldPaths(updateDescription)) {
      FieldPath path = FieldPath.parse(touched);
      if (mayAffectDeclaredField(path, nestedFilters)) {
        return Optional.of(PartialEmbedFallbackReason.NESTED_FILTER_REMAPPING);
      }
      if (mayAffectDeclaredField(path, topLevelFilters)) {
        return Optional.of(PartialEmbedFallbackReason.DESTRUCTIVE_INDEXED_TOUCH);
      }
    }
    // A $set on a strict ancestor of a filter overwrites the parent object. If the replacement
    // contains every declared descendant, the partial $set body extracted from the post-image can
    // rebuild that subtree safely; otherwise the stale MV value must be cleared by a full replace.
    BsonDocument updatedFields = updateDescription.getUpdatedFields();
    if (updatedFields == null) {
      return Optional.empty();
    }
    for (var updated : updatedFields.entrySet()) {
      FieldPath path = FieldPath.parse(updated.getKey());
      if (isStrictAncestorOfAny(path, nestedFilters)
          && !replacementPreservesDeclaredFields(updated.getValue(), path, nestedFilters)) {
        return Optional.of(PartialEmbedFallbackReason.NESTED_FILTER_REMAPPING);
      }
      if (isStrictAncestorOfAny(path, topLevelFilters)
          && !replacementPreservesDeclaredFields(updated.getValue(), path, topLevelFilters)) {
        return Optional.of(PartialEmbedFallbackReason.ANCESTOR_OVERWRITE);
      }
    }
    return Optional.empty();
  }

  /**
   * Returns true when a replacement value still contains every declared filter under {@code
   * updatedPath}. Array-indexed updates keep the conservative full-replace behavior.
   */
  private static boolean replacementPreservesDeclaredFields(
      BsonValue replacement, FieldPath updatedPath, Set<FieldPath> declaredFilters) {
    if (stripArrayIndexSegments(updatedPath).isPresent()) {
      return false;
    }
    for (FieldPath declaredFilter : declaredFilters) {
      if (!declaredFilter.isChildOf(updatedPath)) {
        continue;
      }
      FieldPath relativePath = relativizeUnderNestedRoot(declaredFilter, updatedPath);
      if (!containsPath(replacement, ImmutableList.copyOf(relativePath.getSegments()), 0)) {
        return false;
      }
    }
    return true;
  }

  private static boolean containsPath(BsonValue value, ImmutableList<String> segments, int index) {
    if (value == null) {
      return false;
    }
    if (index == segments.size()) {
      return true;
    }
    if (value.isArray()) {
      return value.asArray().getValues().stream()
          .allMatch(element -> containsPath(element, segments, index));
    }
    if (!value.isDocument()) {
      return false;
    }
    return containsPath(value.asDocument().get(segments.get(index)), segments, index + 1);
  }

  // TODO(CLOUDP-363302): Supports stored source.
  /**
   * Extracts filter field values from the full document to build a $set document for partial
   * updates. Only includes fields that are defined as FILTER in the index definition.
   *
   * <p>Reuses {@link MaterializedViewDocumentHandler} — the same traversal used by full MV writes —
   * so the resulting $set document has the same nested structure (including arrays of subdocuments)
   * as the MV produced by a full replace. This keeps the MV consistent with the source for Lucene
   * replication and avoids MongoDB rejecting dotted-path updates that traverse arrays
   * (CLOUDP-406702).
   *
   * <p>The returned document never contains a top-level {@code _id}; see {@link
   * #stripIdForMaterializedViewWrite} for the contract and rationale.
   *
   * @param fullDocument the full document from the change stream event
   * @param fieldMapping the vector index field mapping
   * @return the filter-field {@code $set} body on success — possibly empty when the index declares
   *     no filter fields — or {@link Optional#empty()} if extraction failed, in which case the
   *     caller must fall back to a full document replacement rather than issue a partial {@code
   *     $set} that would silently drop filter fields. The empty-Optional and empty-body cases are
   *     deliberately distinct so callers don't conflate "no filter fields" with "extraction broke".
   */
  public static Optional<BsonDocument> extractFilterFieldValues(
      RawBsonDocument fullDocument, AutoEmbedFieldMapping fieldMapping) {
    try {
      BsonDocument bsonDoc = new BsonDocument();
      MaterializedViewDocumentHandler handler =
          MaterializedViewDocumentHandler.create(fieldMapping, Optional.empty(), bsonDoc);
      BsonDocumentProcessor.process(fullDocument, handler);
      stripIdForMaterializedViewWrite(bsonDoc);
      return Optional.of(bsonDoc);
    } catch (Exception e) {
      LOG.warn(
          "Failed to extract filter field values; caller must fall back to a full replacement", e);
      return Optional.empty();
    }
  }

  /** Adds the MV's co-located nested filter values as array-positional keys (CLOUDP-402522). */
  public static BsonDocument buildNestedFilterPartialSetBody(
      BsonDocument filterFieldSetBody,
      RawBsonDocument fullDocument,
      AutoEmbedFieldMapping sourceMapping,
      Optional<FieldPath> sourceNestedRoot,
      MaterializedViewSchemaMetadata schemaMetadata) {
    BsonDocument result = filterFieldSetBody.clone();
    if (sourceNestedRoot.isEmpty()) {
      return result;
    }
    FieldPath nestedRoot = sourceNestedRoot.get();
    Optional<FieldPath> mvNestedRoot =
        AutoEmbedFieldMappingCreator.getMatViewNestedRoot(
            sourceNestedRoot, schemaMetadata.autoEmbeddingFieldsMapping());
    if (mvNestedRoot.isEmpty()) {
      return result;
    }
    // When the MV nestedRoot equals the source nestedRoot (no _autoEmbed.* remapping),
    // extractFilterFieldValues already wrote the values there; array-positional keys under the same
    // root would be redundant and MongoDB rejects the $set as a path conflict.
    if (mvNestedRoot.get().equals(nestedRoot)) {
      return result;
    }
    List<FieldPath> filterFields = new ArrayList<>();
    for (var entry : sourceMapping.fieldMap().entrySet()) {
      if (entry.getValue() instanceof AutoEmbedField.PassthroughField
          && entry.getKey().isChildOf(nestedRoot)) {
        filterFields.add(entry.getKey());
      }
    }
    if (filterFields.isEmpty()) {
      return result;
    }
    String mvRoot = mvNestedRoot.get().toString();
    List<BsonValue> elements = nestedRootElements(fullDocument, nestedRoot);
    for (int i = 0; i < elements.size(); i++) {
      if (!elements.get(i).isDocument()) {
        continue;
      }
      BsonDocument element = elements.get(i).asDocument();
      for (FieldPath filterField : filterFields) {
        FieldPath relativePath = relativizeUnderNestedRoot(filterField, nestedRoot);
        String key = mvRoot + FieldPath.DELIMITER + i + FieldPath.DELIMITER + relativePath;
        valueAt(element, relativePath).ifPresent(value -> result.put(key, value));
      }
    }
    return result;
  }

  /**
   * Augments a filter-only {@code $set} body with the new vector and hash entries for each changed
   * auto-embed field. Used by the partial-embed update optimization: instead of replacing the
   * entire materialized-view document, we issue a {@code $set} that touches only the filter fields
   * plus the embeddings of the embed fields that actually changed, leaving every other embedding
   * untouched on the MV document.
   *
   * <p>The MV vector and hash entries are written under their dotted MV paths (e.g. {@code
   * "_autoEmbed.ae1"} and {@code "_autoEmbed._hash.ae1"}). The caller is responsible for verifying
   * — via {@link #computePartialEmbedChangedTexts} — that none of these paths traverses an array on
   * the source document, since MongoDB would otherwise reject the dotted-key {@code $set}.
   *
   * <p>On MV schema v1+ the vector and hash keys live under the MV-reserved {@code _autoEmbed.*}
   * namespace, and the index-definition layer rejects filter declarations under it — so {@code
   * filterFieldSetBody} cannot contain an entry that conflicts (ancestor or descendant) with the
   * dotted keys written here, and MongoDB will not reject the resulting {@code $set} for path
   * overlap. Schema v0 placed these keys at source-relative paths where that guarantee does not
   * hold; v0 is retired (all new MV collections are stamped v1+, and the community/EA naming
   * unification in CLOUDP-409822 orphans the remaining v0 collections).
   *
   * <p>If the embedding service returned no vector for a non-empty changed-path text, this method
   * throws an {@link IllegalStateException}. Voyage's contract is one vector per non-empty input; a
   * null vector indicates a contract violation, so the batch is failed loudly rather than silently
   * writing an inconsistent MV state.
   *
   * @param filterFieldSetBody the filter-field {@code $set} body produced by {@link
   *     #extractFilterFieldValues} — copied, not mutated.
   * @param changedEmbedTexts source-side declared embed paths whose embeddings changed, mapped to
   *     the chosen non-empty text for each (from {@link #computePartialEmbedChangedTexts}).
   *     Precondition: every value is a non-empty source-side text; the builder does not re-verify.
   * @param vectorsPerField the embedding service response, keyed by source field path then by text
   * @param schemaMetadata the MV schema metadata, used to derive MV and hash paths
   * @return a new {@link BsonDocument} containing the filter-field entries plus vector and hash
   *     entries for each changed embed path. Suitable for use as the body of a {@code $set}.
   */
  public static BsonDocument buildPartialEmbedUpdateSetBody(
      BsonDocument filterFieldSetBody,
      ImmutableMap<FieldPath, String> changedEmbedTexts,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> vectorsPerField,
      MaterializedViewSchemaMetadata schemaMetadata) {
    BsonDocument result = filterFieldSetBody.clone();
    for (Map.Entry<FieldPath, String> entry : changedEmbedTexts.entrySet()) {
      FieldPath sourcePath = entry.getKey();
      String text = entry.getValue();
      Vector vector = vectorsPerField.getOrDefault(sourcePath, ImmutableMap.of()).get(text);
      if (vector == null) {
        throw new IllegalStateException(
            "Embedding service returned no vector for non-empty text at path "
                + sourcePath
                + "; failing the batch so the inconsistency is surfaced rather than written to MV");
      }
      FieldPath mvPath =
          getMatViewFieldPath(sourcePath, schemaMetadata.autoEmbeddingFieldsMapping());
      FieldPath hashPath =
          getHashFieldPath(sourcePath, schemaMetadata.materializedViewSchemaVersion());
      result.put(mvPath.toString(), BsonVectorParser.encode(vector));
      result.put(hashPath.toString(), new BsonString(computeTextHash(text)));
    }
    return result;
  }

  /**
   * Computes a SHA-256 hash of the given text value and returns it as a hex string.
   *
   * @param text The text to hash
   * @return The SHA-256 hash as a hex string
   */
  public static String computeTextHash(String text) {
    return Hashing.sha256().hashString(text, StandardCharsets.UTF_8).toString();
  }

  static void addBsonValueToBsonDocument(
      BsonDocument bsonDoc, FieldPath path, BsonValue bsonValue) {
    @Var BsonDocument currentDoc = bsonDoc;
    for (String parent :
        path.getParent().map(FieldPath::getSegments).orElse(Collections.emptyList())) {
      var childBsonValue = currentDoc.get(parent);
      if (childBsonValue == null) {
        currentDoc.put(parent, new BsonDocument());
        currentDoc = currentDoc.getDocument(parent);
      } else if (childBsonValue.isDocument()) {
        currentDoc = childBsonValue.asDocument();
      } else if (childBsonValue.isArray()) {
        BsonArray childBsonArray = childBsonValue.asArray();
        currentDoc = new BsonDocument();
        childBsonArray.add(currentDoc);
      } else {
        // Neither BsonDocument nor BsonArray, we need to convert this leaf node to a BsonArray.
        // This could happen when input document is {a: ["textToEmbed1", {b:"textToEmbed2"}]} and we
        // have both autoEmbed fields on "a" and "a.b"
        BsonArray childBsonArray = new BsonArray();
        currentDoc.put(parent, childBsonArray);
        childBsonArray.add(childBsonValue);
        currentDoc = new BsonDocument();
        childBsonArray.add(currentDoc);
      }
    }
    currentDoc.put(path.getLeaf(), bsonValue);
  }

  private static ImmutableMap<FieldPath, BsonValue> buildEmbeddingsPerMatViewPath(
      RawBsonDocument rawBsonDocument,
      AutoEmbedFieldMapping mappings,
      Map<FieldPath, Map<String, Vector>> consolidatedEmbeddingMap,
      MaterializedViewSchemaMetadata schemaMetadata,
      Optional<FieldPath> sourceNestedRoot)
      throws IOException {
    // Nested-root embed fields are written per-element by addNestedAutoEmbedArrays; this
    // single-vector path handles only non-nested embed fields (all fields for a non-nested index).
    CollectFieldValueDocumentHandler collectTextValuesHandler =
        CollectFieldValueDocumentHandler.create(
            mappings,
            Optional.empty(),
            path ->
                mappings.isEmbed(path)
                    && consolidatedEmbeddingMap.containsKey(path)
                    && sourceNestedRoot.map(root -> !path.isChildOf(root)).orElse(true),
            true);
    BsonDocumentProcessor.process(rawBsonDocument, collectTextValuesHandler);

    // We don't support multiple vectors on the sample FieldPath yet, only embeds the first
    // vector text at this time.
    return collectTextValuesHandler.getCollectedValues().entrySet().stream()
        .map(
            entry ->
                new AbstractMap.SimpleEntry<>(
                    entry.getKey(),
                    entry.getValue().stream()
                        .filter(
                            bsonVal ->
                                bsonVal.isString() && !bsonVal.asString().getValue().isEmpty())
                        .findFirst()
                        .map(bsonVal -> bsonVal.asString().getValue())))
        .filter(
            entry ->
                entry.getValue().isPresent()
                    && consolidatedEmbeddingMap
                        .get(entry.getKey())
                        .containsKey(entry.getValue().get()))
        .flatMap(
            entry ->
                Stream.of(
                    new AbstractMap.SimpleEntry<>(
                        getMatViewFieldPath(
                            entry.getKey(), schemaMetadata.autoEmbeddingFieldsMapping()),
                        BsonVectorParser.encode(
                            consolidatedEmbeddingMap
                                .get(entry.getKey())
                                .get(entry.getValue().get()))),
                    new AbstractMap.SimpleEntry<>(
                        getHashFieldPath(
                            entry.getKey(), schemaMetadata.materializedViewSchemaVersion()),
                        new BsonString(computeTextHash(entry.getValue().get())))))
        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  // Builds the nested auto-embed MV structure: one MV child per source array element (vector(s) +
  // co-located filter value(s)) plus a parallel per-embed-field hash array, index-aligned.
  private static void addNestedAutoEmbedArrays(
      BsonDocument bsonDoc,
      RawBsonDocument source,
      AutoEmbedFieldMapping mappings,
      Map<FieldPath, Map<String, Vector>> consolidatedEmbeddingMap,
      MaterializedViewSchemaMetadata schemaMetadata,
      FieldPath sourceNestedRoot) {
    // v0 sibling-suffix hashes collide with the passed-through source array at the same root path;
    // nested auto-embed is v1+, so fail loud rather than produce a structurally-corrupt MV doc.
    Verify.verify(
        schemaMetadata.materializedViewSchemaVersion() >= 1,
        "nested auto-embed requires MV schema v1+");
    List<FieldPath> embedFields = new ArrayList<>();
    List<FieldPath> filterFields = new ArrayList<>();
    for (var entry : mappings.fieldMap().entrySet()) {
      FieldPath path = entry.getKey();
      if (!path.isChildOf(sourceNestedRoot)) {
        continue;
      }
      if (entry.getValue() instanceof AutoEmbedField.EmbedField) {
        embedFields.add(path);
      } else if (entry.getValue() instanceof AutoEmbedField.PassthroughField) {
        filterFields.add(path);
      }
    }
    if (embedFields.isEmpty() && filterFields.isEmpty()) {
      return;
    }
    Optional<FieldPath> mvNestedRoot =
        AutoEmbedFieldMappingCreator.getMatViewNestedRoot(
            Optional.of(sourceNestedRoot), schemaMetadata.autoEmbeddingFieldsMapping());
    if (mvNestedRoot.isEmpty()) {
      return;
    }
    List<BsonValue> elements = nestedRootElements(source, sourceNestedRoot);
    BsonArray childArray = new BsonArray();
    Map<FieldPath, BsonArray> hashListPerEmbedField = new LinkedHashMap<>();
    embedFields.forEach(embedField -> hashListPerEmbedField.put(embedField, new BsonArray()));
    for (BsonValue elementValue : elements) {
      BsonDocument child = new BsonDocument();
      if (!elementValue.isDocument()) {
        LOG.warn(
            "nested auto-embed: element under '{}' is {}, not a document; skipping its filters",
            sourceNestedRoot,
            elementValue.getBsonType());
      }
      BsonDocument element =
          elementValue.isDocument() ? elementValue.asDocument() : new BsonDocument();
      for (FieldPath embedField : embedFields) {
        FieldPath relativePath = relativizeUnderNestedRoot(embedField, sourceNestedRoot);
        Optional<String> text = firstNonEmptyStringAt(element, relativePath);
        Map<String, Vector> vectors = consolidatedEmbeddingMap.get(embedField);
        if (text.isPresent() && vectors != null && vectors.containsKey(text.get())) {
          addBsonValueToBsonDocument(
              child, relativePath, BsonVectorParser.encode(vectors.get(text.get())));
          hashListPerEmbedField.get(embedField).add(new BsonString(computeTextHash(text.get())));
        } else {
          // Keep the hash array aligned with the element array even when it has no vector.
          hashListPerEmbedField.get(embedField).add(new BsonString(""));
        }
      }
      for (FieldPath filterField : filterFields) {
        FieldPath relativePath = relativizeUnderNestedRoot(filterField, sourceNestedRoot);
        valueAt(element, relativePath)
            .ifPresent(value -> addBsonValueToBsonDocument(child, relativePath, value));
      }
      childArray.add(child);
    }
    addBsonValueToBsonDocument(bsonDoc, mvNestedRoot.get(), childArray);
    for (FieldPath embedField : embedFields) {
      addBsonValueToBsonDocument(
          bsonDoc,
          getHashFieldPath(embedField, schemaMetadata.materializedViewSchemaVersion()),
          hashListPerEmbedField.get(embedField));
    }
  }

  // {@code path} relative to {@code nestedRoot} (must be a strict descendant), e.g. section_name.
  private static FieldPath relativizeUnderNestedRoot(FieldPath path, FieldPath nestedRoot) {
    String prefix = nestedRoot.toString() + FieldPath.DELIMITER;
    return FieldPath.parse(path.toString().substring(prefix.length()));
  }

  // Resolves multi-segment nestedRoot in source to its element list (empty if absent or malformed).
  private static List<BsonValue> nestedRootElements(BsonDocument source, FieldPath nestedRoot) {
    BsonValue value = valueAt(source, nestedRoot).orElse(null);
    if (value != null && value.isArray()) {
      return value.asArray().getValues();
    }
    if (value != null && value.isDocument()) {
      return List.of(value);
    }
    if (value != null) {
      LOG.warn(
          "nested auto-embed: source path '{}' is {}, expected array/document; treating as empty",
          nestedRoot,
          value.getBsonType());
    }
    return List.of();
  }

  private static Optional<BsonValue> valueAt(BsonDocument doc, FieldPath relativePath) {
    @Var BsonValue current = doc;
    for (String segment : relativePath.getSegments()) {
      if (current == null || !current.isDocument()) {
        return Optional.empty();
      }
      current = current.asDocument().get(segment);
    }
    return Optional.ofNullable(current);
  }

  // First non-empty string at relativePath, flattening one array level (one vector per element).
  private static Optional<String> firstNonEmptyStringAt(
      BsonDocument element, FieldPath relativePath) {
    BsonValue resolved = valueAt(element, relativePath).orElse(null);
    if (resolved == null) {
      return Optional.empty();
    }
    List<BsonValue> candidates =
        resolved.isArray() ? resolved.asArray().getValues() : List.of(resolved);
    return candidates.stream()
        .filter(v -> v.isString() && !v.asString().getValue().isEmpty())
        .map(v -> v.asString().getValue())
        .findFirst();
  }
}
