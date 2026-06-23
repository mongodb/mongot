package com.xgen.mongot.embedding.utils;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import static com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator.getHashFieldPath;
import static com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator.getMatViewFieldPath;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.errorprone.annotations.Var;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping.AutoEmbedField;
import com.xgen.mongot.embedding.VectorAutoEmbedFieldMapping;
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
import java.util.Iterator;
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

    ImmutableMap<FieldPath, BsonValue> collectedEmbeddingsPerMatViewPath =
        buildEmbeddingsPerMatViewPath(
            rawDocumentEvent.getDocument().get(),
            fieldMapping,
            consolidatedEmbeddingMap,
            schemaMetadata);

    BsonDocument bsonDoc = new BsonDocument();
    MaterializedViewDocumentHandler handler =
        MaterializedViewDocumentHandler.create(fieldMapping, Optional.empty(), bsonDoc);
    BsonDocumentProcessor.process(rawDocumentEvent.getDocument().get(), handler);

    for (var embeddingOrHashEntry : collectedEmbeddingsPerMatViewPath.entrySet()) {
      addBsonValueToBsonDocument(
          bsonDoc, embeddingOrHashEntry.getKey(), embeddingOrHashEntry.getValue());
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
    // Collect all autoEmbed and filter fields from source doc.
    var sourceFilterValuesCollector =
        CollectFieldValueDocumentHandler.create(mappings, Optional.empty(), (path) -> true, true);

    // Collect all fields from mat view doc, even if they are not in the index definition.
    var matViewFilterValuesCollector =
        CollectFieldValueDocumentHandler.create(
            matViewMappings, Optional.empty(), (path) -> true, false);

    @Var boolean needsReIndexing = false;
    var reusableEmbeddingsBuilder = ImmutableMap.<FieldPath, ImmutableMap<String, Vector>>builder();

    try {
      BsonDocumentProcessor.process(sourceDoc, sourceFilterValuesCollector);
      BsonDocumentProcessor.process(matViewDoc, matViewFilterValuesCollector);

      var sourceDocValues = sourceFilterValuesCollector.getCollectedValues();
      var matViewValues = matViewFilterValuesCollector.getCollectedValues();

      for (Map.Entry<FieldPath, AutoEmbedField> entry : mappings.fieldMap().entrySet()) {
        FieldPath sourceFieldPath = entry.getKey();
        FieldPath matViewFieldPath =
            getMatViewFieldPath(sourceFieldPath, schemaMetadata.autoEmbeddingFieldsMapping());
        FieldPath matViewFieldHashPath =
            getHashFieldPath(sourceFieldPath, schemaMetadata.materializedViewSchemaVersion());
        AutoEmbedField field = entry.getValue();

        // Check filter values = here we do a simple equality check.
        if (field instanceof AutoEmbedField.PassthroughField) {
          // Still use matViewFieldPath for filter as we store filter field as is.
          if (!Objects.equals(
              sourceDocValues.get(sourceFieldPath), matViewValues.get(matViewFieldPath))) {
            needsReIndexing = true;
          }
        }

        var reusableEmbeddingsForFieldBuilder = ImmutableMap.<String, Vector>builder();

        // TODO(CLOUDP-380567): Only checks when modelConfig hash matches when resyncing across
        // redefinitions.
        // check embeddings against their hashes. Collect ones that match for re-use.
        if (field instanceof AutoEmbedField.EmbedField) {
          if (sourceDocValues.containsKey(sourceFieldPath)) {
            var stringValuesWithHashes =
                sourceDocValues.get(sourceFieldPath).stream()
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
            reusableEmbeddingsBuilder.put(sourceFieldPath, reusableEmbeddingsForField);
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
   *   <li>The mapping is a {@link VectorAutoEmbedFieldMapping} — the only index type routed here;
   *       other mappings (e.g. dynamic search) always fall back.
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
    if (!(fieldMapping instanceof VectorAutoEmbedFieldMapping)) {
      // The destructive-touch check below requires fieldMap() to enumerate every indexed path.
      // That holds for vector mappings; SearchIndexAutoEmbedFieldMapping's dynamic mode
      // intentionally under-enumerates, which would let a destructive update misclassify as
      // partial-safe and leave stale values on the MV. Unsupported mappings degrade to a full
      // replace rather than risking staleness.
      return PartialEmbedClassification.fellBack(
          PartialEmbedFallbackReason.UNSUPPORTED_FIELD_MAPPING);
    }

    Set<FieldPath> declaredEmbedPaths = fieldMapping.embedFields().keySet();
    // All declared indexed paths — embeds plus filters. Complete for vector indexes (the only
    // index type routed through this classifier); SearchIndexAutoEmbedFieldMapping's dynamic mode
    // under-enumerates fieldMap() and must not be routed here without revisiting this.
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

    ImmutableSet<FieldPath> changedEmbedPaths = changedEmbedPathsBuilder.build();
    Verify.verify(
        !changedEmbedPaths.isEmpty(),
        "computePartialEmbedChangedTexts called with no embed-field touch; "
            + "caller must gate on requiresEmbeddingGeneration");

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
      MaterializedViewSchemaMetadata schemaMetadata)
      throws IOException {
    CollectFieldValueDocumentHandler collectTextValuesHandler =
        CollectFieldValueDocumentHandler.create(
            mappings,
            Optional.empty(),
            path -> mappings.isEmbed(path) && consolidatedEmbeddingMap.containsKey(path),
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
}
