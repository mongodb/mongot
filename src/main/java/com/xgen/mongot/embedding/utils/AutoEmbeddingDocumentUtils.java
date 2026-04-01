package com.xgen.mongot.embedding.utils;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingIndexDefinitionUtils.getHashFieldPath;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingIndexDefinitionUtils.getMatViewFieldPath;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.errorprone.annotations.Var;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.ingestion.BsonDocumentProcessor;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.BsonVectorParser;
import com.xgen.mongot.util.bson.Vector;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
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
   * Extracts string field values from rawBsonDocument based on VectorIndexFieldMappings (from index
   * definition), only processes string field paths defined in VectorTextFieldDefinition. Text
   * strings are mapped by field path to support multi-model index. Returns a FieldPath to Strings
   * mapping for auto-embedding calls.
   */
  public static ImmutableMap<FieldPath, Set<String>> getVectorTextPathMap(
      RawBsonDocument rawBsonDocument, VectorIndexFieldMapping fieldMapping) throws IOException {
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
      VectorIndexFieldMapping fieldMapping,
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
      VectorIndexDefinition vectorIndexDefinition,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddingsPerField,
      MaterializedViewSchemaMetadata schemaMetadata)
      throws IOException {
    return buildMaterializedViewDocumentEvent(
        rawDocumentEvent, vectorIndexDefinition.getMappings(), embeddingsPerField, schemaMetadata);
  }

  private static DocumentEvent buildMaterializedViewDocumentEvent(
      DocumentEvent rawDocumentEvent,
      VectorIndexFieldMapping fieldMapping,
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
    var filteredMapping =
        VectorIndexFieldMapping.create(
            fieldMapping.fieldMap().values().stream()
                .filter(field -> field.getType() == VectorIndexFieldDefinition.Type.FILTER)
                .toList(),
            fieldMapping.nestedRoot());
    MaterializedViewDocumentHandler handler =
        MaterializedViewDocumentHandler.create(filteredMapping, Optional.empty(), bsonDoc);
    BsonDocumentProcessor.process(rawDocumentEvent.getDocument().get(), handler);

    for (var embeddingOrHashEntry : collectedEmbeddingsPerMatViewPath.entrySet()) {
      addBsonValueToBsonDocument(
          bsonDoc, embeddingOrHashEntry.getKey(), embeddingOrHashEntry.getValue());
    }

    return DocumentEvent.createFromDocumentEvent(
        rawDocumentEvent, new RawBsonDocument(bsonDoc, BsonUtils.BSON_DOCUMENT_CODEC));
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
      VectorIndexFieldMapping mappings,
      VectorIndexFieldMapping matViewMappings,
      MaterializedViewSchemaMetadata schemaMetadata) {
    // Collect all autoEmbed and filter fields from source doc.
    var sourceFilterValuesCollector =
        CollectFieldValueDocumentHandler.create(
            mappings, Optional.empty(), (fieldDefinition) -> true, true);

    // Collect all fields from mat view doc, even if they are not in the index definition.
    var matViewFilterValuesCollector =
        CollectFieldValueDocumentHandler.create(
            matViewMappings, Optional.empty(), (fieldDefinition) -> true, false);

    @Var boolean needsReIndexing = false;
    var reusableEmbeddingsBuilder = ImmutableMap.<FieldPath, ImmutableMap<String, Vector>>builder();

    try {
      BsonDocumentProcessor.process(sourceDoc, sourceFilterValuesCollector);
      BsonDocumentProcessor.process(matViewDoc, matViewFilterValuesCollector);

      var sourceDocValues = sourceFilterValuesCollector.getCollectedValues();
      var matViewValues = matViewFilterValuesCollector.getCollectedValues();

      for (Map.Entry<FieldPath, VectorIndexFieldDefinition> entry :
          mappings.fieldMap().entrySet()) {
        FieldPath sourceFieldPath = entry.getKey();
        FieldPath matViewFieldPath =
            getMatViewFieldPath(sourceFieldPath, schemaMetadata.autoEmbeddingFieldsMapping());
        FieldPath matViewFieldHashPath =
            getHashFieldPath(sourceFieldPath, schemaMetadata.materializedViewSchemaVersion());
        VectorIndexFieldDefinition fieldDefinition = entry.getValue();

        // Check filter values = here we do a simple quality check.
        if (fieldDefinition.getType() == VectorIndexFieldDefinition.Type.FILTER) {
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
        if (fieldDefinition.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED) {
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
        if (!fieldPath.toString().equals("_id")
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
      UpdateDescription updateDescription, VectorIndexFieldMapping fieldMapping) {
    if (updateDescription == null) {
      // No update description means we can't determine what changed - require embedding to be safe
      return true;
    }

    List<String> removedFields =
        Optional.ofNullable(updateDescription.getRemovedFields()).orElseGet(Collections::emptyList);

    Set<String> updatedFields =
        Optional.ofNullable(updateDescription.getUpdatedFields())
            .map(BsonDocument::keySet)
            .orElseGet(Collections::emptySet);

    // If no fields were updated or removed, require embedding to be safe
    if (removedFields.isEmpty() && updatedFields.isEmpty()) {
      return true;
    }

    // Check if any updated field is an AUTO_EMBED or TEXT field
    for (String field : updatedFields) {
      if (fieldMapping
          .getFieldDefinition(FieldPath.parse(field))
          .filter(VectorIndexFieldDefinition::isAutoEmbedField)
          .isPresent()) {
        return true;
      }
    }

    // Check if any removed field is an AUTO_EMBED or TEXT field
    for (String field : removedFields) {
      if (fieldMapping
          .getFieldDefinition(FieldPath.parse(field))
          .filter(VectorIndexFieldDefinition::isAutoEmbedField)
          .isPresent()) {
        return true;
      }
    }

    // No AUTO_EMBED/TEXT fields changed - only filter fields were updated
    return false;
  }

  // TODO(CLOUDP-363302): Supports stored source.
  /**
   * Extracts filter field values from the full document to build a $set document for partial
   * updates. Only includes fields that are defined as FILTER in the index definition.
   *
   * @param fullDocument the full document from the change stream event
   * @param fieldMapping the vector index field mapping
   * @return a BsonDocument containing only the filter field values for use with $set
   */
  public static BsonDocument extractFilterFieldValues(
      RawBsonDocument fullDocument, VectorIndexFieldMapping fieldMapping) {
    try {
      var filterValuesCollector =
          CollectFieldValueDocumentHandler.create(
              fieldMapping,
              Optional.empty(),
              fieldDef -> fieldDef.getType() == VectorIndexFieldDefinition.Type.FILTER,
              true);

      BsonDocumentProcessor.process(fullDocument, filterValuesCollector);
      return filterValuesCollector.toBsonDocument();
    } catch (Exception e) {
      LOG.warn(
          "Failed to extract filter field values, falling back to full document processing", e);
      return new BsonDocument();
    }
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
      VectorIndexFieldMapping mappings,
      Map<FieldPath, Map<String, Vector>> consolidatedEmbeddingMap,
      MaterializedViewSchemaMetadata schemaMetadata)
      throws IOException {
    CollectFieldValueDocumentHandler collectTextValuesHandler =
        CollectFieldValueDocumentHandler.create(
            mappings,
            Optional.empty(),
            fieldDef ->
                fieldDef.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED
                    && consolidatedEmbeddingMap.containsKey(fieldDef.getPath()),
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
