package com.xgen.mongot.index.lucene.document.single;

import static com.google.common.flogger.LazyArgs.lazy;
import static com.xgen.mongot.util.Check.checkState;

import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.StringFacetFieldDefinition;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.index.lucene.extension.KnnByteVectorField;
import com.xgen.mongot.index.lucene.extension.KnnFloatVectorField;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.util.AnalyzedText;
import com.xgen.mongot.index.lucene.util.FieldTypeBuilder;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.path.string.StringMultiFieldPath;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.LoggableIdUtils;
import com.xgen.mongot.util.bson.ByteUtils;
import com.xgen.mongot.util.bson.Vector;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.FacetsConfig.DrillDownTermsIndexing;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

/**
 * Stateless class containing helper methods to add {@link IndexableField}s to Lucene documents via
 * {@link AbstractDocumentWrapper} given field names and values.
 */
public class IndexableFieldFactory {

  /**
   * Value in the nullness field for documents that have a value for the corresponding sort field.  
   */
  static final long NULLNESS_FIELD_PRESENT = 0L;

  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  private static final FieldType DOCUMENT_ID_FIELD_TYPE =
      new FieldTypeBuilder()
          .withIndexOptions(IndexOptions.DOCS)
          .tokenized(false)
          .stored(true)
          .build();

  private static final FieldType AUTOCOMPLETE_FIELD_TYPE =
      new FieldTypeBuilder()
          .withIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
          .tokenized(true)
          .stored(true)
          .build();

  private static final FieldType OBJECT_ID_FIELD_TYPE =
      new FieldTypeBuilder()
          .withIndexOptions(IndexOptions.DOCS)
          .tokenized(false)
          .stored(false)
          .build();

  private static final FieldType FIELD_NAME_FIELD_TYPE_NOT_STORED =
      new FieldTypeBuilder()
          .withIndexOptions(IndexOptions.DOCS)
          .tokenized(false)
          .stored(false)
          .build();

  private static final FieldType STORED_SOURCE_FIELD_TYPE =
      new FieldTypeBuilder()
          .withIndexOptions(IndexOptions.NONE)
          .tokenized(false)
          .stored(true)
          .omitNorms(true)
          .build();

  private static final FieldType EMBEDDED_PATH_FIELD_TYPE =
      new FieldTypeBuilder()
          .withIndexOptions(IndexOptions.DOCS)
          .tokenized(false)
          .stored(false)
          .omitNorms(true)
          .build();

  private static final FieldType EMBEDDED_ROOT_FIELD_TYPE =
      new FieldTypeBuilder()
          .withIndexOptions(IndexOptions.DOCS)
          .tokenized(false)
          .stored(false)
          .omitNorms(true)
          .build();


  /**
   * Adds an IndexableField for the `_id` to the root Lucene document for a BSON document we're
   * indexing.
   *
   * @param document - the document wrapper to add the document id field to
   * @param encodedDocumentId - an invertible encoding of the BsonDocument's _id field. Must be less
   *     than IndexWriter.MAX_TERM_LENGTH.
   * @param includeDocValue - true if we should store _id in a docValue in addition to the index +
   *     stored fields
   */
  static void addDocumentIdField(
      AbstractDocumentWrapper document, byte[] encodedDocumentId, boolean includeDocValue) {
    BytesRef value = new BytesRef(encodedDocumentId);
    document.put(
        new StoredField(
            FieldName.MetaField.ID.getLuceneFieldName(), value, DOCUMENT_ID_FIELD_TYPE));
    if (includeDocValue) {
      document.put(new SortedDocValuesField(FieldName.MetaField.ID.getLuceneFieldName(), value));
    }
  }

  /**
   * Adds an IndexableField for the customVectorEngine id to the root Lucene document for a BSON
   * document we're indexing.
   *
   * @param document - the document wrapper to add the document id field to
   * @param customVectorEngineId - custom vector engine id
   */
  static void addCustomVectorEngineIdField(
      AbstractDocumentWrapper document, long customVectorEngineId) {
    String fieldName = FieldName.MetaField.CUSTOM_VECTOR_ENGINE_ID.getLuceneFieldName();
    document.put(new LongPoint(fieldName, customVectorEngineId));
    document.put(new NumericDocValuesField(fieldName, customVectorEngineId));
  }

  static void addEmbeddedPathField(AbstractDocumentWrapper document) {
    IndexableField indexableField =
        new Field(
            FieldName.MetaField.EMBEDDED_PATH.getLuceneFieldName(),
            document.getEmbeddedRoot().map(FieldPath::toString).orElse(""),
            EMBEDDED_PATH_FIELD_TYPE);
    document.put(indexableField);
  }

  static void addEmbeddedRootField(AbstractDocumentWrapper document) {
    IndexableField indexableField =
        new Field(
            FieldName.MetaField.EMBEDDED_ROOT.getLuceneFieldName(),
            FieldValue.EMBEDDED_ROOT_FIELD_VALUE,
            EMBEDDED_ROOT_FIELD_TYPE);
    document.put(indexableField);
  }

  // Add path to metadata keeping track of the field names we have indexed for this lucene document.
  static void addFieldNamesField(AbstractDocumentWrapper document, FieldPath path) {
    if (!document.getIndexCapabilities().supportsFieldExistsQuery()) {
      return;
    }

    String pathString = path.toString();

    if (pathString.length() > LuceneConfig.MAX_TERM_CHAR_LENGTH) {
      FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
          "Unable to index a field name due to its length= %d > maximum allowed=%d. "
              + "path=%s. _id=%s...",
          pathString.length(),
          LuceneConfig.MAX_TERM_CHAR_LENGTH,
          pathString.substring(0, 20),
          lazy(() -> getLoggingId(document)));
      return;
    }

    IndexableField indexableField =
        new Field(
            FieldName.MetaField.FIELD_NAMES.getLuceneFieldName(),
            path.toString(),
            FIELD_NAME_FIELD_TYPE_NOT_STORED);

    document.put(indexableField);
  }

  static void addStringField(
      AbstractDocumentWrapper document,
      FieldPath fieldPath,
      String value,
      StringFieldDefinition fieldDefinition) {
    String luceneFieldName =
        FieldName.TypeField.STRING.getLuceneFieldName(fieldPath, document.getEmbeddedRoot());
    addSingleStringField(
        document,
        luceneFieldName,
        value,
        fieldDefinition.indexOptions(),
        fieldDefinition.storeFlag(),
        fieldDefinition.norms());

    // handle multi fields
    for (Map.Entry<String, StringFieldDefinition> entry : fieldDefinition.multi().entrySet()) {
      String multiName = entry.getKey();
      StringFieldDefinition multiDefinition = entry.getValue();

      // Don't store the multi field if the base field is already stored.
      addSingleStringField(
          document,
          FieldName.MultiField.getLuceneFieldName(
              new StringMultiFieldPath(fieldPath, multiName), document.getEmbeddedRoot()),
          value,
          multiDefinition.indexOptions(),
          multiDefinition.storeFlag() && !fieldDefinition.storeFlag(),
          multiDefinition.norms());
    }
  }

  private static void addSingleStringField(
      AbstractDocumentWrapper document,
      String luceneFieldName,
      String value,
      StringFieldDefinition.IndexOptions indexOptions,
      boolean store,
      StringFieldDefinition.NormsOptions norms) {

    FieldType fieldType =
        new FieldTypeBuilder()
            .withIndexOptions(indexOptions)
            .tokenized(true)
            .stored(store)
            .omitNorms(norms == StringFieldDefinition.NormsOptions.OMIT)
            .build();

    IndexableField indexableField = new Field(luceneFieldName, value, fieldType);
    document.put(indexableField);
  }

  static void addStringFacetField(AbstractDocumentWrapper document, FieldPath path, String value) {
    if (value.isEmpty()) {
      // empty labels are not allowed for facet indexing
      return;
    }
    String dimension = path.toString();

    // Indexed facet value will be `${path}.${value}`
    int maxValueLength = FacetLabel.MAX_CATEGORY_PATH_LENGTH - 1 - dimension.length();

    if (value.length() > maxValueLength) {
      FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
          "Truncating value for stringFacet field `%s` because it exceeds limit of %s UTF-16 chars."
              + " _id=%s",
          path, FacetLabel.MAX_CATEGORY_PATH_LENGTH, lazy(() -> getLoggingId(document)));
      // Take prefix of value. If last char is a high surrogate, Lucene will replace it with 0xFFFD
      String truncatedValue = value.substring(0, maxValueLength);
      document.put(new SortedSetDocValuesFacetField(dimension, truncatedValue));
    } else {
      document.put(new SortedSetDocValuesFacetField(dimension, value));
    }
  }

  /**
   * Adds sortable string field(s) to the document based on the field definition.
   *
   * <p>This method handles both sortableStringBetaV1 and token field definitions. It validates that
   * exactly one of these definitions is present, truncates the string value if necessary, and adds
   * the appropriate field(s) to the document.
   *
   * @param document - the document wrapper to add the field to
   * @param fieldDefinition - the field definition containing sortable string configuration
   * @param path - the field path
   * @param stringValue - the string value to index
   * @throws IOException if an I/O error occurs during analysis
   */
  static void addSortableStringField(
      AbstractDocumentWrapper document,
      FieldDefinition fieldDefinition,
      FieldPath path,
      String stringValue)
      throws IOException {
    if (fieldDefinition.sortableStringBetaV1FieldDefinition().isEmpty()
        && fieldDefinition.tokenFieldDefinition().isEmpty()) {
      return;
    }

    checkState(
        Stream.of(
                    fieldDefinition.tokenFieldDefinition(),
                    fieldDefinition.sortableStringBetaV1FieldDefinition())
                .filter(Optional::isPresent)
                .count()
            == 1,
        "Exactly one sortable definition should be present for Strings");

    // SortedSetDocValues requires the BytesRef passed in be <= 32766 bytes. We conservatively
    // truncate the string to MAX_TERM_CHAR_LENGTH to ensure that we're truncating on a character
    // boundary rather than potentially in the middle of a code point.
    int end = Math.min(stringValue.length(), LuceneConfig.MAX_TERM_CHAR_LENGTH);
    if (end != stringValue.length()) {
      document.indexingMetricsUpdater.getSortableStringTruncatedCounter().increment();
      FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
          "Unable to fully index sortable string value "
              + "due to its length=%d > maximum allowed=%d. Truncating to %d characters. "
              + "path=%s. _id=%s",
          stringValue.length(),
          LuceneConfig.MAX_TERM_CHAR_LENGTH,
          LuceneConfig.MAX_TERM_CHAR_LENGTH,
          path,
          lazy(() -> getLoggingId(document)));
    }

    String truncatedValue = stringValue.substring(0, end);

    if (fieldDefinition.sortableStringBetaV1FieldDefinition().isPresent()) {
      checkState(
          document.getEmbeddedRoot().isEmpty(),
          "sortableStringBetaV1 is disallowed in embeddedDocuments.");
      String luceneFieldPath =
          FieldName.TypeField.SORTABLE_STRING_BETA_V1.getLuceneFieldName(path, Optional.empty());
      addSortableStringField(document, luceneFieldPath, new BytesRef(truncatedValue));
      return;
    }

    String luceneFieldPath =
        FieldName.TypeField.TOKEN.getLuceneFieldName(path, document.getEmbeddedRoot());
    addTokenFieldWithAnalyzer(document, luceneFieldPath, truncatedValue);
  }

  private static void addSortableStringField(
      AbstractDocumentWrapper document, String luceneFieldName, BytesRef value) {
    document.put(new SortedSetDocValuesField(luceneFieldName, value));

    // Add to the postings list to take advantage of pruning (LUCENE-10633).
    document.put(new StringField(luceneFieldName, value, Field.Store.NO));
  }

  private static void addTokenFieldWithAnalyzer(
      AbstractDocumentWrapper document,
      String luceneFieldName,
      String value)
      throws IOException {
    Analyzer indexAnalyzer = document.getIndexAnalyzer().orElseThrow();
    BytesRef analyzedValue =
        new BytesRef(
            AnalyzedText.applyTokenFieldTypeNormalizer(luceneFieldName, indexAnalyzer, value));
    addSortableStringField(document, luceneFieldName, analyzedValue);
  }

  static void addTokenField(
      AbstractDocumentWrapper document, FieldPath path, String analyzedString) {
    String luceneFieldName =
        FieldName.TypeField.TOKEN.getLuceneFieldName(path, document.getEmbeddedRoot());
    addSortableStringField(document, luceneFieldName, new BytesRef(analyzedString));
  }

  static void addAutocompleteField(AbstractDocumentWrapper document, FieldPath path, String value) {
    String luceneFieldName =
        FieldName.TypeField.AUTOCOMPLETE.getLuceneFieldName(path, document.getEmbeddedRoot());
    document.put(new Field(luceneFieldName, value, AUTOCOMPLETE_FIELD_TYPE));
  }

  static void addDateField(
      AbstractDocumentWrapper document, FieldPath path, long value, boolean isMultiValued) {
    Optional<FieldPath> embeddedRoot = document.getEmbeddedRoot();
    if (isMultiValued) {
      addDateMultipleField(
          document,
          FieldName.TypeField.DATE_MULTIPLE.getLuceneFieldName(path, embeddedRoot),
          value);
    } else {
      addDateSingleField(
          document, FieldName.TypeField.DATE.getLuceneFieldName(path, embeddedRoot), value);
    }
  }

  static void addDateFacetField(AbstractDocumentWrapper document, FieldPath path, long value) {
    addDateSingleField(
        document, FieldName.TypeField.DATE_FACET.getLuceneFieldName(path, Optional.empty()), value);
  }

  private static void addDateSingleField(
      AbstractDocumentWrapper document, String luceneFieldName, long value) {
    document.put(new NumericDocValuesField(luceneFieldName, value));
    document.put(new LongPoint(luceneFieldName, value));
  }

  private static void addDateMultipleField(
      AbstractDocumentWrapper document, String luceneFieldName, long value) {
    document.put(new LongPoint(luceneFieldName, value));
  }

  static void addSortableDateBetaField(
      AbstractDocumentWrapper document, FieldPath path, long value) {
    String luceneFieldName =
        FieldName.TypeField.SORTABLE_DATE_BETA_V1.getLuceneFieldName(path, Optional.empty());

    addSortableDateField(document, luceneFieldName, value);
  }

  static void addSortableDateV2Field(AbstractDocumentWrapper document, FieldPath path, long value) {
    String luceneFieldName =
        FieldName.TypeField.DATE_V2.getLuceneFieldName(path, document.getEmbeddedRoot());
    addSortableDateField(document, luceneFieldName, value);
  }

  /**
   * Adds a nullness indicator field for a sort field. This field is written with value {@link
   * #NULLNESS_FIELD_PRESENT} for documents that have the corresponding sort field value. Documents
   * missing the sort field will not have this nullness field, and the index sort's missing value
   * setting will handle their ordering.
   */
  static void addNullnessField(AbstractDocumentWrapper document, FieldPath path) {
    String luceneFieldName =
        FieldName.getNullnessFieldName(path);
    document.put(new SortedNumericDocValuesField(luceneFieldName, NULLNESS_FIELD_PRESENT));
  }

  private static void addSortableDateField(
      AbstractDocumentWrapper document, String luceneFieldName, long value) {
    document.put(new LongField(luceneFieldName, value, Field.Store.NO));
  }

  static void addBooleanField(AbstractDocumentWrapper document, FieldPath path, boolean value) {
    String luceneFieldName =
        FieldName.TypeField.BOOLEAN.getLuceneFieldName(path, document.getEmbeddedRoot());
    String fieldValue = FieldValue.fromBoolean(value);
    document.put(new StringField(luceneFieldName, fieldValue, Field.Store.NO));
    if (document.getIndexCapabilities().supportsObjectIdAndBooleanDocValues()) {
      document.put(new SortedSetDocValuesField(luceneFieldName, new BytesRef(fieldValue)));
    }
  }

  /**
   * Adds an ObjectId field to the supplied Lucene document wrapper.
   *
   * @param document - the document wrapper to add the object id field to
   * @param path - the field path
   * @param value - the object id value to add
   */
  public static void addObjectIdField(
      AbstractDocumentWrapper document, FieldPath path, ObjectId value) {
    String luceneFieldName =
        FieldName.TypeField.OBJECT_ID.getLuceneFieldName(path, document.getEmbeddedRoot());
    var bytesRef = new BytesRef(value.toByteArray());
    document.put(new Field(luceneFieldName, bytesRef, OBJECT_ID_FIELD_TYPE));
    if (document.getIndexCapabilities().supportsObjectIdAndBooleanDocValues()) {
      document.put(new SortedSetDocValuesField(luceneFieldName, bytesRef));
    }
  }

  /**
   * Adds KnnVectorField to the supplied Lucene document wrapper.
   *
   * @param document - the document wrapper to add the knn vector field to
   * @param field - the field path of the field to add the knn vector to
   * @param vector - the vector to add
   * @param specification - the vector field specification containing dimensions, similarity, and
   *     quantization
   */
  public static void addKnnVectorField(
      AbstractDocumentWrapper document,
      FieldPath field,
      Vector vector,
      VectorFieldSpecification specification) {
    // This is the field name we use to check for duplicate vectors and invalid vectors. The
    // embeddedRoot is set to be empty when we are retrieving the field name for this purpose.
    String checkFieldName =
        FieldName.TypeField.KNN_VECTOR.getLuceneFieldName(field, Optional.empty());

    // Skip vectors of invalid size to not fail indexing.
    // Note: we need to do duplicate and validity check before the dimension check and similarity
    // method check since we do not want to index any following vectors if any of the previous
    // vectors were either already indexed or shown as invalid for indexing.
    if (!document.canIndexVectorField(checkFieldName)) {
      FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
          "nested embeddings are already indexed or invalid: "
              + "only first valid vector field with lucene field name %s is indexed. _id=%s",
          checkFieldName, lazy(() -> getLoggingId(document)));
      return;
    }

    int dimensions = specification.numDimensions();
    VectorSimilarity similarity = specification.similarity();
    VectorQuantization quantization = specification.quantization();

    if (vector.numDimensions() != dimensions) {
      FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
          "Vector dimension mismatch: expected %d, got %d. Skipping. _id=%s",
          dimensions, vector.numDimensions(), lazy(() -> getLoggingId(document)));
      document.markVectorFieldInvalid(checkFieldName);
      return;
    }

    if (similarity == VectorSimilarity.COSINE && vector.isZeroVector()) {
      FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
          "Cosine similarity does not support zero vectors. Skipping. _id=%s",
          lazy(() -> getLoggingId(document)));
      document.markVectorFieldInvalid(checkFieldName);
      return;
    }

    VectorSimilarityFunction similarityFunction = similarity.getLuceneSimilarityFunction();

    try {
      switch (vector.getVectorType()) {
        case FLOAT -> {
          String floatVectorFieldName =
              quantization.toTypeField().getLuceneFieldName(field, document.getEmbeddedRoot());
          document.addVectorField(
              new KnnFloatVectorField(
                  floatVectorFieldName,
                  vector.asFloatVector().getFloatVector(),
                  similarityFunction),
              checkFieldName);
        }
        case BYTE -> {
          if (isQuantizationEnabled(quantization)) {
            FLOGGER.atWarning().atMostEvery(10, TimeUnit.MINUTES).log(
                "Quantization is only supported for FLOAT vectors but found BYTE vector, "
                    + "skipping. _id=%s",
                lazy(() -> getLoggingId(document)));
            return;
          }
          String byteVectorFieldName =
              FieldName.TypeField.KNN_BYTE.getLuceneFieldName(field, document.getEmbeddedRoot());
          document.addVectorField(
              new KnnByteVectorField(
                  byteVectorFieldName, vector.asByteVector().getByteVector(), similarityFunction),
              checkFieldName);
        }
        case BIT -> {
          if (isQuantizationEnabled(quantization)) {
            FLOGGER.atWarning().atMostEvery(10, TimeUnit.MINUTES).log(
                "Quantization is only supported for FLOAT vectors but found BIT vector, "
                    + "skipping. _id=%s",
                lazy(() -> getLoggingId(document)));
            return;
          }
          if (similarity != VectorSimilarity.EUCLIDEAN) {
            FLOGGER.atWarning().atMostEvery(10, TimeUnit.MINUTES).log(
                "Cannot index binary vector with %s similarity, skipping. _id=%s",
                similarity, lazy(() -> getLoggingId(document)));
            return;
          }
          String bitVectorFieldName =
              FieldName.TypeField.KNN_BIT.getLuceneFieldName(field, document.getEmbeddedRoot());
          document.addVectorField(
              new KnnByteVectorField(
                  bitVectorFieldName, vector.asBitVector().getBitVector(), similarityFunction),
              checkFieldName);
        }
      }
    } catch (IllegalArgumentException e) {
      FLOGGER.atWarning().atMostEvery(1, TimeUnit.HOURS).withCause(e).log(
          "Unable to create vector field: %s. _id=%s",
          e.getMessage(), lazy(() -> getLoggingId(document)));
      return;
    }
  }

  /**
   * Adds a stored source field to the supplied Lucene document wrapper.
   *
   * @param document - the document wrapper to add the stored source field to
   * @param stored - the stored source to add
   */
  public static void addStoredSourceField(AbstractDocumentWrapper document, BsonDocument stored) {
    IndexableField field =
        new Field(
            FieldName.StaticField.STORED_SOURCE.getLuceneFieldName(),
            ByteUtils.toByteArray(stored),
            STORED_SOURCE_FIELD_TYPE);
    document.put(field);
  }

  static void addUuidField(AbstractDocumentWrapper document, FieldPath path, UUID uuid) {
    String luceneFieldName =
        FieldName.TypeField.UUID.getLuceneFieldName(path, document.getEmbeddedRoot());
    String uuidString = uuid.toString();
    document.put(new StringField(luceneFieldName, uuidString, Field.Store.NO));
    document.put(new SortedSetDocValuesField(luceneFieldName, new BytesRef(uuidString)));
  }

  static void addNullField(AbstractDocumentWrapper document, FieldPath path) {
    String luceneFieldName =
        FieldName.TypeField.NULL.getLuceneFieldName(path, document.getEmbeddedRoot());
    var res =
        document.addNullFieldIfAbsent(
            new StringField(luceneFieldName, FieldValue.NULL_FIELD_VALUE, Field.Store.NO),
            luceneFieldName);
    if (res) {
      document.put(
          new SortedDocValuesField(luceneFieldName, new BytesRef(FieldValue.NULL_FIELD_VALUE)));
    }
  }

  public static Optional<FacetsConfig> createFacetsConfig(SearchIndexDefinition indexDefinition) {
    Set<Map.Entry<String, StringFacetFieldDefinition>> facetFields =
        indexDefinition.getStaticFieldDefinitionsOfType(
            FieldDefinition::stringFacetFieldDefinition);
    if (facetFields.isEmpty()) {
      return Optional.empty();
    }

    FacetsConfig config = new FacetsConfig();
    facetFields.stream().map(Map.Entry::getKey).forEach(field -> addFacetField(config, field));
    return Optional.of(config);
  }

  private static void addFacetField(FacetsConfig config, String field) {
    // enabled to match existing array search indexing behavior:
    config.setMultiValued(field, true);
    // enabled to support future drilldown functionality without re-indexing:
    config.setDrillDownTermsIndexing(field, DrillDownTermsIndexing.ALL);
    // disabled, because current faceting mechanism does not support hierarchical
    // dimensions:
    config.setHierarchical(field, false);
  }

  static void addIntegralValueToNumericField(
      AbstractDocumentWrapper document,
      FieldPath path,
      long value,
      NumericFieldOptions.Representation representation,
      boolean isMultiValued) {
    String luceneFieldName =
        numberFieldName(representation, path, document.getEmbeddedRoot(), isMultiValued);
    if (isMultiValued) {
      addNumericMultipleField(
          document, luceneFieldName, numericIndexedValue(representation, value));
    } else {
      addNumericSingleField(document, luceneFieldName, numericIndexedValue(representation, value));
    }
  }

  static void addFloatingPointValueToNumericField(
      AbstractDocumentWrapper document,
      FieldPath path,
      double value,
      NumericFieldOptions.Representation representation,
      boolean isMultiValued) {
    String luceneFieldName =
        numberFieldName(representation, path, document.getEmbeddedRoot(), isMultiValued);
    if (isMultiValued) {
      addNumericMultipleField(
          document, luceneFieldName, numericIndexedValue(representation, value));
    } else {
      addNumericSingleField(document, luceneFieldName, numericIndexedValue(representation, value));
    }
  }

  static void addIntegralNumericFacetField(
      AbstractDocumentWrapper document,
      FieldPath path,
      long value,
      NumericFieldOptions.Representation representation) {
    addNumericFacetField(
        document,
        numberFacetFieldName(representation, path, Optional.empty()),
        numericIndexedValue(representation, value));
  }

  static void addFloatingPointNumericFacetField(
      AbstractDocumentWrapper document,
      FieldPath path,
      double value,
      NumericFieldOptions.Representation representation) {
    addNumericFacetField(
        document,
        numberFacetFieldName(representation, path, Optional.empty()),
        numericIndexedValue(representation, value));
  }

  static void addIntegralValueToSortBetaNumericField(
      AbstractDocumentWrapper document, FieldPath path, long value) {
    String luceneName = sortableNumberBetaFieldName(path);
    long indexedValue = sortBetaNumericIndexedValue(value);
    document.put(new LongField(luceneName, indexedValue, Field.Store.NO));
  }

  static void addFloatingPointValueToSortBetaNumericField(
      AbstractDocumentWrapper document, FieldPath path, double value) {
    String luceneName = sortableNumberBetaFieldName(path);
    long indexedValue = sortBetaNumericIndexedValue(value);
    document.put(new LongField(luceneName, indexedValue, Field.Store.NO));
  }

  static void addIntegralValueToSortableNumericField(
      AbstractDocumentWrapper document,
      FieldPath path,
      long value,
      NumericFieldOptions.Representation representation) {
    String fieldName = sortableNumberFieldName(representation, path, document.getEmbeddedRoot());
    document.put(
        new LongField(
            fieldName, sortableNumericIndexedValue(representation, value), Field.Store.NO));
  }

  static void addFloatingPointValueToSortableNumericField(
      AbstractDocumentWrapper document,
      FieldPath path,
      double value,
      NumericFieldOptions.Representation representation) {
    String fieldName = sortableNumberFieldName(representation, path, document.getEmbeddedRoot());
    document.put(
        new LongField(
            fieldName, sortableNumericIndexedValue(representation, value), Field.Store.NO));
  }

  private static void addNumericSingleField(
      AbstractDocumentWrapper document, String luceneFieldName, long value) {
    document.put(new NumericDocValuesField(luceneFieldName, value));
    document.put(new LongPoint(luceneFieldName, value));
  }

  private static void addNumericMultipleField(
      AbstractDocumentWrapper document, String luceneFieldName, long value) {
    document.put(new LongPoint(luceneFieldName, value));
  }

  private static void addNumericFacetField(
      AbstractDocumentWrapper document, String luceneFieldName, long value) {
    document.put(new NumericDocValuesField(luceneFieldName, value));
  }

  private static String numberFieldName(
      NumericFieldOptions.Representation representation,
      FieldPath path,
      Optional<FieldPath> embeddedRoot,
      boolean isMultiValued) {
    return switch (representation) {
      case INT64 -> {
        FieldName.TypeField int64TypeField =
            isMultiValued
                ? FieldName.TypeField.NUMBER_INT64_MULTIPLE
                : FieldName.TypeField.NUMBER_INT64;
        yield int64TypeField.getLuceneFieldName(path, embeddedRoot);
      }
      case DOUBLE -> {
        FieldName.TypeField doubleTypeField =
            isMultiValued
                ? FieldName.TypeField.NUMBER_DOUBLE_MULTIPLE
                : FieldName.TypeField.NUMBER_DOUBLE;
        yield doubleTypeField.getLuceneFieldName(path, embeddedRoot);
      }
    };
  }

  private static String numberFacetFieldName(
      NumericFieldOptions.Representation representation,
      FieldPath path,
      Optional<FieldPath> embeddedRoot) {
    return switch (representation) {
      case INT64 -> FieldName.TypeField.NUMBER_INT64_FACET.getLuceneFieldName(path, embeddedRoot);
      case DOUBLE -> FieldName.TypeField.NUMBER_DOUBLE_FACET.getLuceneFieldName(path, embeddedRoot);
    };
  }

  private static String sortableNumberFieldName(
      NumericFieldOptions.Representation representation,
      FieldPath path,
      Optional<FieldPath> embeddedRoot) {
    return switch (representation) {
      case INT64 -> FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(path, embeddedRoot);
      case DOUBLE -> FieldName.TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(path, embeddedRoot);
    };
  }

  private static String sortableNumberBetaFieldName(FieldPath path) {
    return FieldName.TypeField.SORTABLE_NUMBER_BETA_V1.getLuceneFieldName(path, Optional.empty());
  }

  static long numericIndexedValue(NumericFieldOptions.Representation representation, double value) {
    return switch (representation) {
      case INT64 -> (long) value;
      case DOUBLE -> LuceneDoubleConversionUtils.toLong(value);
    };
  }

  static long numericIndexedValue(NumericFieldOptions.Representation representation, long value) {
    return switch (representation) {
      case INT64 -> value;
      case DOUBLE -> LuceneDoubleConversionUtils.toIndexedLong(value);
    };
  }

  /**
   * Depending on the representation configured the double value will either be converted to long
   * losslessly via DoubleConversionUtils or cast to long and indexed into Lucene. When casting the
   * double directly into long note the behavior of the following edge cases:
   *
   * <ul>
   *   <li>-Infinity: When cast into long -Inf will be collapsed into Long.MIN_VALUE
   *   <li>Infinity: When cast into long +Inf will be collapsed into Long.MAX_VALUE
   *   <li>NaN: NaN is special cased to be collapsed into Long.MIN_VALUE in order to stay as
   *       consistent as possible with MongoDB NaN ordering
   *   <li>Any number outside of the range of {Long.MIN_VALUE, Long.MAX_VALUE} will be collapsed
   *       into the corresponding
   * </ul>
   */
  static long sortableNumericIndexedValue(
      NumericFieldOptions.Representation representation, double value) {
    return switch (representation) {
      case INT64 -> Double.isNaN(value) ? Long.MIN_VALUE : (long) value;
      case DOUBLE -> LuceneDoubleConversionUtils.toMqlSortableLong(value);
    };
  }

  static long sortableNumericIndexedValue(
      NumericFieldOptions.Representation representation, long value) {
    return switch (representation) {
      case INT64 -> value;
      case DOUBLE -> LuceneDoubleConversionUtils.toMqlIndexedLong(value);
    };
  }

  static long sortBetaNumericIndexedValue(double value) {
    // Don't support representation for sortable numbers, so always index as a double.
    return LuceneDoubleConversionUtils.toMqlSortableLong(value);
  }

  static long sortBetaNumericIndexedValue(long value) {
    // Don't support representation for sortable numbers, so always index as a double.
    return LuceneDoubleConversionUtils.toMqlIndexedLong(value);
  }

  private static boolean isQuantizationEnabled(VectorQuantization quantization) {
    return quantization != VectorQuantization.NONE;
  }

  /**
   * Returns the logging ID of the document wrapper.
   * 
   * <p>If the document wrapper does not have a root ID or fails to get the root ID, do not throw
   * and returns "unknown".
   *
   * @param document - the document wrapper to get the logging ID of
   * @return the logging ID of the document wrapper
   */
  static String getLoggingId(AbstractDocumentWrapper document) {
    try {
      BsonValue rootId = document.getRootId();
      return LoggableIdUtils.getLoggableId(rootId);
    } catch (AssertionError e) {
      return LoggableIdUtils.UNKNOWN_LOGGABLE_ID;
    }
  }
}
