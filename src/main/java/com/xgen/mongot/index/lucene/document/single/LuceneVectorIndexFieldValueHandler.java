package com.xgen.mongot.index.lucene.document.single;

import static com.google.common.flogger.LazyArgs.lazy;

import com.google.common.collect.ImmutableMap;
import com.google.common.flogger.FluentLogger;
import com.mongodb.client.model.geojson.Geometry;
import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.definition.VectorIndexVectorFieldDefinition;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.index.lucene.document.context.IndexingPolicyBuilderContext;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.bson.BsonValue;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

/** Contains logic related to indexing Lucene values that are supported by vector indexes. */
public class LuceneVectorIndexFieldValueHandler implements FieldValueHandler {

  private static final FluentLogger flogger = FluentLogger.forEnclosingClass();

  final VectorIndexDocumentWrapper documentWrapper;
  final VectorIndexFieldMapping mapping;

  final Optional<VectorIndexFieldDefinition> fieldDefinition;
  final FieldPath path;
  final IndexingPolicyBuilderContext context;

  LuceneVectorIndexFieldValueHandler(
      VectorIndexDocumentWrapper documentWrapper,
      VectorIndexFieldMapping mapping,
      FieldPath path,
      IndexingPolicyBuilderContext context) {
    this.documentWrapper = documentWrapper;
    this.mapping = mapping;
    this.path = path;
    this.fieldDefinition = mapping.getFieldDefinition(path);
    this.context = context;
  }

  @Override
  public void handleBinary(Supplier<Binary> supplier) {
    // do nothing, since UUID indexing logic is handled below in handleUuid() and all other
    // remaining types of binary data are currently not indexed
  }

  @Override
  public void handleBoolean(Supplier<Boolean> supplier) {
    if (isNotFilterField()) {
      return;
    }

    IndexableFieldFactory.addBooleanField(this.documentWrapper, this.path, supplier.get());
  }

  @Override
  public void handleDateTime(Supplier<Long> supplier) {
    if (isNotFilterField()) {
      return;
    }

    IndexableFieldFactory.addDateField(this.documentWrapper, this.path, supplier.get(), true);
    IndexableFieldFactory.addSortableDateV2Field(this.documentWrapper, this.path, supplier.get());
  }

  @Override
  public void handleDouble(Supplier<Double> supplier) {
    if (isNotFilterField()) {
      return;
    }

    IndexableFieldFactory.addFloatingPointValueToNumericField(
        this.documentWrapper,
        this.path,
        supplier.get(),
        NumericFieldOptions.Representation.DOUBLE,
        true);
    IndexableFieldFactory.addFloatingPointValueToSortableNumericField(
        this.documentWrapper, this.path, supplier.get(), NumericFieldOptions.Representation.INT64);
  }

  @Override
  public void handleGeometry(Supplier<Optional<Geometry>> supplier) {}

  @Override
  public void handleInt32(Supplier<Integer> supplier) {
    handleInt64(() -> supplier.get().longValue());
  }

  @Override
  public void handleInt64(Supplier<Long> supplier) {
    if (isNotFilterField()) {
      return;
    }

    IndexableFieldFactory.addIntegralValueToNumericField(
        this.documentWrapper,
        this.path,
        supplier.get(),
        NumericFieldOptions.Representation.INT64,
        true);
    IndexableFieldFactory.addIntegralValueToSortableNumericField(
        this.documentWrapper, this.path, supplier.get(), NumericFieldOptions.Representation.INT64);
  }

  @Override
  public void handleKnnVector(Supplier<Optional<Vector>> supplier) {
    if (isNotVectorField()) {
      return;
    }

    Optional<Vector> maybeVector = supplier.get();
    if (maybeVector.isEmpty()) {
      // failed to deserialize
      return;
    }

    Vector vector = maybeVector.get();
    VectorIndexVectorFieldDefinition vectorField = this.fieldDefinition.get().asVectorField();
    VectorFieldSpecification specification = vectorField.specification();
    IndexableFieldFactory.addKnnVectorField(
        this.documentWrapper,
        this.path,
        vector,
        specification,
        this.documentWrapper.indexCapabilities);
  }

  @Override
  public void handleNull() {
    // Index nulls only once per field in a Lucene document, since SortedDocValues does not allow
    // multiple values to be indexed
    IndexableFieldFactory.addNullField(this.documentWrapper, this.path);
  }

  @Override
  public void handleObjectId(Supplier<ObjectId> supplier) {
    if (isNotFilterField()) {
      return;
    }

    IndexableFieldFactory.addObjectIdField(this.documentWrapper, this.path, supplier.get());
  }

  @Override
  public void handleString(Supplier<String> supplier) {
    // replace string as vector in autoEmbeddings if this is auto-embedding field.
    if (isVectorTextField() && this.context.autoEmbeddings().containsKey(this.path)) {
      String stringValue = supplier.get();
      ImmutableMap<String, Vector> textToVector = this.context.autoEmbeddings().get(this.path);
      if (!textToVector.containsKey(stringValue)) {
        // failed to deserialize auto embedding text to vector field.
        return;
      }
      Vector vector = textToVector.get(stringValue);
      if (vector == null) {
        // failed to deserialize auto embedding text to vector field.
        return;
      }

      VectorIndexVectorFieldDefinition vectorField = this.fieldDefinition.get().asVectorField();
      VectorFieldSpecification specification = vectorField.specification();
      IndexableFieldFactory.addKnnVectorField(
          this.documentWrapper,
          this.path,
          vector,
          specification,
          this.documentWrapper.indexCapabilities);
      return;
    }

    if (isNotFilterField()) {
      return;
    }

    // SortedSetDocValues requires the BytesRef passed in be <= 3276 bytes. We
    // conservatively truncate the string to MAX_TERM_CHAR_LENGTH to ensure that we're
    // truncating on a character boundary rather than potentially in the middle of a code
    // point.
    String stringValue = supplier.get();
    int end = Math.min(stringValue.length(), LuceneConfig.MAX_TERM_CHAR_LENGTH);
    if (end != stringValue.length()) {
      this.documentWrapper.indexingMetricsUpdater.getSortableStringTruncatedCounter().increment();
      flogger.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
          "Unable to fully index token string value "
              + "due to its length=%d > maximum allowed=%d. "
              + "Truncating to %d characters. path=%s. _id=%s",
          stringValue.length(),
          LuceneConfig.MAX_TERM_CHAR_LENGTH,
          LuceneConfig.MAX_TERM_CHAR_LENGTH,
          this.path,
          lazy(() -> IndexableFieldFactory.getLoggingId(this.documentWrapper)));
    }

    String truncatedValue = stringValue.substring(0, end);
    IndexableFieldFactory.addTokenField(this.documentWrapper, this.path, truncatedValue);
  }

  @Override
  public void handleUuid(Supplier<Optional<UUID>> supplier) {
    if (isNotFilterField()) {
      return;
    }

    Optional<UUID> uuid = supplier.get();
    if (uuid.isEmpty()) {
      // binary value wasn't a UUID v4
      return;
    }

    IndexableFieldFactory.addUuidField(this.documentWrapper, this.path, uuid.get());
  }

  @Override
  public void handleRawBsonValue(Supplier<BsonValue> supplier) {}

  @Override
  public void markFieldNameExists() {
    IndexableFieldFactory.addFieldNamesField(this.documentWrapper, this.path);
  }

  @Override
  public Optional<FieldValueHandler> arrayFieldValueHandler() {
    return Optional.of(this);
  }

  @Override
  public Optional<DocumentHandler> subDocumentHandler() {
    if (!this.mapping.subDocumentExists(this.path)) {
      return Optional.empty();
    }
    return Optional.of(
        new LuceneVectorIndexDocumentBuilder(
            this.documentWrapper, this.mapping, Optional.of(this.path), this.context));
  }

  private boolean isNotFilterField() {
    return this.fieldDefinition.isEmpty()
        || this.fieldDefinition.get().getType() != VectorIndexFieldDefinition.Type.FILTER;
  }

  private boolean isNotVectorField() {
    return this.fieldDefinition.isEmpty() || !this.fieldDefinition.get().isVectorField();
  }

  private boolean isVectorTextField() {
    return this.fieldDefinition
        .filter(
            vectorFieldDefinition ->
                vectorFieldDefinition.getType() == VectorIndexFieldDefinition.Type.TEXT
                    || vectorFieldDefinition.getType()
                        == VectorIndexFieldDefinition.Type.AUTO_EMBED)
        .isPresent();
  }
}
