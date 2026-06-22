package com.xgen.mongot.index.lucene.document.single;

import static com.xgen.mongot.util.Check.checkState;

import com.mongodb.client.model.geojson.Geometry;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.GeoFieldDefinition;
import com.xgen.mongot.index.definition.NumberFacetFieldDefinition;
import com.xgen.mongot.index.definition.NumberFieldDefinition;
import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.definition.SortableNumberBetaV1FieldDefinition;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.geo.LuceneGeometryFields;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.bson.BsonValue;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

/** Contains logic related to indexing Lucene values that are supported by search indexes. */
public class LuceneSearchIndexFieldValueHandler implements FieldValueHandler {

  private final DocumentWrapper documentWrapper;
  private final FieldDefinition fieldDefinition;
  private final FieldPath path;

  /** Represents that the parent(s) field's value is an array. */
  final boolean isMultiValued;

  private LuceneSearchIndexFieldValueHandler(
      DocumentWrapper documentWrapper,
      FieldDefinition fieldDefinition,
      FieldPath path,
      boolean isMultiValued) {
    this.documentWrapper = documentWrapper;
    this.path = path;
    this.isMultiValued = isMultiValued;
    this.fieldDefinition = fieldDefinition;
  }

  static FieldValueHandler create(
      DocumentWrapper documentWrapper,
      FieldDefinition fieldDefinition,
      FieldPath path,
      boolean isMultiValued) {
    return new LuceneSearchIndexFieldValueHandler(
        documentWrapper, fieldDefinition, path, isMultiValued);
  }

  @Override
  public void handleBoolean(Supplier<Boolean> supplier) {
    if (this.fieldDefinition.booleanFieldDefinition().isPresent()) {
      boolean value = supplier.get();
      IndexableFieldFactory.addBooleanField(this.documentWrapper, this.path, value);
    }
  }

  @Override
  public void handleBinary(Supplier<Binary> supplier) {
    // do nothing, since UUID indexing logic is handled below in handleUuid() and all other
    // remaining types of binary data are currently not indexed
  }

  @Override
  public void handleDateTime(Supplier<Long> supplier) {
    handleDateField(supplier);
    handleDateFacetField(supplier);
    handleSortableDateField(supplier);
  }

  private void handleDateField(Supplier<Long> supplier) {
    if (this.fieldDefinition.dateFieldDefinition().isPresent()) {
      IndexableFieldFactory.addDateField(
          this.documentWrapper, this.path, supplier.get(), this.isMultiValued);
    }
  }

  private void handleDateFacetField(Supplier<Long> supplier) {
    if (this.fieldDefinition.dateFieldDefinition().isPresent() || this.isMultiValued) {
      return;
    }

    if (this.fieldDefinition.dateFacetFieldDefinition().isPresent()
        && this.documentWrapper.getEmbeddedRoot().isEmpty()) {
      // Only handle date facet if "date"-type field is not present. There are no options to index
      // dates differently, so only index a date once.
      IndexableFieldFactory.addDateFacetField(this.documentWrapper, this.path, supplier.get());
    }
  }

  private void handleSortableDateField(Supplier<Long> supplier) {
    // Support multi-values for sortable dates but not embedded documents.
    if (this.fieldDefinition.sortableDateBetaV1FieldDefinition().isEmpty()) {
      if (!this.documentWrapper.isNumberAndDateSortable
          || this.fieldDefinition.dateFieldDefinition().isEmpty()) {
        return;
      }
    }

    if (this.fieldDefinition.sortableDateBetaV1FieldDefinition().isPresent()) {
      checkState(
          this.documentWrapper.getEmbeddedRoot().isEmpty(),
          "sortableDateBetaV1 is disallowed in embeddedDocuments. _id=%s",
          IndexableFieldFactory.getLoggingId(this.documentWrapper));
      IndexableFieldFactory.addSortableDateBetaField(
          this.documentWrapper, this.path, supplier.get());
    }

    if (this.documentWrapper.isNumberAndDateSortable
        && this.fieldDefinition.dateFieldDefinition().isPresent()) {
      IndexableFieldFactory.addSortableDateV2Field(this.documentWrapper, this.path, supplier.get());
      addNullnessFieldIfNeeded(Optional.empty());
    }
  }

  @Override
  public void handleDouble(Supplier<Double> supplier) {
    Optional<NumberFieldDefinition> numberFieldDefinition =
        this.fieldDefinition.numberFieldDefinition();
    Optional<NumberFacetFieldDefinition> numberFacetFieldDefinition =
        this.fieldDefinition.numberFacetFieldDefinition();
    Optional<SortableNumberBetaV1FieldDefinition> sortableNumberFieldDefinition =
        this.fieldDefinition.sortableNumberBetaV1FieldDefinition();

    numberFieldDefinition.ifPresent(
        definition -> {
          handleDoubleField(supplier, definition);
          handleSortableDoubleV2Field(supplier, definition);
        });
    numberFacetFieldDefinition.ifPresent(
        definition -> handleDoubleFacetField(supplier, numberFieldDefinition, definition));
    sortableNumberFieldDefinition.ifPresent(definition -> handleSortBetaDoubleField(supplier));
  }

  private void handleDoubleField(
      Supplier<Double> supplier, NumberFieldDefinition numberFieldDefinition) {
    if (numberFieldDefinition.options().indexDoubles()) {
      IndexableFieldFactory.addFloatingPointValueToNumericField(
          this.documentWrapper,
          this.path,
          supplier.get(),
          numberFieldDefinition.options().representation(),
          this.isMultiValued);
    }
  }

  private void handleDoubleFacetField(
      Supplier<Double> supplier,
      Optional<NumberFieldDefinition> numberFieldDefinition,
      NumberFacetFieldDefinition numberFacetFieldDefinition) {
    if (this.isMultiValued) {
      return;
    }

    if (numberFieldDefinition.isPresent()
        && numberFacetFieldDefinition.hasSameOptionsAs(numberFieldDefinition.get())) {
      return;
    }

    if (numberFacetFieldDefinition.options().indexDoubles()
        && this.documentWrapper.getEmbeddedRoot().isEmpty()) {
      IndexableFieldFactory.addFloatingPointNumericFacetField(
          this.documentWrapper,
          this.path,
          supplier.get(),
          numberFacetFieldDefinition.options().representation());
    }
  }

  private void handleSortBetaDoubleField(Supplier<Double> supplier) {
    // Support multi-values for sortable numbers but not embedded documents.
    checkState(
        this.documentWrapper.getEmbeddedRoot().isEmpty(),
        "sortableNumberBetaV1 is disallowed in embeddedDocuments. _id=%s",
        IndexableFieldFactory.getLoggingId(this.documentWrapper));

    IndexableFieldFactory.addFloatingPointValueToSortBetaNumericField(
        this.documentWrapper, this.path, supplier.get());
  }

  private void handleSortableDoubleV2Field(
      Supplier<Double> supplier, NumberFieldDefinition numberFieldDefinition) {
    if (this.documentWrapper.isNumberAndDateSortable
        && numberFieldDefinition.options().indexDoubles()) {
      IndexableFieldFactory.addFloatingPointValueToSortableNumericField(
          this.documentWrapper,
          this.path,
          supplier.get(),
          numberFieldDefinition.options().representation());
      addNullnessFieldIfNeeded(Optional.of(numberFieldDefinition.options().representation()));
    }
  }

  @Override
  public void handleGeometry(Supplier<Optional<Geometry>> supplier) {
    Optional<GeoFieldDefinition> geoFieldDefinition = this.fieldDefinition.geoFieldDefinition();
    if (geoFieldDefinition.isEmpty()) {
      return;
    }

    Optional<Geometry> geometry = supplier.get();
    if (geometry.isEmpty()) {
      // failed to deserialize
      return;
    }

    if (geoFieldDefinition.get().indexShapes()) {
      LuceneGeometryFields.forGeoShape(
              geometry.get(),
              FieldName.TypeField.GEO_SHAPE.getLuceneFieldName(
                  this.path, this.documentWrapper.getEmbeddedRoot()),
              this.documentWrapper.indexingMetricsUpdater)
          .forEach(this.documentWrapper.luceneDocument::add);
    }

    LuceneGeometryFields.forGeoPoint(
            geometry.get(),
            FieldName.TypeField.GEO_POINT.getLuceneFieldName(
                this.path, this.documentWrapper.getEmbeddedRoot()),
            this.documentWrapper.indexingMetricsUpdater)
        .forEach(this.documentWrapper.luceneDocument::add);
  }

  @Override
  public void handleInt32(Supplier<Integer> supplier) {
    handleInt64(() -> supplier.get().longValue());
  }

  @Override
  public void handleInt64(Supplier<Long> supplier) {
    Optional<NumberFieldDefinition> numberFieldDefinition =
        this.fieldDefinition.numberFieldDefinition();
    Optional<NumberFacetFieldDefinition> numberFacetFieldDefinition =
        this.fieldDefinition.numberFacetFieldDefinition();
    Optional<SortableNumberBetaV1FieldDefinition> sortBetaNumberFieldDefinition =
        this.fieldDefinition.sortableNumberBetaV1FieldDefinition();

    numberFieldDefinition.ifPresent(
        definition -> {
          handleInt64Field(supplier, definition);
          handleSortableLongV2Field(supplier, definition);
        });
    numberFacetFieldDefinition.ifPresent(
        definition -> handleInt64FacetField(supplier, numberFieldDefinition, definition));
    sortBetaNumberFieldDefinition.ifPresent(definition -> handleSortBetaLongField(supplier));
  }

  private void handleInt64Field(
      Supplier<Long> supplier, NumberFieldDefinition numberFieldDefinition) {
    if (numberFieldDefinition.options().indexIntegers()) {
      IndexableFieldFactory.addIntegralValueToNumericField(
          this.documentWrapper,
          this.path,
          supplier.get(),
          numberFieldDefinition.options().representation(),
          this.isMultiValued);
    }
  }

  private void handleInt64FacetField(
      Supplier<Long> supplier,
      Optional<NumberFieldDefinition> numberFieldDefinition,
      NumberFacetFieldDefinition numberFacetFieldDefinition) {
    if (this.isMultiValued) {
      return;
    }

    if (numberFieldDefinition.isPresent()
        && numberFacetFieldDefinition.hasSameOptionsAs(numberFieldDefinition.get())) {
      return;
    }

    if (numberFacetFieldDefinition.options().indexIntegers()
        && this.documentWrapper.getEmbeddedRoot().isEmpty()) {
      IndexableFieldFactory.addIntegralNumericFacetField(
          this.documentWrapper,
          this.path,
          supplier.get(),
          numberFacetFieldDefinition.options().representation());
    }
  }

  private void handleSortBetaLongField(Supplier<Long> supplier) {
    // Support multi-values for sortable numbers but not embedded documents.
    checkState(
        this.documentWrapper.getEmbeddedRoot().isEmpty(),
        "sortableNumberBetaV1 is disallowed in embeddedDocuments. _id=%s",
        IndexableFieldFactory.getLoggingId(this.documentWrapper));

    IndexableFieldFactory.addIntegralValueToSortBetaNumericField(
        this.documentWrapper, this.path, supplier.get());
  }

  private void handleSortableLongV2Field(
      Supplier<Long> supplier, NumberFieldDefinition numberFieldDefinition) {
    if (this.documentWrapper.isNumberAndDateSortable
        && numberFieldDefinition.options().indexIntegers()) {
      IndexableFieldFactory.addIntegralValueToSortableNumericField(
          this.documentWrapper,
          this.path,
          supplier.get(),
          numberFieldDefinition.options().representation());
      addNullnessFieldIfNeeded(Optional.of(numberFieldDefinition.options().representation()));
    }
  }

  @Override
  public void handleKnnVector(Supplier<Optional<Vector>> supplier) {
    Optional<VectorFieldSpecification> maybeFieldSpec =
        this.fieldDefinition.vectorFieldSpecification();

    if (maybeFieldSpec.isEmpty() || this.isMultiValued) {
      return;
    }

    checkState(
        this.documentWrapper.getEmbeddedRoot().isEmpty(),
        "should be impossible for knn fields to be present in embedded documents. _id=%s",
        IndexableFieldFactory.getLoggingId(this.documentWrapper));

    Optional<Vector> maybeVector = supplier.get();
    if (maybeVector.isEmpty()) {
      // failed to deserialize
      return;
    }

    VectorFieldSpecification fieldSpec = maybeFieldSpec.get();
    Vector vector = maybeVector.get();
    IndexableFieldFactory.addKnnVectorField(this.documentWrapper, this.path, vector, fieldSpec);
  }

  @Override
  public void handleNull() {
    // Check the current path to see if it's already indexed, since SortedDocValues does not allow
    // multiple values to be indexed. Note that we don't need to check the embeddedRoot since all
    // fields for the wrapped Lucene document have the same embedded root.
    IndexableFieldFactory.addNullField(this.documentWrapper, this.path);
  }

  @Override
  public void handleObjectId(Supplier<ObjectId> supplier) {
    if (this.fieldDefinition.objectIdFieldDefinition().isPresent()) {
      ObjectId objectId = supplier.get();
      IndexableFieldFactory.addObjectIdField(this.documentWrapper, this.path, objectId);
    }
  }

  @Override
  public void handleString(Supplier<String> supplier) throws IOException {
    handleSortableString(supplier);

    this.fieldDefinition
        .stringFieldDefinition()
        .ifPresent(
            stringField ->
                doHandleString(this.documentWrapper, stringField, this.path, supplier.get()));

    if (this.fieldDefinition.stringFacetFieldDefinition().isPresent()
        && this.documentWrapper.getEmbeddedRoot().isEmpty()) {
      IndexableFieldFactory.addStringFacetField(this.documentWrapper, this.path, supplier.get());
    }

    if (this.fieldDefinition.autocompleteFieldDefinition().isPresent()) {
      IndexableFieldFactory.addAutocompleteField(this.documentWrapper, this.path, supplier.get());
    }
  }

  private static void doHandleString(
      AbstractDocumentWrapper wrapper,
      StringFieldDefinition fieldDefinition,
      FieldPath fieldPath,
      String value) {
    IndexableFieldFactory.addStringField(wrapper, fieldPath, value, fieldDefinition);
  }

  private void handleSortableString(Supplier<String> supplier) throws IOException {
    IndexableFieldFactory.addSortableStringField(
        this.documentWrapper, this.fieldDefinition, this.path, supplier.get());
  }

  @Override
  public void handleUuid(Supplier<Optional<UUID>> supplier) {
    if (this.fieldDefinition.uuidFieldDefinition().isEmpty()) {
      return;
    }

    Optional<UUID> uuid = supplier.get();
    if (uuid.isEmpty()) {
      // binary data wasn't a UUID v4
      return;
    }

    IndexableFieldFactory.addUuidField(this.documentWrapper, this.path, uuid.get());
  }

  @Override
  public void handleRawBsonValue(Supplier<BsonValue> supplier) {
    // do nothing
  }

  @Override
  public void markFieldNameExists() {
    IndexableFieldFactory.addFieldNamesField(this.documentWrapper, this.path);
  }

  @Override
  public Optional<FieldValueHandler> arrayFieldValueHandler() {
    if (this.fieldDefinition.stringFieldDefinition().isEmpty()
        && this.fieldDefinition.stringFacetFieldDefinition().isEmpty()
        && this.fieldDefinition.objectIdFieldDefinition().isEmpty()
        && this.fieldDefinition.geoFieldDefinition().isEmpty()
        && this.fieldDefinition.documentFieldDefinition().isEmpty()
        && this.fieldDefinition.sortableDateBetaV1FieldDefinition().isEmpty()
        && this.fieldDefinition.sortableNumberBetaV1FieldDefinition().isEmpty()
        && this.fieldDefinition.sortableStringBetaV1FieldDefinition().isEmpty()
        && this.fieldDefinition.tokenFieldDefinition().isEmpty()
        && this.fieldDefinition.uuidFieldDefinition().isEmpty()
        && indexDoesNotSupportMultiValuedNumbers()
        && indexDoesNotSupportAutocompleteArrays()
        && indexDoesNotSupportMultiValuedBooleans()) {
      return Optional.empty();
    }

    return this.isMultiValued
        ? Optional.of(this)
        : Optional.of(
            new LuceneSearchIndexFieldValueHandler(
                this.documentWrapper, this.fieldDefinition, this.path, true));
  }

  private boolean indexDoesNotSupportMultiValuedNumbers() {
    return this.fieldDefinition.dateFieldDefinition().isEmpty()
        && this.fieldDefinition.numberFieldDefinition().isEmpty();
  }

  private boolean indexDoesNotSupportAutocompleteArrays() {
    return this.fieldDefinition.autocompleteFieldDefinition().isEmpty();
  }

  private boolean indexDoesNotSupportMultiValuedBooleans() {
    return this.fieldDefinition.booleanFieldDefinition().isEmpty();
  }

  @Override
  public Optional<DocumentHandler> subDocumentHandler() {
    return this.fieldDefinition
        .documentFieldDefinition()
        .map(
            documentFieldDefinition ->
                LuceneDocumentHandler.create(
                    this.documentWrapper, documentFieldDefinition, this.path, this.isMultiValued));
  }

  private void addNullnessFieldIfNeeded(
      Optional<NumericFieldOptions.Representation> representation) {
    if (representation.isPresent()
        && representation.get() != NumericFieldOptions.Representation.INT64) {
      return;
    }

    if (this.documentWrapper.getIndexSortNullnessFields().contains(this.path)) {
      IndexableFieldFactory.addNullnessField(this.documentWrapper, this.path);
    }
  }
}
