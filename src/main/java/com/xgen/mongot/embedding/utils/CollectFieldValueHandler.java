package com.xgen.mongot.embedding.utils;

import com.mongodb.client.model.geojson.Geometry;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

public class CollectFieldValueHandler implements FieldValueHandler {

  private final AutoEmbedFieldMapping mapping;
  private final FieldPath path;
  private final Map<FieldPath, List<BsonValue>> collectedValues;
  private final Set<FieldPath> arrayPaths;
  private final Predicate<FieldPath> fieldPredicate;
  private final boolean onlyCollectFieldsinMapping;

  private CollectFieldValueHandler(
      AutoEmbedFieldMapping mapping,
      FieldPath path,
      Map<FieldPath, List<BsonValue>> collectedValues,
      Set<FieldPath> arrayPaths,
      Predicate<FieldPath> fieldPredicate,
      boolean onlyCollectFieldsinMapping) {
    this.mapping = mapping;
    this.path = path;
    this.collectedValues = collectedValues;
    this.arrayPaths = arrayPaths;
    this.fieldPredicate = fieldPredicate;
    this.onlyCollectFieldsinMapping = onlyCollectFieldsinMapping;
  }

  public static FieldValueHandler create(
      AutoEmbedFieldMapping mapping,
      FieldPath path,
      Map<FieldPath, List<BsonValue>> vectorTextPathMap,
      Set<FieldPath> arrayPaths,
      Predicate<FieldPath> fieldPredicate,
      boolean onlyCollectFieldsinMapping) {
    return new CollectFieldValueHandler(
        mapping, path, vectorTextPathMap, arrayPaths, fieldPredicate, onlyCollectFieldsinMapping);
  }

  @Override
  public void handleBinary(Supplier<Binary> supplier) {
    if (shouldCollect()) {
      Binary binary = supplier.get();
      collectValue(new BsonBinary(binary.getType(), binary.getData()));
    }
  }

  @Override
  public void handleBoolean(Supplier<Boolean> supplier) {
    if (shouldCollect()) {
      collectValue(new BsonBoolean(supplier.get()));
    }
  }

  @Override
  public void handleDateTime(Supplier<Long> supplier) {
    if (shouldCollect()) {
      collectValue(new BsonDateTime(supplier.get()));
    }
  }

  @Override
  public void handleDouble(Supplier<Double> supplier) {
    if (shouldCollect()) {
      collectValue(new BsonDouble(supplier.get()));
    }
  }

  @Override
  public void handleGeometry(Supplier<Optional<Geometry>> supplier) {}

  @Override
  public void handleInt32(Supplier<Integer> supplier) {
    if (shouldCollect()) {
      collectValue(new BsonInt32(supplier.get()));
    }
  }

  @Override
  public void handleInt64(Supplier<Long> supplier) {
    if (shouldCollect()) {
      collectValue(new BsonInt64(supplier.get()));
    }
  }

  @Override
  public void handleKnnVector(Supplier<Optional<Vector>> supplier) {}

  @Override
  public void handleNull() {}

  @Override
  public void handleObjectId(Supplier<ObjectId> supplier) {
    if (shouldCollect()) {
      collectValue(new BsonObjectId(supplier.get()));
    }
  }

  @Override
  public void handleString(Supplier<String> supplier) {
    if (shouldCollect()) {
      collectValue(new BsonString(supplier.get()));
    }
  }

  @Override
  public void handleUuid(Supplier<Optional<UUID>> supplier) {
    if (shouldCollect()) {
      supplier.get().ifPresent(uuid -> collectValue(new BsonBinary(uuid)));
    }
  }

  @Override
  public void handleRawBsonValue(Supplier<BsonValue> supplier) {
    if (shouldCollect()) {
      collectValue(supplier.get());
    }
  }

  @Override
  public void markFieldNameExists() {}

  @Override
  public Optional<FieldValueHandler> arrayFieldValueHandler() {
    // Track that this path is an array so callers can distinguish between
    // scalar values and single-element arrays (e.g., "red" vs ["red"])
    this.arrayPaths.add(this.path);
    return Optional.of(this);
  }

  @Override
  public Optional<DocumentHandler> subDocumentHandler() {
    if (!this.mapping.subDocumentExists(this.path)) {
      return Optional.empty();
    }
    return Optional.of(
        new CollectFieldValueDocumentHandler(
            this.mapping,
            Optional.of(this.path),
            this.collectedValues,
            this.arrayPaths,
            this.fieldPredicate,
            this.onlyCollectFieldsinMapping));
  }

  private boolean shouldCollect() {
    if (this.onlyCollectFieldsinMapping) {
      return this.mapping.getField(this.path).isPresent() && this.fieldPredicate.test(this.path);
    } else {
      return true;
    }
  }

  private void collectValue(BsonValue value) {
    this.collectedValues.computeIfAbsent(this.path, key -> new ArrayList<>()).add(value);
  }
}
