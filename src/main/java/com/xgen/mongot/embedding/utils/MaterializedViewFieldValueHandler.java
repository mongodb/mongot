package com.xgen.mongot.embedding.utils;

import com.mongodb.client.model.geojson.Geometry;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.BsonVectorParser;
import com.xgen.mongot.util.bson.Vector;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

/**
 * Handles a single field value during materialized view document traversal. Copies the value if the
 * field is a passthrough field, and descends into sub-documents that are ancestors of passthrough
 * fields.
 */
public class MaterializedViewFieldValueHandler implements FieldValueHandler {
  private final Predicate<FieldPath> shouldKeep;
  private final Predicate<FieldPath> shouldDescend;
  private final FieldPath path;
  private final BsonValue bsonValue;

  private MaterializedViewFieldValueHandler(
      Predicate<FieldPath> shouldKeep,
      Predicate<FieldPath> shouldDescend,
      FieldPath path,
      BsonValue bsonValue) {
    this.shouldKeep = shouldKeep;
    this.shouldDescend = shouldDescend;
    this.path = path;
    this.bsonValue = bsonValue;
  }

  public static FieldValueHandler create(
      Predicate<FieldPath> shouldKeep,
      Predicate<FieldPath> shouldDescend,
      FieldPath path,
      BsonValue bsonValue) {
    Check.checkArg(
        (bsonValue instanceof BsonDocument) || (bsonValue instanceof BsonArray),
        "bsonValue input must be either BsonDocument or BsonArray");
    return new MaterializedViewFieldValueHandler(shouldKeep, shouldDescend, path, bsonValue);
  }

  @Override
  public void handleBinary(Supplier<Binary> supplier) {
    if (shouldKeepField()) {
      Binary binary = supplier.get();
      add(new BsonBinary(binary.getType(), binary.getData()));
    }
  }

  @Override
  public void handleBoolean(Supplier<Boolean> supplier) {
    if (shouldKeepField()) {
      add(new BsonBoolean(supplier.get()));
    }
  }

  @Override
  public void handleDateTime(Supplier<Long> supplier) {
    if (shouldKeepField()) {
      add(new BsonDateTime(supplier.get()));
    }
  }

  @Override
  public void handleDouble(Supplier<Double> supplier) {
    if (shouldKeepField()) {
      add(new BsonDouble(supplier.get()));
    }
  }

  @Override
  public void handleGeometry(Supplier<Optional<Geometry>> supplier) {
    // BSON has no native representation for geometries, so do nothing.
  }

  @Override
  public void handleInt32(Supplier<Integer> supplier) {
    if (shouldKeepField()) {
      add(new BsonInt32(supplier.get()));
    }
  }

  @Override
  public void handleInt64(Supplier<Long> supplier) {
    if (shouldKeepField()) {
      add(new BsonInt64(supplier.get()));
    }
  }

  @Override
  public void handleKnnVector(Supplier<Optional<Vector>> supplier) {
    if (shouldKeepField()) {
      Optional<Vector> vector = supplier.get();
      if (vector.isPresent()) {
        add(BsonVectorParser.encode(vector.get()));
      }
    }
  }

  @Override
  public void handleNull() {
    if (shouldKeepField()) {
      add(new BsonNull());
    }
  }

  @Override
  public void handleObjectId(Supplier<ObjectId> supplier) {
    if (shouldKeepField()) {
      add(new BsonObjectId(supplier.get()));
    }
  }

  @Override
  public void handleString(Supplier<String> supplier) {
    String textValue = supplier.get();
    if (shouldKeepField()) {
      add(new BsonString(textValue));
    }
  }

  @Override
  public void handleUuid(Supplier<Optional<UUID>> supplier) {
    if (shouldKeepField()) {
      Optional<UUID> uuid = supplier.get();
      if (uuid.isPresent()) {
        add(new BsonBinary(uuid.get()));
      }
    }
  }

  @Override
  public void handleRawBsonValue(Supplier<BsonValue> supplier) {
    if (shouldKeepField()) {
      add(supplier.get());
    }
  }

  @Override
  public void markFieldNameExists() {}

  @Override
  public Optional<FieldValueHandler> arrayFieldValueHandler() {
    BsonArray childBsonArray = new BsonArray();
    add(childBsonArray);
    return Optional.of(create(this.shouldKeep, this.shouldDescend, this.path, childBsonArray));
  }

  @Override
  public Optional<DocumentHandler> subDocumentHandler() {
    if (this.shouldDescend.test(this.path)) {
      BsonDocument childBsonDocument = new BsonDocument();
      add(childBsonDocument);
      return Optional.of(
          new MaterializedViewDocumentHandler(
              this.shouldKeep, this.shouldDescend, Optional.of(this.path), childBsonDocument));
    }
    return Optional.empty();
  }

  private void add(BsonValue childValue) {
    switch (this.bsonValue) {
      case BsonDocument bsonDocument -> bsonDocument.append(this.path.getLeaf(), childValue);
      case BsonArray bsonArray -> bsonArray.add(childValue);
      default ->
          throw new IllegalStateException(
              "Unexpected bsonValue type: " + this.bsonValue.getBsonType());
    }
  }

  private boolean shouldKeepField() {
    return this.shouldKeep.test(this.path);
  }
}
