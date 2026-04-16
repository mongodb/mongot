package com.xgen.mongot.embedding.utils;

import com.mongodb.client.model.geojson.Geometry;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.bson.BsonValue;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

/**
 * Collects string values for vector text fields.
 *
 * <p>If the FieldPath, path, is found in the VectorIndexFieldMapping, and it is a vector text, then
 * any String values are added to the given set.
 */
public class CollectVectorTextStringsFieldValueHandler implements FieldValueHandler {

  private final AutoEmbedFieldMapping mapping;
  private final FieldPath path;
  private final Map<FieldPath, Set<String>> vectorTextPathMap;

  private CollectVectorTextStringsFieldValueHandler(
      AutoEmbedFieldMapping mapping,
      FieldPath path,
      Map<FieldPath, Set<String>> vectorTextPathMap
  ) {
    this.mapping = mapping;
    this.path = path;
    this.vectorTextPathMap = vectorTextPathMap;
  }

  public static FieldValueHandler create(
      AutoEmbedFieldMapping mapping,
      FieldPath path,
      Map<FieldPath, Set<String>> vectorTextPathMap
  ) {
    return new CollectVectorTextStringsFieldValueHandler(mapping, path, vectorTextPathMap);
  }

  @Override
  public void handleBinary(Supplier<Binary> supplier) {
  }

  @Override
  public void handleBoolean(Supplier<Boolean> supplier) {
  }

  @Override
  public void handleDateTime(Supplier<Long> supplier) {
  }

  @Override
  public void handleDouble(Supplier<Double> supplier) {
  }

  @Override
  public void handleGeometry(Supplier<Optional<Geometry>> supplier) {
  }

  @Override
  public void handleInt32(Supplier<Integer> supplier) {
  }

  @Override
  public void handleInt64(Supplier<Long> supplier) {
  }

  @Override
  public void handleKnnVector(Supplier<Optional<Vector>> supplier) {
  }

  @Override
  public void handleNull() {
  }

  @Override
  public void handleObjectId(Supplier<ObjectId> supplier) {
  }

  @Override
  public void handleString(Supplier<String> supplier) {
    if (this.mapping.isEmbed(this.path)) {
      this.vectorTextPathMap.computeIfAbsent(this.path, key -> new HashSet<>()).add(supplier.get());
    }
  }

  @Override
  public void handleUuid(Supplier<Optional<UUID>> supplier) {
  }

  @Override
  public void handleRawBsonValue(Supplier<BsonValue> supplier) {
  }

  @Override
  public void markFieldNameExists() {
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
        new CollectVectorTextStringsDocumentHandler(
            this.mapping, Optional.of(this.path), this.vectorTextPathMap));
  }
}
