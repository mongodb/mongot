package com.xgen.mongot.util.bson;

import com.xgen.mongot.util.FloatCollector;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonValue;

/** Parses vectors stored as BSON arrays of native numeric values. */
public final class NativeVectorParser {

  private NativeVectorParser() {}

  /** Parses a materialized BSON array, preserving native vector float narrowing behavior. */
  public static float[] parse(BsonValue value) throws BsonParseException {
    if (!value.isArray()) {
      throw unsupported(value.getBsonType());
    }
    return parse(value.asArray());
  }

  /** Parses a materialized BSON array, preserving native vector float narrowing behavior. */
  public static float[] parse(BsonArray array) throws BsonParseException {
    var vector = new FloatCollector();
    for (BsonValue value : array) {
      vector.add(parseElement(value));
    }
    return vector.toArray();
  }

  /** Parses a BSON array directly from the reader without materializing its contents. */
  public static Optional<float[]> parse(BsonReader reader) {
    var vector = new FloatCollector();
    reader.readStartArray();

    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.getCurrentBsonType()) {
        case INT32 -> vector.add((float) reader.readInt32());
        case INT64 -> vector.add((float) reader.readInt64());
        case DOUBLE -> vector.add((float) reader.readDouble());
        default -> {
          return Optional.empty();
        }
      }
    }
    reader.readEndArray();

    return Optional.of(vector.toArray());
  }

  private static float parseElement(BsonValue value) throws BsonParseException {
    return switch (value.getBsonType()) {
      case INT32 -> (float) value.asInt32().getValue();
      case INT64 -> (float) value.asInt64().getValue();
      case DOUBLE -> (float) value.asDouble().getValue();
      default -> throw unsupported(value.getBsonType());
    };
  }

  private static BsonParseException unsupported(BsonType type) {
    return new BsonParseException("Unsupported BsonValue type: " + type, Optional.empty());
  }
}
