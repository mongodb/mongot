package com.xgen.mongot.util.bson;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FloatCollector;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Encodable;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonValue;

/**
 * Abstract representation of a vector. The stored vector could be a float32 vector, int8 vector
 * (scalar quantized vector) or a bit vector (binary quantized vector).
 */
public abstract sealed class Vector implements Encodable
    permits BitVector, ByteVector, FloatVector, Float16Vector {

  /**
   * The data type of the vector. This indicates whether the vector consists of floats, half-floats
   * (f16), int8s/bytes or bits.
   */
  public enum VectorType {
    FLOAT,
    FLOAT16,
    BYTE,
    BIT
  }

  public static FloatVector fromFloats(float[] floats, FloatVector.OriginalType originalType) {
    return new FloatVector(floats, originalType);
  }

  /** Build a half-precision vector from its raw little-endian f16 bytes (two per dimension). */
  public static Float16Vector fromFloat16Bytes(byte[] bytes) {
    return new Float16Vector(bytes);
  }

  public static ByteVector fromBytes(byte[] bytes) {
    return new ByteVector(bytes);
  }

  public static BitVector fromBits(byte[] bytes) {
    return new BitVector(bytes);
  }

  public FloatVector asFloatVector() {
    Check.expectedType(VectorType.FLOAT, getVectorType());
    return (FloatVector) this;
  }

  public Float16Vector asFloat16Vector() {
    Check.expectedType(VectorType.FLOAT16, getVectorType());
    return (Float16Vector) this;
  }

  public ByteVector asByteVector() {
    Check.expectedType(VectorType.BYTE, getVectorType());
    return (ByteVector) this;
  }

  public BitVector asBitVector() {
    Check.expectedType(VectorType.BIT, getVectorType());
    return (BitVector) this;
  }

  public static Vector fromBson(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    if (value.isArray()) {
      // TODO(CLOUDP-271775): re-use code from KnnVectorParser class
      BsonArray array = value.asArray();
      FloatCollector collector = new FloatCollector();
      for (BsonValue bsonValue : array) {
        float floatValue;
        if (bsonValue.isDouble()) {
          floatValue = (float) bsonValue.asDouble().getValue();
        } else if (bsonValue.isInt32()) {
          floatValue = (float) bsonValue.asInt32().getValue();
        } else if (bsonValue.isInt64()) {
          floatValue = (float) bsonValue.asInt64().getValue();
        } else {
          throw new BsonParseException(
              "Unsupported BsonValue type: " + bsonValue.getBsonType(), Optional.empty());
        }
        collector.add(floatValue);
      }
      return Vector.fromFloats(collector.toArray(), FloatVector.OriginalType.NATIVE);
    } else if (value.isBinary()) {
      return BsonVectorParser.parse(value.asBinary());
    } else {
      return context.handleSemanticError("Unexpected type found when parsing vector");
    }
  }

  /**
   * This works only for {@link BitVector} and {@link ByteVector} type objects
   *
   * @return the vector contents as a byte array
   */
  public abstract byte[] getBytes();

  /**
   * Check if the vector has zero dimensions
   *
   * @return true if vector has zero dimensions
   */
  public boolean isEmpty() {
    return numDimensions() == 0;
  }

  /**
   * Getter for the number of dimension of the vector
   *
   * @return The number of dimensions of the stored vector
   */
  public abstract int numDimensions();

  public abstract VectorType getVectorType();

  /**
   * Check if the vector is a zero vector
   *
   * @return true if all elements in the vector are zeros
   */
  public abstract boolean isZeroVector();
}
