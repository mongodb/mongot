package com.xgen.mongot.util.bson;

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.Arrays;
import org.bson.BSONException;
import org.bson.BsonBinary;

/**
 * Class to parse bson vector data type. This class assumes the following specification for the byte
 * arrangement of bson vectors
 *
 * <p>binary ::= int32 subtype (byte*) From https://bsonspec.org/spec.html ^ number of bytes in
 * (byte*)
 *
 * <p>subtype ::= unsigned_byte(9) New bson vector sub-type within bson binary
 *
 * <p>The first two bytes of the (byte*) indicate the vector data type and any padding added at the
 * end. In the first version, we support vector data types of binary (1 bit), int8 (signed 8 bit
 * int), and float32 (32 bit floating point number).
 *
 * <p>The representation of the first two bytes is shown below
 * +---------------------------------------------------------------+ | 1st byte | 2nd byte |
 * |---------------------------------------------------------------| | dtype |element size | empty |
 * padding | |---------------|-----------------------------------|-----------| | | | | | | | | | | |
 * | | | | | | +---------------------------------------------------------------+ The first byte
 * (dtype + element size) is used to determine the data type of the bson vector. The lower 3 bits of
 * the second byte are used for padding. The padding is added in the event of fractional bytes. The
 * padding value indicates the number of elements to ignore at the end.
 *
 * <p>As mentioned above, in the first version we only support 3 data types. The corresponding
 * values of the first byte is listed in the table below
 * +-----------------------------------------------+ | binary (1 bit) | 0x10 |
 * |-----------------------------------------------| | int8 (signed 8 bit int) | 0x03 |
 * |-----------------------------------------------| | float32 (32 bit floating point) | 0x27 |
 * +-----------------------------------------------+
 *
 * <p>All bytes are assumed to be in little-endian byte order For more info about the bson vector
 * spec see the doc linked here - https://jira.mongodb.org/browse/WRITING-17964
 */
// TODO(CLOUDP-280897): Revisit exception handling/throwing after migrating to new java driver
// version
public class BsonVectorParser {

  public static final int BSON_VECTOR_HEADER_SIZE = 2;
  public static final byte VECTOR_SUB_TYPE = (byte) 0x09;

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public static Vector parse(BsonBinary bsonBinary) {
    if (bsonBinary.getType() != VECTOR_SUB_TYPE) {
      throw new BSONException(
          "BSON binary subtype should be vector(0x09) but got " + bsonBinary.getType());
    }
    byte[] bsonByteArray = bsonBinary.getData();
    if (bsonByteArray == null) {
      throw new BSONException("Got a null BSON vector");
    }
    if (bsonByteArray.length < BSON_VECTOR_HEADER_SIZE) {
      throw new BSONException(
          "Expected at least 2 bytes in BSON vector but got " + bsonByteArray.length);
    }
    ByteBuffer bsonInput = ByteBuffer.wrap(bsonByteArray).order(ByteOrder.LITTLE_ENDIAN);
    // parse data type
    byte dataType = bsonInput.get();
    VectorDataType vectorDataType =
        Arrays.stream(VectorDataType.values())
            .filter(dt -> dt.getValue() == dataType)
            .findFirst()
            .orElseThrow(() -> new BSONException("Unsupported vector subtype found"));
    int padding = getPadding(bsonInput.get());

    return switch (vectorDataType) {
      case FLOAT32_VECTOR_DATA_TYPE -> parseFloatVector(bsonInput, padding);
      case BYTE_VECTOR_DATA_TYPE -> parseByteVector(bsonInput, padding);
      case BIT_VECTOR_DATA_TYPE -> parseBitVector(bsonInput, padding);
    };
  }

  // only encodes float32 and int8 vector types for now
  public static BsonBinary encode(Vector vector) {
    Vector.VectorType vectorType = vector.getVectorType();
    return switch (vectorType) {
      case FLOAT -> encodeFloatVector(vector.asFloatVector());
      case BYTE -> encodeByteVector(vector.asByteVector());
      case BIT -> encodeBitVector(vector.asBitVector());
    };
  }

  private static BsonBinary encodeFloatVector(FloatVector vector) {
    int bufferSize = BSON_VECTOR_HEADER_SIZE + Float.BYTES * vector.numDimensions();
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
    buffer.put(VectorDataType.FLOAT32_VECTOR_DATA_TYPE.getValue());
    buffer.put((byte) 0x00); // padding is always 0 in case of float32 vectors
    if (!vector.isEmpty()) {
      FloatBuffer floatBuffer = FloatBuffer.wrap(vector.asFloatVector().getFloatVector());
      while (floatBuffer.hasRemaining()) {
        buffer.putFloat(floatBuffer.get());
      }
    }
    return new BsonBinary(VECTOR_SUB_TYPE, buffer.array());
  }

  private static BsonBinary encodeByteVector(ByteVector vector) {
    int bufferSize = BSON_VECTOR_HEADER_SIZE + vector.numDimensions();
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
    buffer.put(VectorDataType.BYTE_VECTOR_DATA_TYPE.getValue());
    buffer.put((byte) 0x00); // padding is always 0 in case of int8 vectors
    if (!vector.isEmpty()) {
      buffer.put(vector.asByteVector().getByteVector());
    }
    return new BsonBinary(VECTOR_SUB_TYPE, buffer.array());
  }

  private static BsonBinary encodeBitVector(BitVector vector) {
    int bufferSize = BSON_VECTOR_HEADER_SIZE + vector.size();
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
    buffer.put(VectorDataType.BIT_VECTOR_DATA_TYPE.getValue());
    buffer.put((byte) vector.getPadding());
    if (!vector.isEmpty()) {
      buffer.put(vector.asBitVector().getBitVector());
    }
    return new BsonBinary(VECTOR_SUB_TYPE, buffer.array());
  }

  @VisibleForTesting
  static int getPadding(byte paddingByte) {
    return paddingByte & 0x07; // only lower 3 bits are used for padding
  }

  private static Vector parseFloatVector(ByteBuffer bsonInput, int padding) {
    if (padding != 0) {
      throw new BSONException("Padding should be zero");
    }
    FloatBuffer floatBuffer = bsonInput.asFloatBuffer();
    float[] vector = new float[floatBuffer.remaining()];
    floatBuffer.get(vector);
    return Vector.fromFloats(vector, FloatVector.OriginalType.BSON);
  }

  private static Vector parseByteVector(ByteBuffer bsonInput, int padding) {
    if (padding != 0) {
      throw new BSONException("Padding should be zero");
    }
    byte[] byteVector = new byte[bsonInput.remaining()];
    bsonInput.get(byteVector);
    return Vector.fromBytes(byteVector);
  }

  private static Vector parseBitVector(ByteBuffer bsonInput, int padding) {
    if (padding != 0) {
      throw new BSONException(
          """
          The dimensions of binary vector should be a multiple of 8 and
          the padding should be zero.
          """);
    }
    byte[] vector = new byte[bsonInput.remaining()];
    bsonInput.get(vector);
    return Vector.fromBits(vector);
  }

  @VisibleForTesting
  public enum VectorDataType {
    BYTE_VECTOR_DATA_TYPE((byte) 0x03),
    FLOAT32_VECTOR_DATA_TYPE((byte) 0x27),
    BIT_VECTOR_DATA_TYPE((byte) 0x10);

    private final byte value;

    VectorDataType(byte value) {
      this.value = value;
    }

    public byte getValue() {
      return this.value;
    }
  }
}
