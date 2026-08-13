package com.xgen.mongot.util.bson;

import java.util.Arrays;
import org.bson.BSONException;
import org.bson.BsonValue;

/**
 * A half-precision (IEEE-754 binary16) vector. The raw little-endian f16 bytes are stored as they
 * arrive on the wire so they can be carried half-width all the way to the native vectorlite engine
 * without a widening round-trip. Each dimension occupies two bytes.
 *
 * <p>f16 input is only supported by the vectorlite engine; the Lucene indexing/query paths reject
 * it.
 */
public final class Float16Vector extends Vector {

  /** Raw little-endian IEEE-754 binary16 values, two bytes per dimension. */
  private final byte[] bytes;

  Float16Vector(byte[] bytes) {
    if (bytes.length % Short.BYTES != 0) {
      throw new BSONException(
          "f16 vector byte length must be a multiple of 2 but got " + bytes.length);
    }
    this.bytes = bytes;
  }

  /** The raw little-endian f16 bytes (two per dimension). */
  public byte[] getFloat16Bytes() {
    return this.bytes;
  }

  @Override
  public int numDimensions() {
    return this.bytes.length / Short.BYTES;
  }

  @Override
  public VectorType getVectorType() {
    return VectorType.FLOAT16;
  }

  @Override
  public boolean isZeroVector() {
    for (int i = 0; i < this.bytes.length; i += Short.BYTES) {
      short bits = (short) ((this.bytes[i] & 0xFF) | (this.bytes[i + 1] << 8));
      if (Float.float16ToFloat(bits) != 0.0f) {
        return false;
      }
    }
    return true;
  }

  @Override
  public byte[] getBytes() {
    return this.bytes;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || !that.getClass().equals(Float16Vector.class)) {
      return false;
    }
    return Arrays.equals(this.bytes, ((Float16Vector) that).bytes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.bytes);
  }

  // NOTE: this method and the corresponding fromBson() method are not in parity
  // This is not a problem because toBson() is only used for debugging purposes
  @Override
  public BsonValue toBson() {
    return BsonVectorParser.encode(this);
  }

  @Override
  public String toString() {
    return "Float16Vector[dims=" + numDimensions() + "]";
  }
}
