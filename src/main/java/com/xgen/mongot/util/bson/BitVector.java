package com.xgen.mongot.util.bson;

import java.util.Arrays;
import org.bson.BsonValue;

public final class BitVector extends Vector {

  private final byte[] bytes;

  // The dimensions of bit vectors must be a multiple of 8.
  // Hence, padding is set to 0.
  // If this condition of enforcing bit vector dimensions to be a multiple of 8 is removed,
  // then this needs to be changed to be a non-constant class member variable.
  private static final int PADDING = 0;

  BitVector(byte[] bytes) {
    this.bytes = bytes;
  }

  public byte[] getBitVector() {
    return this.bytes;
  }

  public int getPadding() {
    return PADDING;
  }

  public int size() {
    return this.bytes.length;
  }

  @Override
  public int numDimensions() {
    return (this.bytes.length * 8) - PADDING;
  }

  @Override
  public VectorType getVectorType() {
    return VectorType.BIT;
  }

  @Override
  public boolean isZeroVector() {
    if (isEmpty()) {
      return true;
    }
    for (var i : this.bytes) {
      if (i != 0) {
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
    if (that == null || !that.getClass().equals(BitVector.class)) {
      return false;
    }
    return Arrays.equals(this.bytes, ((BitVector) that).bytes);
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

  // Only the first couple elements are printed to save space
  @Override
  public String toString() {
    if (this.bytes.length <= 2) {
      return Arrays.toString(this.bytes);
    }
    return "[" + this.bytes[0] + ", " + this.bytes[1] + ",...]";
  }
}
