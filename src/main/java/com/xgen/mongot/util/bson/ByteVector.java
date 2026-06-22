package com.xgen.mongot.util.bson;

import java.util.Arrays;
import org.bson.BsonValue;

public final class ByteVector extends Vector {

  private final byte[] bytes;

  ByteVector(byte[] bytes) {
    this.bytes = bytes;
  }

  public byte[] getByteVector() {
    return this.bytes;
  }

  @Override
  public int numDimensions() {
    return this.bytes.length;
  }

  @Override
  public VectorType getVectorType() {
    return VectorType.BYTE;
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
    if (that == null || !that.getClass().equals(ByteVector.class)) {
      return false;
    }
    return Arrays.equals(this.bytes, ((ByteVector) that).bytes);
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
