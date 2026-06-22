package com.xgen.mongot.util.bson;

import com.google.errorprone.annotations.DoNotCall;
import com.xgen.mongot.util.Check;
import java.util.Arrays;
import java.util.EnumSet;
import org.bson.BsonArray;
import org.bson.BsonDouble;
import org.bson.BsonValue;

public final class FloatVector extends Vector {

  /**
   * This enum is used for keeping track of the original type of the float vector which is inferred
   * at the time of parsing the vector.
   */
  public enum OriginalType {
    /* Old format where vector is stored as BSON array of BSON doubles. */
    NATIVE,

    /* Space efficient BSON vector format. */
    BSON
  }

  private final float[] floats;
  private final OriginalType originalType;

  FloatVector(float[] floats, OriginalType originalType) {
    this.floats = floats;
    this.originalType = originalType;
  }

  /** Returns a mutable reference to the underlying vector data. */
  public float[] getFloatVector() {
    return this.floats;
  }

  public OriginalType getOriginalType() {
    return this.originalType;
  }

  @Override
  @Deprecated
  @DoNotCall
  public byte[] getBytes() {
    Check.expectedType(EnumSet.of(VectorType.BYTE, VectorType.BIT), getVectorType());
    return new byte[0];
  }

  @Override
  public int numDimensions() {
    return this.floats.length;
  }

  @Override
  public VectorType getVectorType() {
    return VectorType.FLOAT;
  }

  @Override
  public boolean isZeroVector() {
    if (isEmpty()) {
      return true;
    }
    for (var i : this.floats) {
      if (i != 0.0f) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || !that.getClass().equals(FloatVector.class)) {
      return false;
    }
    return Arrays.equals(this.floats, ((FloatVector) that).floats);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.floats);
  }

  // NOTE: this method and the corresponding fromBson() method are not in parity
  // This is not a problem because toBson() is mainly used for debugging purposes
  @Override
  public BsonValue toBson() {
    int length = this.floats.length;
    var bsonArray = new BsonArray(length);
    for (var i = 0; i < length; i++) {
      bsonArray.add(new BsonDouble(this.floats[i]));
    }
    return bsonArray;
  }

  // Only the first couple elements are printed to save space
  @Override
  public String toString() {
    if (this.floats.length <= 2) {
      return Arrays.toString(this.floats);
    }
    return "[" + this.floats[0] + ", " + this.floats[1] + ",...]";
  }
}
