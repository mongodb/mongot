package com.xgen.mongot.index.version;

import java.util.Objects;
import org.bson.types.ObjectId;

/**
 * Used to uniquely identify indexes. Specifically, GenerationId should be used instead of ObjectId
 * indexId to identify and differentiate two Index instances that coexist during an index swap.
 */
public class GenerationId {
  public final ObjectId indexId;
  public final Generation generation;

  public GenerationId(ObjectId indexId, Generation generation) {
    this.indexId = indexId;
    this.generation = generation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GenerationId id = (GenerationId) o;
    return this.indexId.equals(id.indexId) && this.generation.equals(id.generation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.indexId, this.generation);
  }

  /** A string used to uniquely identify GenerationId. */
  public String uniqueString() {
    return String.format(
        "%s-f%s-u%s-a%s",
        this.indexId.toHexString(),
        this.generation.formatVersion.versionNumber(),
        this.generation.userIndexVersion.versionNumber,
        this.generation.attemptNumber);
  }

  @Override
  public String toString() {
    return uniqueString();
  }
}
