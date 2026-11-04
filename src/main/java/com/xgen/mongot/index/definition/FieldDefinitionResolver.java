package com.xgen.mongot.index.definition;

import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

public interface FieldDefinitionResolver {

  /**
   * Returns true if the provided path is indexed or is a part of path to an indexed node. For
   * example if path "a.b.c" is indexed as a certain type, paths "a", "a.b" and "a.b.c" are used.
   */
  boolean isUsed(FieldPath path);

  Optional<VectorFieldSpecification> getVectorFieldSpecification(FieldPath path);

  /**
   * Resolves the quantization of the vector field at {@code path}, or {@link
   * VectorQuantization#NONE} when {@code path} is not a vector field.
   */
  default VectorQuantization resolveQuantization(FieldPath path) {
    return getVectorFieldSpecification(path)
        .map(VectorFieldSpecification::quantization)
        .orElse(VectorQuantization.NONE);
  }
}
