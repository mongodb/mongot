package com.xgen.mongot.index.definition.quantization;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.bson.Vector;

/**
 * Resolves the Lucene {@link FieldName.TypeField} that backs a vector field. The mapping depends on
 * both the field's indexed {@link VectorQuantization} and the {@link Vector.VectorType} of the
 * query vector.
 */
public final class VectorTypeFieldResolver {

  private VectorTypeFieldResolver() {}

  /**
   * Returns the {@link FieldName.TypeField} backing a vector field indexed with {@code
   * quantization} and queried with a {@code vectorType} vector.
   *
   * @param quantization the quantization type
   * @param vectorType the vector type
   */
  public static FieldName.TypeField resolveTypeField(
      VectorQuantization quantization, Vector.VectorType vectorType) {
    return switch (vectorType) {
      case FLOAT ->
          switch (quantization) {
            case NONE -> FieldName.TypeField.KNN_VECTOR;
            case SCALAR -> FieldName.TypeField.KNN_F32_Q7;
            case BINARY -> FieldName.TypeField.KNN_F32_Q1;
          };
      case BYTE ->
          switch (quantization) {
            case NONE, SCALAR -> FieldName.TypeField.KNN_BYTE;
            case BINARY -> FieldName.TypeField.KNN_BYTE_Q1;
          };
      case BIT -> FieldName.TypeField.KNN_BIT;
    };
  }
}
