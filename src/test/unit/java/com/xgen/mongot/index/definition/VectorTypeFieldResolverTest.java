package com.xgen.mongot.index.definition;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_BIT;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_BYTE;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_BYTE_Q1;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_F32_Q1;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_F32_Q7;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_VECTOR;

import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.definition.quantization.VectorTypeFieldResolver;
import com.xgen.mongot.util.bson.Vector.VectorType;
import org.junit.Test;

public class VectorTypeFieldResolverTest {

  @Test
  public void resolveTypeField_floatNone_returnsKnnVector() {
    assertThat(VectorTypeFieldResolver.resolveTypeField(VectorQuantization.NONE, VectorType.FLOAT))
        .isEqualTo(KNN_VECTOR);
  }

  @Test
  public void resolveTypeField_floatScalar_returnsKnnF32Q7() {
    assertThat(
            VectorTypeFieldResolver.resolveTypeField(VectorQuantization.SCALAR, VectorType.FLOAT))
        .isEqualTo(KNN_F32_Q7);
  }

  @Test
  public void resolveTypeField_floatBinary_returnsKnnF32Q1() {
    assertThat(
            VectorTypeFieldResolver.resolveTypeField(VectorQuantization.BINARY, VectorType.FLOAT))
        .isEqualTo(KNN_F32_Q1);
  }

  @Test
  public void resolveTypeField_byteNone_returnsKnnByte() {
    assertThat(VectorTypeFieldResolver.resolveTypeField(VectorQuantization.NONE, VectorType.BYTE))
        .isEqualTo(KNN_BYTE);
  }

  @Test
  public void resolveTypeField_byteScalar_returnsKnnByte() {
    assertThat(VectorTypeFieldResolver.resolveTypeField(VectorQuantization.SCALAR, VectorType.BYTE))
        .isEqualTo(KNN_BYTE);
  }

  @Test
  public void resolveTypeField_byteBinary_returnsKnnByteQ1() {
    assertThat(VectorTypeFieldResolver.resolveTypeField(VectorQuantization.BINARY, VectorType.BYTE))
        .isEqualTo(KNN_BYTE_Q1);
  }

  @Test
  public void resolveTypeField_bitNone_returnsKnnBit() {
    assertThat(VectorTypeFieldResolver.resolveTypeField(VectorQuantization.NONE, VectorType.BIT))
        .isEqualTo(KNN_BIT);
  }

  @Test
  public void resolveTypeField_bitScalar_returnsKnnBit() {
    assertThat(VectorTypeFieldResolver.resolveTypeField(VectorQuantization.SCALAR, VectorType.BIT))
        .isEqualTo(KNN_BIT);
  }

  @Test
  public void resolveTypeField_bitBinary_returnsKnnBit() {
    assertThat(VectorTypeFieldResolver.resolveTypeField(VectorQuantization.BINARY, VectorType.BIT))
        .isEqualTo(KNN_BIT);
  }
}
