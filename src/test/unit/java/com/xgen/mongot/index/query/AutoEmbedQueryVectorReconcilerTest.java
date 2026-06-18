package com.xgen.mongot.index.query;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import org.junit.Test;

public class AutoEmbedQueryVectorReconcilerTest {

  private static final FieldPath PATH = FieldPath.parse("summary");

  private static final Vector FLOAT_VECTOR =
      Vector.fromFloats(new float[] {0.1f, -0.2f, 0.3f, -0.4f}, FloatVector.OriginalType.NATIVE);
  private static final Vector BYTE_VECTOR = Vector.fromBytes(new byte[] {1, -2, 3, -4});
  private static final Vector BIT_VECTOR = Vector.fromBits(new byte[] {0x55});

  @Test
  public void floatQuantization_floatVector_passesThrough() throws Exception {
    assertSame(
        FLOAT_VECTOR,
        AutoEmbedQueryVectorReconciler.reconcile(
            FLOAT_VECTOR, VectorAutoEmbedQuantization.FLOAT, PATH));
  }

  @Test
  public void floatQuantization_byteVector_throws() {
    // int8 is not faithfully convertible to the index's float32 embeddings (scoring would be
    // distorted), so it is rejected rather than widened.
    InvalidQueryException e =
        assertThrows(
            InvalidQueryException.class,
            () ->
                AutoEmbedQueryVectorReconciler.reconcile(
                    BYTE_VECTOR, VectorAutoEmbedQuantization.FLOAT, PATH));
    assertThat(e).hasMessageThat().contains("autoEmbed quantization 'float'");
    assertThat(e).hasMessageThat().contains("float32");
  }

  @Test
  public void floatQuantization_bitVector_throws() {
    InvalidQueryException e =
        assertThrows(
            InvalidQueryException.class,
            () ->
                AutoEmbedQueryVectorReconciler.reconcile(
                    BIT_VECTOR, VectorAutoEmbedQuantization.FLOAT, PATH));
    assertThat(e).hasMessageThat().contains("autoEmbed quantization 'float'");
    assertThat(e).hasMessageThat().contains(PATH.toString());
  }

  @Test
  public void mismatchError_usesBsonDtypeVocabulary_notInternalEnumNames() {
    InvalidQueryException e =
        assertThrows(
            InvalidQueryException.class,
            () ->
                AutoEmbedQueryVectorReconciler.reconcile(
                    FLOAT_VECTOR, VectorAutoEmbedQuantization.SCALAR, PATH));
    // The actual-type side names the BSON dtype a client sent, not the internal enum constant.
    assertThat(e).hasMessageThat().contains("float32");
    assertThat(e).hasMessageThat().doesNotContain("FLOAT");
    assertThat(e).hasMessageThat().doesNotContain("BYTE");
  }

  @Test
  public void scalarQuantization_byteVector_passesThrough() throws Exception {
    assertSame(
        BYTE_VECTOR,
        AutoEmbedQueryVectorReconciler.reconcile(
            BYTE_VECTOR, VectorAutoEmbedQuantization.SCALAR, PATH));
  }

  @Test
  public void scalarQuantization_floatVector_throwsWithGuidance() {
    InvalidQueryException e =
        assertThrows(
            InvalidQueryException.class,
            () ->
                AutoEmbedQueryVectorReconciler.reconcile(
                    FLOAT_VECTOR, VectorAutoEmbedQuantization.SCALAR, PATH));
    assertThat(e).hasMessageThat().contains("autoEmbed quantization 'scalar'");
    assertThat(e).hasMessageThat().contains("int8");
  }

  @Test
  public void scalarQuantization_bitVector_throws() {
    InvalidQueryException e =
        assertThrows(
            InvalidQueryException.class,
            () ->
                AutoEmbedQueryVectorReconciler.reconcile(
                    BIT_VECTOR, VectorAutoEmbedQuantization.SCALAR, PATH));
    assertThat(e).hasMessageThat().contains("autoEmbed quantization 'scalar'");
  }

  @Test
  public void binaryQuantization_floatVector_passesThrough() throws Exception {
    assertSame(
        FLOAT_VECTOR,
        AutoEmbedQueryVectorReconciler.reconcile(
            FLOAT_VECTOR, VectorAutoEmbedQuantization.BINARY, PATH));
  }

  @Test
  public void binaryQuantization_byteVector_throws() {
    // binary also indexes float32 embeddings (quantized Lucene-side), so int8 is rejected.
    InvalidQueryException e =
        assertThrows(
            InvalidQueryException.class,
            () ->
                AutoEmbedQueryVectorReconciler.reconcile(
                    BYTE_VECTOR, VectorAutoEmbedQuantization.BINARY, PATH));
    assertThat(e).hasMessageThat().contains("autoEmbed quantization 'binary'");
    assertThat(e).hasMessageThat().contains("float32");
  }

  @Test
  public void binaryQuantization_bitVector_throws() {
    InvalidQueryException e =
        assertThrows(
            InvalidQueryException.class,
            () ->
                AutoEmbedQueryVectorReconciler.reconcile(
                    BIT_VECTOR, VectorAutoEmbedQuantization.BINARY, PATH));
    assertThat(e).hasMessageThat().contains("autoEmbed quantization 'binary'");
  }

  @Test
  public void binaryNoRescoreQuantization_bitVector_passesThrough() throws Exception {
    assertSame(
        BIT_VECTOR,
        AutoEmbedQueryVectorReconciler.reconcile(
            BIT_VECTOR, VectorAutoEmbedQuantization.BINARY_NO_RESCORE, PATH));
  }

  @Test
  public void binaryNoRescoreQuantization_floatVector_throwsWithGuidance() {
    InvalidQueryException e =
        assertThrows(
            InvalidQueryException.class,
            () ->
                AutoEmbedQueryVectorReconciler.reconcile(
                    FLOAT_VECTOR, VectorAutoEmbedQuantization.BINARY_NO_RESCORE, PATH));
    assertThat(e).hasMessageThat().contains("autoEmbed quantization 'binaryNoRescore'");
    assertThat(e).hasMessageThat().contains("packedBit");
  }

  @Test
  public void binaryNoRescoreQuantization_byteVector_throws() {
    InvalidQueryException e =
        assertThrows(
            InvalidQueryException.class,
            () ->
                AutoEmbedQueryVectorReconciler.reconcile(
                    BYTE_VECTOR, VectorAutoEmbedQuantization.BINARY_NO_RESCORE, PATH));
    assertThat(e).hasMessageThat().contains("autoEmbed quantization 'binaryNoRescore'");
  }
}
