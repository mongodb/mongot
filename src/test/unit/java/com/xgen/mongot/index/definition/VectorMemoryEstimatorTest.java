package com.xgen.mongot.index.definition;

import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.VectorDataFieldDefinitionBuilder;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class VectorMemoryEstimatorTest {

  @Test
  public void estimateFieldBytes_noQuantization_returnsFullFidelitySize() {
    long result = VectorMemoryEstimator.estimateFieldBytes(128, VectorQuantization.NONE, 1000);

    Assert.assertEquals(1000L * 128 * Float.BYTES, result);
  }

  @Test
  public void estimateFieldBytes_scalarQuantization_dividesByThreePointSevenFive() {
    long result = VectorMemoryEstimator.estimateFieldBytes(128, VectorQuantization.SCALAR, 1000);

    long fullFidelityBytes = 1000L * 128 * Float.BYTES;
    Assert.assertEquals((long) Math.ceil(fullFidelityBytes * (1.0 / 3.75)), result);
  }

  @Test
  public void estimateFieldBytes_binaryQuantization_dividesByTwentyFour() {
    long result = VectorMemoryEstimator.estimateFieldBytes(128, VectorQuantization.BINARY, 1000);

    long fullFidelityBytes = 1000L * 128 * Float.BYTES;
    Assert.assertEquals((long) Math.ceil(fullFidelityBytes * (1.0 / 24.0)), result);
  }

  @Test
  public void estimateFieldBytes_zeroDocs_returnsZero() {
    long result = VectorMemoryEstimator.estimateFieldBytes(128, VectorQuantization.NONE, 0);

    Assert.assertEquals(0L, result);
  }

  @Test
  public void estimateFieldBytes_zeroDimensions_returnsZero() {
    long result = VectorMemoryEstimator.estimateFieldBytes(0, VectorQuantization.NONE, 1000);

    Assert.assertEquals(0L, result);
  }

  // Empty index must report 0 for every quantization type: no constant graph-size offset.
  @Test
  public void estimateFieldBytes_zeroDocsBinaryQuantization_returnsZero() {
    long result = VectorMemoryEstimator.estimateFieldBytes(128, VectorQuantization.BINARY, 0);

    Assert.assertEquals(0L, result);
  }

  @Test
  public void estimateFieldBytes_zeroDocsScalarQuantization_returnsZero() {
    long result = VectorMemoryEstimator.estimateFieldBytes(128, VectorQuantization.SCALAR, 0);

    Assert.assertEquals(0L, result);
  }

  @Test
  public void estimateTotalBytes_multipleVectorFields_sumsPerFieldEstimates() {
    FieldPath path1 = FieldPath.parse("embedding1");
    VectorDataFieldDefinition field1 =
        VectorDataFieldDefinitionBuilder.builder()
            .path(path1)
            .numDimensions(128)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.NONE)
            .build();
    FieldPath path2 = FieldPath.parse("embedding2");
    VectorDataFieldDefinition field2 =
        VectorDataFieldDefinitionBuilder.builder()
            .path(path2)
            .numDimensions(64)
            .similarity(VectorSimilarity.EUCLIDEAN)
            .quantization(VectorQuantization.BINARY)
            .build();
    VectorIndexFieldMapping mapping =
        VectorIndexFieldMapping.create(List.of(field1, field2), Optional.empty());

    long result = VectorMemoryEstimator.estimateTotalBytes(mapping, 1000);

    long expectedField1Bytes =
        VectorMemoryEstimator.estimateFieldBytes(128, VectorQuantization.NONE, 1000);
    long expectedField2Bytes =
        VectorMemoryEstimator.estimateFieldBytes(64, VectorQuantization.BINARY, 1000);
    Assert.assertEquals(expectedField1Bytes + expectedField2Bytes, result);
  }

  @Test
  public void estimateTotalBytes_nonVectorFieldsOnly_returnsZero() {
    FieldPath filterPath = FieldPath.parse("filter");
    VectorIndexFilterFieldDefinition filterField =
        VectorIndexFilterFieldDefinition.create(filterPath);
    VectorIndexFieldMapping mapping =
        VectorIndexFieldMapping.create(List.of(filterField), Optional.empty());

    long result = VectorMemoryEstimator.estimateTotalBytes(mapping, 1000);

    Assert.assertEquals(0L, result);
  }
}
