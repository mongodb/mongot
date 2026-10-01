package com.xgen.mongot.index.lucene.codec.flat;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.backward_codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.bson.types.ObjectId;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ScalarQuantizedFlatVectorsFormatTest extends BaseKnnVectorsFormatTestCase {

  private final KnnVectorsFormat format = new ScalarQuantizedFlatVectorsFormat();

  /**
   * Enable the NATIVE_SCALAR_VECTOR_SCORING flag so the native Q7 path is exercised on accelerated
   * platforms; it falls back to Lucene's Java scalar scorer on unaccelerated hardware or
   * non-memory-mapped directories, so the suite passes everywhere.
   */
  @BeforeClass
  public static void enableNativeScalarScoring() {
    DynamicFeatureFlagRegistry.resetForTest();
    DynamicFeatureFlagConfig enabled =
        new DynamicFeatureFlagConfig(
            DynamicFeatureFlags.NATIVE_SCALAR_VECTOR_SCORING.getName(),
            DynamicFeatureFlagConfig.Phase.ENABLED,
            List.of(),
            List.of(),
            100,
            DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER);
    DynamicFeatureFlagRegistry.initializeProcessInstance(
        new DynamicFeatureFlagRegistry(
            Optional.of(List.of(enabled)),
            Optional.empty(),
            Optional.empty(),
            Optional.of(new ObjectId())));
  }

  @AfterClass
  public static void resetRegistry() {
    DynamicFeatureFlagRegistry.resetForTest();
  }

  @Override
  protected boolean supportsFloatVectorFallback() {
    return false;
  }

  @Override
  public int getQuantizationBits() {
    return 8;
  }

  @Override
  protected Codec getCodec() {
    return new Lucene99Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return ScalarQuantizedFlatVectorsFormatTest.this.format;
      }
    };
  }
}
