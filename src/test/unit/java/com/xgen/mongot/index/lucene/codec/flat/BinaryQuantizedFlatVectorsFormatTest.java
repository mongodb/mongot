package com.xgen.mongot.index.lucene.codec.flat;

import org.apache.lucene.backward_codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;

public class BinaryQuantizedFlatVectorsFormatTest extends BaseKnnVectorsFormatTestCase {

  private final KnnVectorsFormat format = new BinaryQuantizedFlatVectorsFormat();

  @Override
  protected boolean supportsFloatVectorFallback() {
    return false;
  }

  @Override
  public int getQuantizationBits() {
    return 1;
  }

  @Override
  protected Codec getCodec() {
    return new Lucene99Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return BinaryQuantizedFlatVectorsFormatTest.this.format;
      }
    };
  }
}
