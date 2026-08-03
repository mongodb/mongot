package com.xgen.mongot.index.lucene.vector;

import com.google.common.truth.Truth;
import java.io.IOException;
import org.apache.lucene.backward_codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.junit.Test;

public class Lucene99NativeHnswVectorsFormatTest extends BaseKnnVectorsFormatTestCase {

  @Override
  protected boolean supportsFloatVectorFallback() {
    return false;
  }

  @Override
  protected Codec getCodec() {
    return new Lucene99Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new Lucene99NativeHnswVectorsFormat();
      }
    };
  }

  private static final String FIELD = "vec";
  private static final byte[] VECTOR = {1, 2, 3, 4};

  /**
   * Verifies that {@link Lucene99NativeHnswVectorsFormat} overrides the {@code
   * Lucene99HnswVectorsFormat} SPI entry: when a segment is read back, the per-field reader
   * resolved by name must be a {@link
   * Lucene99NativeHnswVectorsFormat.Lucene99NativeHnswVectorsReader}, not a plain {@code
   * Lucene99HnswVectorsReader}.
   */
  @Test
  public void fieldsReader_returnsLucene99NativeHnswVectorsReader() throws IOException {
    try (ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexWriter w =
            new IndexWriter(dir, new IndexWriterConfig().setCodec(new Lucene99NativeTestCodec()))) {
      Document doc = new Document();
      doc.add(new KnnByteVectorField(FIELD, VECTOR, VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
      w.commit();

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        CodecReader leafReader = (CodecReader) reader.leaves().get(0).reader();
        KnnVectorsReader vectorReader = leafReader.getVectorReader();
        // When the codec wraps Lucene99NativeHnswVectorsFormat in a PerFieldKnnVectorsFormat,
        // unwrap to the per-field reader; otherwise the codec exposes the
        // Lucene99NativeHnswVectorsReader directly.
        KnnVectorsReader fieldReader =
            vectorReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader
                ? fieldsReader.getFieldReader(FIELD)
                : vectorReader;

        Truth.assertThat(fieldReader)
            .isInstanceOf(Lucene99NativeHnswVectorsFormat.Lucene99NativeHnswVectorsReader.class);
      }
    }
  }
}
