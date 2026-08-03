package com.xgen.mongot.index.lucene.quantization;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;

import com.google.common.truth.Truth;
import java.io.IOException;
import org.apache.lucene.backward_codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.junit.Test;

public class MongotHnswScalarQuantizedVectorsFormatTest extends BaseKnnVectorsFormatTestCase {

  private final KnnVectorsFormat format =
      new MongotHnswScalarQuantizedVectorsFormat(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH);

  // Matches the fork's TestLucene99HnswScalarQuantizedVectorsFormat: the 9.9-era scalar-quantized
  // reader does not serve dequantized floats when the raw vector data is missing.
  @Override
  protected boolean supportsFloatVectorFallback() {
    return false;
  }

  @Override
  protected Codec getCodec() {
    return new Lucene99Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return MongotHnswScalarQuantizedVectorsFormatTest.this.format;
      }
    };
  }

  /**
   * The rollback invariant this format exists for: an HNSW graph must be written for every segment,
   * including ones below Lucene 10.4's {@code HNSW_GRAPH_THRESHOLD} (100 vectors). Graph-less
   * segments write {@code numLevels=0} metadata that pre-10.4 readers (mongot on Lucene
   * 9.11.1/10.1.0) NPE on at query time.
   */
  @Test
  public void fieldsWriter_tinySegmentFlush_writesHnswGraph() throws IOException {
    String field = "field";
    // A plain IndexWriterConfig (no framework randomization) so all docs flush as one segment.
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(getCodec()))) {
      int numDocs = 8;
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(
            new KnnFloatVectorField(field, randomVector(16), VectorSimilarityFunction.EUCLIDEAN));
        writer.addDocument(doc);
      }
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        Truth.assertThat(reader.leaves()).hasSize(1);
        LeafReader leaf = reader.leaves().get(0).reader();
        KnnVectorsReader fieldReader =
            ((PerFieldKnnVectorsFormat.FieldsReader) ((CodecReader) leaf).getVectorReader())
                .getFieldReader(field);
        Truth.assertThat(fieldReader).isInstanceOf(HnswGraphProvider.class);
        HnswGraph graph = ((HnswGraphProvider) fieldReader).getGraph(field);
        assertWithMessage(
                "expected an HNSW graph on a tiny (%s-vector) segment; graph-less segments break"
                    + " 9.11/10.1 rollback",
                numDocs)
            .that(graph.numLevels())
            .isAtLeast(1);
      }
    }
  }
}
