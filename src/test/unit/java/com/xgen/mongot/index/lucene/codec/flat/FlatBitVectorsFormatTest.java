package com.xgen.mongot.index.lucene.codec.flat;

import com.google.common.truth.Truth;
import java.io.IOException;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseIndexFileFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.junit.Test;

/**
 * This deliberately does not extend {@code BaseKnnVectorsFormatTestCase}: that suite indexes
 * float32 vectors, which a bit-only format rejects by design.
 */
public class FlatBitVectorsFormatTest extends BaseIndexFileFormatTestCase {

  /** 32 bits per vector, stored as 4 bytes. */
  private static final int BYTE_DIMENSION = 4;

  private static final int BIT_DIMENSION = BYTE_DIMENSION * Byte.SIZE;

  private static final String FIELD = "bits";

  /**
   * The returned codec keeps the default codec's name, so on read the segment resolves via SPI to
   * the real default codec, whose {@code knnVectorsFormat()} is a {@link PerFieldKnnVectorsFormat}.
   * The write side must therefore go through {@code PerFieldKnnVectorsFormat} too: that is what
   * records the per-field format name and suffix as FieldInfo attributes. Returning a bare {@link
   * FlatBitVectorsFormat} here writes no such attributes, and the reader then silently finds no
   * vectors at all rather than failing.
   */
  @Override
  protected Codec getCodec() {
    return new FilterCodec(TestUtil.getDefaultCodec().getName(), TestUtil.getDefaultCodec()) {
      @Override
      public KnnVectorsFormat knnVectorsFormat() {
        return new PerFieldKnnVectorsFormat() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return new FlatBitVectorsFormat();
          }
        };
      }
    };
  }

  @Override
  protected void addRandomFields(Document doc) {
    doc.add(
        new KnnByteVectorField(
            FIELD, new byte[BYTE_DIMENSION], VectorSimilarityFunction.EUCLIDEAN));
  }

  private static Document docWithVector(byte[] vector) {
    var doc = new Document();
    doc.add(new KnnByteVectorField(FIELD, vector, VectorSimilarityFunction.EUCLIDEAN));
    return doc;
  }

  @Test
  public void byteVectorValues_afterIndexing_roundTripsStoredBits() throws IOException {
    byte[] first = {0x00, 0x0F, (byte) 0xF0, (byte) 0xFF};
    byte[] second = {0x01, 0x02, 0x03, 0x04};

    try (Directory dir = newDirectory()) {
      try (var writer = new IndexWriter(dir, writerConfig())) {
        writer.addDocument(docWithVector(first));
        writer.addDocument(docWithVector(second));
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        ByteVectorValues values = leaf.getByteVectorValues(FIELD);

        Truth.assertThat(values.size()).isEqualTo(2);
        Truth.assertThat(values.dimension()).isEqualTo(BYTE_DIMENSION);
        Truth.assertThat(values.vectorValue(0)).isEqualTo(first);
        Truth.assertThat(values.vectorValue(1)).isEqualTo(second);
      }
    }
  }

  @Test
  public void knnQuery_exactMatchPresent_rankedFirstWithFullBitSimilarity() throws IOException {
    byte[] target = {0x00, 0x0F, (byte) 0xF0, (byte) 0xFF};
    // Differs from the target in exactly 4 bits.
    byte[] nearby = {0x0F, 0x0F, (byte) 0xF0, (byte) 0xFF};
    // Differs from the target in all 32 bits.
    byte[] opposite = {(byte) 0xFF, (byte) 0xF0, 0x0F, 0x00};

    try (Directory dir = newDirectory()) {
      try (var writer = new IndexWriter(dir, writerConfig())) {
        writer.addDocument(docWithVector(opposite));
        writer.addDocument(docWithVector(nearby));
        writer.addDocument(docWithVector(target));
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        var searcher = new IndexSearcher(reader);

        TopDocs topDocs = searcher.search(new KnnByteVectorQuery(FIELD, target, 3), 3);

        Truth.assertThat(topDocs.scoreDocs).hasLength(3);
        Truth.assertThat(topDocs.scoreDocs[0].score).isEqualTo(1.0f);
        Truth.assertThat(topDocs.scoreDocs[1].score)
            .isEqualTo((BIT_DIMENSION - 4) / (float) BIT_DIMENSION);
        Truth.assertThat(topDocs.scoreDocs[2].score).isEqualTo(0.0f);
      }
    }
  }

  @Test
  public void knnQuery_kSmallerThanSegment_returnsOnlyTopK() throws IOException {
    byte[] target = {0x00, 0x00, 0x00, 0x00};

    try (Directory dir = newDirectory()) {
      try (var writer = new IndexWriter(dir, writerConfig())) {
        writer.addDocument(docWithVector(new byte[] {0x00, 0x00, 0x00, 0x00}));
        writer.addDocument(docWithVector(new byte[] {0x01, 0x00, 0x00, 0x00}));
        writer.addDocument(docWithVector(new byte[] {0x03, 0x00, 0x00, 0x00}));
        writer.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        var searcher = new IndexSearcher(reader);

        TopDocs topDocs = searcher.search(new KnnByteVectorQuery(FIELD, target, 2), 10);

        Truth.assertThat(topDocs.scoreDocs).hasLength(2);
        Truth.assertThat(topDocs.scoreDocs[0].score).isEqualTo(1.0f);
        Truth.assertThat(topDocs.scoreDocs[1].score)
            .isEqualTo((BIT_DIMENSION - 1) / (float) BIT_DIMENSION);
      }
    }
  }

  private IndexWriterConfig writerConfig() {
    return new IndexWriterConfig().setCodec(getCodec());
  }
}
