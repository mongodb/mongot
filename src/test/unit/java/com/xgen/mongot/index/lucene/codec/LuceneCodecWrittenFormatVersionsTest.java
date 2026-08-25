package com.xgen.mongot.index.lucene.codec;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_BIT;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_BYTE;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_F32_Q1;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_F32_Q7;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_VECTOR;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.junit.Test;

/**
 * Byte-level guard on what {@link LuceneCodec} writes under each {@link LuceneCodec.WriteFormat}:
 * writes one segment per write format and parses the raw index headers of the emitted files.
 *
 * <p>This complements {@code OnDiskFormatVersionsTest} (which pins the version <i>constants</i>) by
 * locking the version each writer actually stamps on disk, in both directions:
 *
 * <ul>
 *   <li><b>{@link LuceneCodec.WriteFormat#LUCENE99} (flag off):</b> every header carries the
 *       9.11-compatible version. The fork's write-version defaults match upstream 10.4 and the 9.11
 *       path opts DOWN explicitly (the fork's {@code backward_codecs.lucene99.Lucene99Codec}
 *       internally; {@code LuceneCodec.WriteFormat} for the per-field HNSW formats) — this
 *       direction catches any missed opt-down, which would silently break binary rollback.
 *   <li><b>{@link LuceneCodec.WriteFormat#LUCENE104} (flag on):</b> every header carries the
 *       vanilla 10.4 version (points, fieldInfos, default postings, codec name, and all five HNSW
 *       graph encodings) — catches a missed slot that would make flag-on output diverge from
 *       vanilla.
 * </ul>
 */
public class LuceneCodecWrittenFormatVersionsTest {

  private static final String KEYWORD_FIELD = "keyword";
  private static final String POINT_FIELD = "point";

  private static final int FLOAT_DIMS = 16;
  private static final int BYTE_DIMS = 16;
  private static final int BIT_DIM_BYTES = 2;

  /** Graph write version stamped into a field's {@code .vem} file, per KNN type field. */
  private static final ImmutableMap<FieldName.TypeField, Integer> LUCENE99_EXPECTED_VEM_VERSIONS =
      ImmutableMap.of(
          KNN_VECTOR, 0,
          KNN_BYTE, 0,
          KNN_BIT, 0,
          KNN_F32_Q7, 0,
          KNN_F32_Q1, 0);

  /** Every HNSW format writes the vanilla 10.4 group-varint graph encoding under LUCENE104. */
  private static final ImmutableMap<FieldName.TypeField, Integer> LUCENE104_EXPECTED_VEM_VERSIONS =
      ImmutableMap.of(
          KNN_VECTOR, 1,
          KNN_BYTE, 1,
          KNN_BIT, 1,
          KNN_F32_Q7, 1,
          KNN_F32_Q1, 1);

  @Test
  public void lucene99WriteFormat_writes911CompatibleVersions() throws IOException {
    assertWrittenVersions(
        LuceneCodec.WriteFormat.LUCENE99,
        "Lucene99",
        "Lucene99",
        /* expectedIndexCreatedVersionMajor= */ 9,
        /* expectedFnmVersion= */ 1,
        /* expectedPointsMetaVersion= */ 0,
        LUCENE99_EXPECTED_VEM_VERSIONS);
  }

  @Test
  public void lucene104WriteFormat_writesVanilla104Versions() throws IOException {
    assertWrittenVersions(
        LuceneCodec.WriteFormat.LUCENE104,
        "Lucene104",
        "Lucene104",
        /* expectedIndexCreatedVersionMajor= */ 10,
        /* expectedFnmVersion= */ 2,
        /* expectedPointsMetaVersion= */ 1,
        LUCENE104_EXPECTED_VEM_VERSIONS);
  }

  private static void assertWrittenVersions(
      LuceneCodec.WriteFormat writeFormat,
      String expectedCodecName,
      String expectedPostingsFormatName,
      int expectedIndexCreatedVersionMajor,
      int expectedFnmVersion,
      int expectedPointsMetaVersion,
      Map<FieldName.TypeField, Integer> expectedVemVersions)
      throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      writeSingleSegment(directory, writeFormat);

      // Codec name recorded in segment metadata (what a reader resolves via SPI).
      SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
      assertThat(segmentInfos.size()).isEqualTo(1);
      assertThat(segmentInfos.info(0).info.getCodec().getName()).isEqualTo(expectedCodecName);
      // Index-wide created version: 9 keeps new indexes openable by 9.11 rollback binaries;
      // LUCENE104 stamps 10 (index-wide, permanent).
      assertWithMessage("indexCreatedVersionMajor under %s", writeFormat)
          .that(segmentInfos.getIndexCreatedVersionMajor())
          .isEqualTo(expectedIndexCreatedVersionMajor);
      String segmentName = segmentInfos.info(0).info.name;

      // FieldInfos: the fork's V1 shim stamps v1 (9.11-compatible); the core class stamps v2.
      IndexHeader fnmHeader =
          readIndexHeader(directory, IndexFileNames.segmentFileName(segmentName, "", "fnm"));
      assertThat(fnmHeader.codecName()).isEqualTo("Lucene94FieldInfos");
      assertWithMessage(".fnm version under %s", writeFormat)
          .that(fnmHeader.version())
          .isEqualTo(expectedFnmVersion);

      // Points (BKD) metadata.
      IndexHeader kdmHeader =
          readIndexHeader(directory, IndexFileNames.segmentFileName(segmentName, "", "kdm"));
      assertThat(kdmHeader.codecName()).isEqualTo("Lucene90PointsFormatMeta");
      assertWithMessage(".kdm version under %s", writeFormat)
          .that(kdmHeader.version())
          .isEqualTo(expectedPointsMetaVersion);

      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        assertThat(reader.leaves()).hasSize(1);
        SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();

        // Per-field postings format name recorded in field metadata.
        FieldInfo keywordInfo = segmentReader.getFieldInfos().fieldInfo(KEYWORD_FIELD);
        assertWithMessage("postings format attribute under %s", writeFormat)
            .that(keywordInfo.getAttribute(PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY))
            .isEqualTo(expectedPostingsFormatName);

        // Per-KNN-field graph metadata (.vem) version. The per-field format name and suffix
        // attributes reconstruct each field's exact file name.
        for (Map.Entry<FieldName.TypeField, Integer> expected : expectedVemVersions.entrySet()) {
          String fieldName = luceneFieldName(expected.getKey());
          FieldInfo fieldInfo = segmentReader.getFieldInfos().fieldInfo(fieldName);
          String formatName = fieldInfo.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY);
          String suffix = fieldInfo.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_SUFFIX_KEY);
          String vemFileName =
              IndexFileNames.segmentFileName(segmentName, formatName + "_" + suffix, "vem");

          IndexHeader vemHeader = readIndexHeader(directory, vemFileName);
          assertThat(vemHeader.codecName()).isEqualTo("Lucene99HnswVectorsFormatMeta");
          assertWithMessage(".vem version for %s under %s", expected.getKey(), writeFormat)
              .that(vemHeader.version())
              .isEqualTo(expected.getValue());
        }
      }
    }
  }

  private static void writeSingleSegment(Directory directory, LuceneCodec.WriteFormat writeFormat)
      throws IOException {
    LuceneCodec codec = new LuceneCodec(writeFormat);
    IndexWriterConfig config =
        new IndexWriterConfig()
            .setCodec(codec)
            .setIndexCreatedVersionMajor(codec.getIndexCreatedVersionMajor())
            // Keep per-format files as raw segment files so their headers can be parsed.
            .setUseCompoundFile(false);
    Random random = new Random(42);
    try (IndexWriter writer = new IndexWriter(directory, config)) {
      Document doc = new Document();
      doc.add(new StringField(KEYWORD_FIELD, "doc1", Field.Store.YES));
      doc.add(new IntPoint(POINT_FIELD, 42));
      doc.add(
          new KnnFloatVectorField(
              luceneFieldName(KNN_VECTOR),
              randomFloatVector(random),
              VectorSimilarityFunction.EUCLIDEAN));
      doc.add(
          new KnnByteVectorField(
              luceneFieldName(KNN_BYTE),
              randomByteVector(random, BYTE_DIMS),
              VectorSimilarityFunction.EUCLIDEAN));
      doc.add(
          new KnnByteVectorField(
              luceneFieldName(KNN_BIT),
              randomByteVector(random, BIT_DIM_BYTES),
              VectorSimilarityFunction.EUCLIDEAN));
      doc.add(
          new KnnFloatVectorField(
              luceneFieldName(KNN_F32_Q7),
              randomFloatVector(random),
              VectorSimilarityFunction.EUCLIDEAN));
      doc.add(
          new KnnFloatVectorField(
              luceneFieldName(KNN_F32_Q1),
              randomFloatVector(random),
              VectorSimilarityFunction.EUCLIDEAN));
      writer.addDocument(doc);
      writer.commit();
    }
  }

  private static String luceneFieldName(FieldName.TypeField typeField) {
    return typeField.getLuceneFieldName(FieldPath.parse("vector"), Optional.empty());
  }

  private record IndexHeader(String codecName, int version) {}

  /**
   * Parses a Lucene index-file header written by {@link CodecUtil#writeIndexHeader}: big-endian
   * magic, the codec name string, then the big-endian version.
   */
  private static IndexHeader readIndexHeader(Directory directory, String fileName)
      throws IOException {
    try (IndexInput in = directory.openInput(fileName, IOContext.READONCE)) {
      int magic = readBigEndianInt(in);
      assertWithMessage("codec magic of %s", fileName).that(magic).isEqualTo(CodecUtil.CODEC_MAGIC);
      String codecName = in.readString();
      int version = readBigEndianInt(in);
      return new IndexHeader(codecName, version);
    }
  }

  private static int readBigEndianInt(IndexInput in) throws IOException {
    return ((in.readByte() & 0xFF) << 24)
        | ((in.readByte() & 0xFF) << 16)
        | ((in.readByte() & 0xFF) << 8)
        | (in.readByte() & 0xFF);
  }

  private static float[] randomFloatVector(Random random) {
    float[] vector = new float[FLOAT_DIMS];
    for (int i = 0; i < vector.length; i++) {
      vector[i] = random.nextFloat() * 2 - 1;
    }
    return vector;
  }

  private static byte[] randomByteVector(Random random, int dims) {
    byte[] vector = new byte[dims];
    random.nextBytes(vector);
    return vector;
  }
}
