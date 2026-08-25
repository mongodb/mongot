package com.xgen.mongot.index.lucene.codec;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_BIT;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_BYTE;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_F32_Q1;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_F32_Q7;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_VECTOR;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Optional;
import java.util.Random;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

/**
 * Guards mongot's write-format-dependent HNSW tiny-segment behavior.
 *
 * <p>Under {@link LuceneCodec.WriteFormat#LUCENE99}, every production vector format must write an
 * HNSW graph for every segment, no matter how small: Lucene 10.4's upstream default ({@code
 * HNSW_GRAPH_THRESHOLD} = 100) skips the graph for segments with fewer vectors, writing {@code
 * numLevels=0} metadata that pre-10.4 readers (mongot on Lucene 9.11.1/10.1.0) NPE on at query time
 * — so a rollback would crash queries. LuceneCodec therefore passes {@link
 * com.xgen.mongot.index.lucene.vector.Lucene99NativeHnswVectorsFormat#DEFAULT_HNSW_GRAPH_THRESHOLD}
 * (0) to every HNSW writer.
 *
 * <p>Under {@link LuceneCodec.WriteFormat#LUCENE104} (flag on), the rollback floor is a 10.4-jar
 * binary whose readers handle graph-less segments, so LuceneCodec uses the Lucene default
 * threshold: tiny segments skip the graph, matching vanilla 10.4 behavior.
 */
@RunWith(Theories.class)
public class LuceneCodecTinySegmentHnswGraphTest {

  /** Well below Lucene 10.4's {@code HNSW_GRAPH_THRESHOLD} of 100. */
  private static final int NUM_DOCS = 8;

  private static final int FLOAT_DIMS = 16;
  private static final int BYTE_DIMS = 16;
  private static final int BIT_DIM_BYTES = 2;

  @DataPoints
  public static final FieldName.TypeField[] hnswTypeFields = {
    KNN_VECTOR, KNN_BYTE, KNN_BIT, KNN_F32_Q7, KNN_F32_Q1
  };

  @Theory
  public void tinySegment_lucene99_everyHnswFormat_writesGraph(FieldName.TypeField typeField)
      throws IOException {
    int numLevels =
        writeTinySegmentAndGetGraphNumLevels(typeField, LuceneCodec.WriteFormat.LUCENE99);
    assertWithMessage(
            "expected an HNSW graph for %s on a %s-vector segment under LUCENE99. Graph-less"
                + " segments break rollback: 9.11.1/10.1.0 readers NPE at query time. Every HNSW"
                + " format must pass tinySegmentsThreshold=0 (DEFAULT_HNSW_GRAPH_THRESHOLD), not"
                + " Lucene 10.4's default of 100.",
            typeField, NUM_DOCS)
        .that(numLevels)
        .isAtLeast(1);
  }

  @Theory
  public void tinySegment_lucene104_everyHnswFormat_skipsGraph(FieldName.TypeField typeField)
      throws IOException {
    int numLevels =
        writeTinySegmentAndGetGraphNumLevels(typeField, LuceneCodec.WriteFormat.LUCENE104);
    assertWithMessage(
            "expected NO HNSW graph for %s on a %s-vector segment under LUCENE104: the flag-on"
                + " path uses Lucene's default tiny-segment threshold, matching vanilla 10.4"
                + " output.",
            typeField, NUM_DOCS)
        .that(numLevels)
        .isEqualTo(0);
  }

  private static int writeTinySegmentAndGetGraphNumLevels(
      FieldName.TypeField typeField, LuceneCodec.WriteFormat writeFormat) throws IOException {
    String fieldName = typeField.getLuceneFieldName(FieldPath.parse("vector"), Optional.empty());
    Random random = new Random(42);

    try (Directory directory = new ByteBuffersDirectory();
        IndexWriter writer =
            new IndexWriter(
                directory, new IndexWriterConfig().setCodec(new LuceneCodec(writeFormat)))) {
      for (int i = 0; i < NUM_DOCS; i++) {
        Document doc = new Document();
        doc.add(createVectorField(typeField, fieldName, random));
        writer.addDocument(doc);
      }

      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        assertThat(reader.leaves()).hasSize(1);
        LeafReader leaf = reader.leaves().get(0).reader();
        KnnVectorsReader fieldReader =
            ((PerFieldKnnVectorsFormat.FieldsReader) ((CodecReader) leaf).getVectorReader())
                .getFieldReader(fieldName);

        assertWithMessage("the reader for %s must expose its HNSW graph", typeField)
            .that(fieldReader)
            .isInstanceOf(HnswGraphProvider.class);
        return ((HnswGraphProvider) fieldReader).getGraph(fieldName).numLevels();
      }
    }
  }

  private static IndexableField createVectorField(
      FieldName.TypeField typeField, String fieldName, Random random) {
    return switch (typeField) {
      case KNN_VECTOR, KNN_F32_Q7, KNN_F32_Q1 ->
          new KnnFloatVectorField(
              fieldName, randomFloatVector(random), VectorSimilarityFunction.EUCLIDEAN);
      case KNN_BYTE ->
          new KnnByteVectorField(
              fieldName, randomByteVector(random, BYTE_DIMS), VectorSimilarityFunction.EUCLIDEAN);
      case KNN_BIT ->
          new KnnByteVectorField(
              fieldName,
              randomByteVector(random, BIT_DIM_BYTES),
              VectorSimilarityFunction.EUCLIDEAN);
      default -> throw new IllegalArgumentException("not an HNSW vector type field: " + typeField);
    };
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
