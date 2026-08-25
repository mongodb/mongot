package com.xgen.mongot.index.lucene.codec;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_BIT;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_BYTE;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_F32_Q1;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_F32_Q7;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.KNN_VECTOR;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.Test;

/**
 * Rollback-semantics coverage for {@link LuceneCodec.WriteFormat}:
 *
 * <ul>
 *   <li><b>Flag-on round-trip:</b> segments written under {@code LUCENE104} (all five KNN type
 *       fields, points, stored fields) are readable through a plain {@link DirectoryReader} — i.e.
 *       via SPI codec resolution from segment metadata, not the writing codec instance — including
 *       after a {@code forceMerge}.
 *   <li><b>Mixed-format index:</b> an index holding a {@code LUCENE99} segment and a {@code
 *       LUCENE104} segment side by side is readable, and merging under either write format rewrites
 *       it to that format.
 *   <li><b>Flag flip-back:</b> disabling the flag after {@code LUCENE104} segments exist requires
 *       no rebuild — the existing segments stay readable and new segments are written as {@code
 *       Lucene99} (the supported flag OFF path on &gt;= 10.4-jar binaries). The index-wide
 *       created-10 stamp persists, however, so an index <i>created</i> under the flag remains
 *       unopenable below Lucene-10 jars forever; only a re-index clears it.
 * </ul>
 */
public class LuceneCodecWriteFormatRoundTripTest {

  private static final String ID_FIELD = "id";
  private static final String POINT_FIELD = "point";

  private static final int FLOAT_DIMS = 16;
  private static final int BYTE_DIMS = 16;
  private static final int BIT_DIM_BYTES = 2;

  @Test
  public void lucene104_allKnnTypesPointsAndStored_roundTripsThroughSpiResolution()
      throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = newWriter(directory, LuceneCodec.WriteFormat.LUCENE104)) {
        writer.addDocument(fullDocument("doc1", 1));
        writer.commit();
        writer.addDocument(fullDocument("doc2", 2));
        writer.commit();
      }

      assertAllDocsReadable(directory, List.of("doc1", "doc2"));
      assertKnnFieldsReadable(directory);
      assertThat(createdVersionMajor(directory)).isEqualTo(10);

      try (IndexWriter writer = newWriter(directory, LuceneCodec.WriteFormat.LUCENE104)) {
        writer.forceMerge(1);
        writer.commit();
      }

      assertThat(codecNamesPerSegment(directory)).containsExactly("Lucene104");
      assertAllDocsReadable(directory, List.of("doc1", "doc2"));
      assertKnnFieldsReadable(directory);
    }
  }

  @Test
  public void mixedFormatSegments_readableAndMergeableUnderEitherWriteFormat() throws IOException {
    for (LuceneCodec.WriteFormat mergeFormat : LuceneCodec.WriteFormat.values()) {
      try (Directory directory = new ByteBuffersDirectory()) {
        try (IndexWriter writer = newWriter(directory, LuceneCodec.WriteFormat.LUCENE99)) {
          writer.addDocument(fullDocument("doc1", 1));
          writer.commit();
        }
        try (IndexWriter writer = newWriter(directory, LuceneCodec.WriteFormat.LUCENE104)) {
          writer.addDocument(fullDocument("doc2", 2));
          writer.commit();
        }

        assertThat(codecNamesPerSegment(directory)).containsExactly("Lucene99", "Lucene104");
        assertAllDocsReadable(directory, List.of("doc1", "doc2"));
        assertKnnFieldsReadable(directory);

        try (IndexWriter writer = newWriter(directory, mergeFormat)) {
          writer.forceMerge(1);
          writer.commit();
        }

        assertThat(codecNamesPerSegment(directory))
            .containsExactly(new LuceneCodec(mergeFormat).getName());
        assertAllDocsReadable(directory, List.of("doc1", "doc2"));
        assertKnnFieldsReadable(directory);
        // The created version is index-wide and immutable: set at creation (LUCENE99 here),
        // unaffected by later write formats or merges.
        assertThat(createdVersionMajor(directory)).isEqualTo(9);
      }
    }
  }

  @Test
  public void flagFlipBack_lucene104SegmentsStayReadable_newSegmentsAreLucene99()
      throws IOException {
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = newWriter(directory, LuceneCodec.WriteFormat.LUCENE104)) {
        writer.addDocument(fullDocument("doc1", 1));
        writer.commit();
      }

      // Reopen with the flag off: no rebuild — the writer appends to the existing index and the
      // Lucene104 segment remains readable alongside the new Lucene99 one.
      try (IndexWriter writer = newWriter(directory, LuceneCodec.WriteFormat.LUCENE99)) {
        writer.addDocument(fullDocument("doc2", 2));
        writer.commit();
      }

      assertThat(codecNamesPerSegment(directory)).containsExactly("Lucene104", "Lucene99");
      assertAllDocsReadable(directory, List.of("doc1", "doc2"));
      assertKnnFieldsReadable(directory);
      // The created-10 stamp is permanent: flipping the flag back does not restore
      // pre-Lucene-10 openability for indexes created while it was on.
      assertThat(createdVersionMajor(directory)).isEqualTo(10);
    }
  }

  private static IndexWriter newWriter(Directory directory, LuceneCodec.WriteFormat writeFormat)
      throws IOException {
    LuceneCodec codec = new LuceneCodec(writeFormat);
    return new IndexWriter(
        directory,
        new IndexWriterConfig()
            .setCodec(codec)
            .setIndexCreatedVersionMajor(codec.getIndexCreatedVersionMajor()));
  }

  private static int createdVersionMajor(Directory directory) throws IOException {
    return SegmentInfos.readLatestCommit(directory).getIndexCreatedVersionMajor();
  }

  private static Document fullDocument(String id, int seed) {
    Random random = new Random(seed);
    Document doc = new Document();
    doc.add(new StringField(ID_FIELD, id, Field.Store.YES));
    doc.add(new IntPoint(POINT_FIELD, seed));
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
    return doc;
  }

  /** Codec names recorded in segment metadata, in segment order. */
  private static List<String> codecNamesPerSegment(Directory directory) throws IOException {
    return SegmentInfos.readLatestCommit(directory).asList().stream()
        .map(info -> info.info.getCodec().getName())
        .toList();
  }

  private static void assertAllDocsReadable(Directory directory, List<String> expectedIds)
      throws IOException {
    try (DirectoryReader reader = DirectoryReader.open(directory)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      TopDocs topDocs = searcher.search(MatchAllDocsQuery.INSTANCE, expectedIds.size() + 1);
      assertThat(topDocs.totalHits.value()).isEqualTo(expectedIds.size());
      assertThat(searcher.count(IntPoint.newRangeQuery(POINT_FIELD, 1, expectedIds.size())))
          .isEqualTo(expectedIds.size());
      // Decode each hit's stored document: stored-fields decompression is lazy, so a successful
      // reader open does not prove the stored bytes are decodable by the SPI-resolved codec.
      StoredFields storedFields = searcher.storedFields();
      for (String id : expectedIds) {
        TopDocs hit = searcher.search(new TermQuery(new Term(ID_FIELD, id)), 1);
        assertThat(hit.totalHits.value()).isEqualTo(1);
        assertThat(storedFields.document(hit.scoreDocs[0].doc).get(ID_FIELD)).isEqualTo(id);
      }
    }
  }

  /** Every KNN field must expose vector values in every segment through plain SPI reads. */
  private static void assertKnnFieldsReadable(Directory directory) throws IOException {
    try (DirectoryReader reader = DirectoryReader.open(directory)) {
      for (LeafReaderContext ctx : reader.leaves()) {
        SegmentReader leaf = (SegmentReader) ctx.reader();
        for (FieldName.TypeField typeField : List.of(KNN_VECTOR, KNN_F32_Q7, KNN_F32_Q1)) {
          assertThat(leaf.getFloatVectorValues(luceneFieldName(typeField))).isNotNull();
        }
        for (FieldName.TypeField typeField : List.of(KNN_BYTE, KNN_BIT)) {
          assertThat(leaf.getByteVectorValues(luceneFieldName(typeField))).isNotNull();
        }
      }
    }
  }

  private static String luceneFieldName(FieldName.TypeField typeField) {
    return typeField.getLuceneFieldName(FieldPath.parse("vector"), Optional.empty());
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
