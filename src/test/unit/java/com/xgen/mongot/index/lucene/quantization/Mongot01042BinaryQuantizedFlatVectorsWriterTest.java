package com.xgen.mongot.index.lucene.quantization;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.lucene.document.context.IndexingPolicyBuilderContext;
import com.xgen.mongot.index.lucene.document.single.IndexableFieldFactory;
import com.xgen.mongot.index.lucene.document.single.VectorIndexDocumentWrapper;
import com.xgen.mongot.index.lucene.extension.KnnByteVectorField;
import com.xgen.mongot.index.lucene.extension.KnnFloatVectorField;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.FloatVector.OriginalType;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.LuceneIndexRule;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.util.RandomSegmentingIndexWriter;
import com.xgen.testing.util.VectorTestUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.bson.BsonInt32;
import org.junit.Test;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class Mongot01042BinaryQuantizedFlatVectorsWriterTest {
  private static final byte[] DUMMY_ENCODED_BYTES = new byte[8];

  private static final String FLOAT_FIELD =
      FieldName.TypeField.KNN_F32_Q1.getLuceneFieldName(FieldPath.parse("field"), Optional.empty());

  private static final String BYTE_FIELD =
      FieldName.TypeField.KNN_BYTE_Q1.getLuceneFieldName(
          FieldPath.parse("field"), Optional.empty());

  private final IndexWriterConfig mongotConfig = LuceneIndexRule.getIndexWriterConfig();

  @Test
  public void floatPath_mergeSegments_withGhostFields_doesNotThrow() throws IOException {
    try (Directory dir = LuceneIndexRule.newDirectoryForTest();
        IndexWriter w = new IndexWriter(dir, this.mongotConfig)) {
      float[] vector = VectorTestUtils.createFloatVector(30);

      @Var Document doc = new Document();
      doc.add(new StringField("id", "0", Field.Store.NO));
      w.addDocument(doc);

      VectorIndexDocumentWrapper wrappedDocument =
          VectorIndexDocumentWrapper.createRoot(
              LuceneDocumentIdEncoder.encodeDocumentId(new BsonInt32(1)),
              SearchIndexCapabilities.CURRENT,
              new IndexMetricsUpdater.IndexingMetricsUpdater(
                  SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
              IndexingPolicyBuilderContext.builder().build());

      wrappedDocument.luceneDocument.add(new StringField("id", "1", Field.Store.NO));
      IndexableFieldFactory.addKnnVectorField(
          wrappedDocument,
          FieldPath.newRoot("field"),
          Vector.fromFloats(vector, OriginalType.NATIVE),
          new VectorFieldSpecification(
              vector.length,
              VectorSimilarity.COSINE,
              VectorQuantization.BINARY,
              new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));
      w.addDocument(wrappedDocument.luceneDocument);
      w.deleteDocuments(new Term("id", "1"));
      w.commit();

      doc = new Document();
      doc.add(new StringField("id", "2", Field.Store.NO));
      w.addDocument(doc);
      w.commit(); // add new segment
      w.forceMerge(1); // ensure that merge does not throw

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = Iterables.getOnlyElement(reader.leaves()).reader();
        assertNotNull(
            leafReader
                .getFieldInfos()
                .fieldInfo(FLOAT_FIELD)); // Check to see if fieldInfo is actually present
      }
    }
  }

  @Theory
  public void floatPath_mergeSegments_preservesQuantization(VectorSimilarity similarity)
      throws Exception {
    int numVectors = 20;
    try (Directory dir = LuceneIndexRule.newDirectoryForTest();
        RandomSegmentingIndexWriter w = new RandomSegmentingIndexWriter(dir, this.mongotConfig)) {
      float[] vector = VectorTestUtils.createFloatVector(16);

      List<Iterable<IndexableField>> docs = new ArrayList<>();
      for (int i = 0; i < numVectors; ++i) {
        VectorIndexDocumentWrapper document =
            VectorIndexDocumentWrapper.createRoot(
                DUMMY_ENCODED_BYTES,
                SearchIndexCapabilities.CURRENT,
                new IndexMetricsUpdater.IndexingMetricsUpdater(
                    SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
                IndexingPolicyBuilderContext.builder().build());
        IndexableFieldFactory.addKnnVectorField(
            document,
            FieldPath.newRoot("field"),
            Vector.fromFloats(vector, OriginalType.NATIVE),
            new VectorFieldSpecification(
                vector.length,
                similarity,
                VectorQuantization.BINARY,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));
        docs.add(document.luceneDocument);
      }
      w.addDocuments(docs); // create randomized segments
      List<BytesRef> initialValues = getAllVectors(DirectoryReader.open(w), FLOAT_FIELD);

      w.forceMerge(1);

      List<BytesRef> postMergeValues = getAllVectors(DirectoryReader.open(w), FLOAT_FIELD);
      assertThat(postMergeValues).containsExactlyElementsIn(initialValues);
      assertEquals(numVectors, initialValues.size());
      assertThat(DirectoryReader.open(w).leaves()).hasSize(1);
    }
  }

  @Theory
  public void bytePath_mergeSegments_preservesQuantization(VectorSimilarity similarity)
      throws Exception {
    int numVectors = 20;
    try (Directory dir = LuceneIndexRule.newDirectoryForTest();
        RandomSegmentingIndexWriter w =
            new RandomSegmentingIndexWriter(
                dir, new IndexWriterConfig().setCodec(new Mongot01042BinaryQuantizedTestCodec()))) {

      List<Iterable<IndexableField>> docs = new ArrayList<>();
      for (int i = 0; i < numVectors; ++i) {
        Document document = new Document();
        byte[] vector = VectorTestUtils.createByteVector(16);
        document.add(
            new KnnByteVectorField(BYTE_FIELD, vector, similarity.getLuceneSimilarityFunction()));
        docs.add(document);
      }
      w.addDocuments(docs); // create randomized segments
      List<BytesRef> initialValues = getAllVectors(DirectoryReader.open(w), BYTE_FIELD);

      w.forceMerge(1);

      List<BytesRef> postMergeValues = getAllVectors(DirectoryReader.open(w), BYTE_FIELD);
      assertThat(postMergeValues).containsExactlyElementsIn(initialValues);
      assertEquals(numVectors, initialValues.size());
      assertThat(DirectoryReader.open(w).leaves()).hasSize(1);
    }
  }

  @Test
  public void bytePath_mergeSegments_withGhostFields_doesNotThrow() throws IOException {
    try (Directory dir = LuceneIndexRule.newDirectoryForTest();
        IndexWriter w =
            new IndexWriter(
                dir, new IndexWriterConfig().setCodec(new Mongot01042BinaryQuantizedTestCodec()))) {
      byte[] vector = VectorTestUtils.createByteVector(30);

      @Var Document doc = new Document();
      doc.add(new StringField("id", "0", Field.Store.NO));
      w.addDocument(doc);

      Document vectorDoc = new Document();
      vectorDoc.add(new StringField("id", "1", Field.Store.NO));
      vectorDoc.add(
          new KnnByteVectorField(
              BYTE_FIELD, vector, VectorSimilarity.COSINE.getLuceneSimilarityFunction()));
      w.addDocument(vectorDoc);
      w.deleteDocuments(new Term("id", "1"));
      w.commit();

      doc = new Document();
      doc.add(new StringField("id", "2", Field.Store.NO));
      w.addDocument(doc);
      w.commit(); // add new segment
      w.forceMerge(1); // ensure that merge does not throw

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = Iterables.getOnlyElement(reader.leaves()).reader();
        assertNotNull(
            leafReader
                .getFieldInfos()
                .fieldInfo(BYTE_FIELD)); // Check to see if fieldInfo is actually present
      }
    }
  }

  @Test
  public void mixedEncodings_floatAndByteFieldsInSameSegment_bothQuantizedCorrectly()
      throws Exception {
    int numVectors = 20;
    int docsPerSegment = 5;
    int dim = 16;
    float[] floatVector = VectorTestUtils.createFloatVector(dim);
    byte[] byteVector = VectorTestUtils.createByteVector(dim);

    byte[] expectedFloatQuantized = new byte[dim];
    BinaryQuantizer.INSTANCE.quantize(
        floatVector, expectedFloatQuantized, VectorSimilarity.COSINE.getLuceneSimilarityFunction());
    byte[] expectedFloatCompressed = new byte[BinaryQuantizationUtils.requiredBytes(dim)];
    BinaryQuantizationUtils.compressBytes(expectedFloatQuantized, expectedFloatCompressed);

    byte[] expectedByteQuantized = new byte[dim];
    BinaryQuantizer.quantizeBytes(byteVector, expectedByteQuantized);
    byte[] expectedByteCompressed = new byte[BinaryQuantizationUtils.requiredBytes(dim)];
    BinaryQuantizationUtils.compressBytes(expectedByteQuantized, expectedByteCompressed);

    try (Directory dir = LuceneIndexRule.newDirectoryForTest();
        IndexWriter w =
            new IndexWriter(
                dir, new IndexWriterConfig().setCodec(new Mongot01042BinaryQuantizedTestCodec()))) {

      for (int i = 0; i < numVectors; i++) {
        Document doc = new Document();
        doc.add(
            new KnnFloatVectorField(
                FLOAT_FIELD, floatVector, VectorSimilarity.COSINE.getLuceneSimilarityFunction()));
        doc.add(
            new KnnByteVectorField(
                BYTE_FIELD, byteVector, VectorSimilarity.COSINE.getLuceneSimilarityFunction()));
        w.addDocument(doc);
        if ((i + 1) % docsPerSegment == 0) {
          w.commit(); // flush a segment every docsPerSegment docs
        }
      }
      w.forceMerge(1);

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = Iterables.getOnlyElement(reader.leaves()).reader();
        assertQuantizedVectorsMatch(leafReader, FLOAT_FIELD, expectedFloatCompressed);
        assertQuantizedVectorsMatch(leafReader, BYTE_FIELD, expectedByteCompressed);
      }
    }
  }

  @Test
  public void bytePath_mergeSegments_withDeletedDocs_preservesSurvivingVectors() throws Exception {
    int dim = 16;
    int numDocs = 6;
    int docsPerSegment = 3;

    try (Directory dir = LuceneIndexRule.newDirectoryForTest();
        IndexWriter w =
            new IndexWriter(
                dir, new IndexWriterConfig().setCodec(new Mongot01042BinaryQuantizedTestCodec()))) {

      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", String.valueOf(i), Field.Store.NO));
        doc.add(
            new KnnByteVectorField(
                BYTE_FIELD,
                VectorTestUtils.idEncodingByteVector(i, dim),
                VectorSimilarity.COSINE.getLuceneSimilarityFunction()));
        w.addDocument(doc);
        if ((i + 1) % docsPerSegment == 0) {
          w.commit(); // flush a segment every docsPerSegment docs
        }
      }

      List<Integer> survivorDocs = deleteRandomHalf(w, numDocs);
      w.forceMerge(1);

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = Iterables.getOnlyElement(reader.leaves()).reader();
        ByteVectorValues vectorValues = leafReader.getByteVectorValues(BYTE_FIELD);
        QuantizedByteVectorValues quantized =
            VectorTestUtils.getQuantizedReader(leafReader, BYTE_FIELD);
        // Each surviving vector self-identifies via its encoded signs, so decoding the merged raw
        // vectors back to ids proves exactly which docs survived
        List<Integer> survivorIds = new ArrayList<>();
        ByteVectorValues.DocIndexIterator iter = vectorValues.iterator();
        for (int doc = iter.nextDoc(); doc != NO_MORE_DOCS; doc = iter.nextDoc()) {
          int id = VectorTestUtils.decodeId(vectorValues.vectorValue(iter.index()));
          survivorIds.add(id);

          byte[] expectedQuantized = new byte[dim];
          BinaryQuantizer.quantizeBytes(
              VectorTestUtils.idEncodingByteVector(id, dim), expectedQuantized);
          byte[] expectedCompressed = new byte[BinaryQuantizationUtils.requiredBytes(dim)];
          BinaryQuantizationUtils.compressBytes(expectedQuantized, expectedCompressed);
          assertArrayEquals(expectedCompressed, quantized.vectorValue(iter.index()));
        }
        assertThat(survivorIds).containsExactlyElementsIn(survivorDocs);
      }
    }
  }

  @Test
  public void floatPath_mergeSegments_withDeletedDocs_preservesSurvivingVectors() throws Exception {
    int dim = 16;
    int numDocs = 6;
    int docsPerSegment = 3;

    try (Directory dir = LuceneIndexRule.newDirectoryForTest();
        IndexWriter w =
            new IndexWriter(
                dir, new IndexWriterConfig().setCodec(new Mongot01042BinaryQuantizedTestCodec()))) {

      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", String.valueOf(i), Field.Store.NO));
        doc.add(
            new KnnFloatVectorField(
                FLOAT_FIELD,
                VectorTestUtils.idEncodingFloatVector(i, dim),
                VectorSimilarity.COSINE.getLuceneSimilarityFunction()));
        w.addDocument(doc);
        if ((i + 1) % docsPerSegment == 0) {
          w.commit(); // flush a segment every docsPerSegment docs
        }
      }

      List<Integer> survivorDocs = deleteRandomHalf(w, numDocs);
      w.forceMerge(1);

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = Iterables.getOnlyElement(reader.leaves()).reader();
        FloatVectorValues vectorValues = leafReader.getFloatVectorValues(FLOAT_FIELD);
        QuantizedByteVectorValues quantized =
            VectorTestUtils.getQuantizedReader(leafReader, FLOAT_FIELD);
        List<Integer> survivorIds = new ArrayList<>();
        FloatVectorValues.DocIndexIterator iter = vectorValues.iterator();
        for (int doc = iter.nextDoc(); doc != NO_MORE_DOCS; doc = iter.nextDoc()) {
          int id = VectorTestUtils.decodeId(vectorValues.vectorValue(iter.index()));
          survivorIds.add(id);

          byte[] expectedQuantized = new byte[dim];
          BinaryQuantizer.INSTANCE.quantize(
              VectorTestUtils.idEncodingFloatVector(id, dim),
              expectedQuantized,
              VectorSimilarity.COSINE.getLuceneSimilarityFunction());
          byte[] expectedCompressed = new byte[BinaryQuantizationUtils.requiredBytes(dim)];
          BinaryQuantizationUtils.compressBytes(expectedQuantized, expectedCompressed);
          assertArrayEquals(expectedCompressed, quantized.vectorValue(iter.index()));
        }
        assertThat(survivorIds).containsExactlyElementsIn(survivorDocs);
      }
    }
  }

  /** Deletes a random half of ids [0, numDocs), commits, and returns the surviving ids. */
  private static List<Integer> deleteRandomHalf(IndexWriter w, int numDocs) throws IOException {
    List<Integer> docIds = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      docIds.add(i);
    }
    Collections.shuffle(docIds);
    List<Integer> survivorDocs = docIds.subList(0, numDocs / 2);
    List<Integer> deletedDocs = docIds.subList(numDocs / 2, numDocs);

    for (int docId : deletedDocs) {
      w.deleteDocuments(new Term("id", String.valueOf(docId)));
    }
    w.commit();
    return survivorDocs;
  }

  /**
   * Asserts quantized vector values match, and that the quantized vector values score correction
   * constant is 0
   */
  private static void assertQuantizedVectorsMatch(
      LeafReader leafReader, String field, byte[] expectedCompressed) throws IOException {
    QuantizedByteVectorValues quantizedByteVectorValues =
        VectorTestUtils.getQuantizedReader(leafReader, field);
    var iter = quantizedByteVectorValues.iterator();
    for (int i = 0; i < quantizedByteVectorValues.size(); i++) {
      iter.nextDoc();
      assertArrayEquals(expectedCompressed, quantizedByteVectorValues.vectorValue(iter.index()));
      assertEquals(0f, quantizedByteVectorValues.getScoreCorrectionConstant(iter.index()), 0f);
    }
  }

  /** Return a list of all quantized vector values from all segments in the index. */
  private static List<BytesRef> getAllVectors(IndexReader reader, String field) throws Exception {
    List<BytesRef> result = new ArrayList<>();

    for (var leaf : reader.leaves()) {
      LeafReader leafReader = leaf.reader();
      if (leafReader.getFloatVectorValues(field) == null
          && leafReader.getByteVectorValues(field) == null) {
        continue;
      }

      QuantizedByteVectorValues vectorValues =
          VectorTestUtils.getQuantizedReader(leafReader, field);
      var iter = vectorValues.iterator();
      for (int i = 0; i < vectorValues.size(); ++i) {
        iter.nextDoc();
        byte[] copy = vectorValues.vectorValue(iter.index()).clone();
        result.add(new BytesRef(copy));
      }
    }
    return result;
  }
}
