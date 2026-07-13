package com.xgen.mongot.index.lucene.quantization;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
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
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.LuceneIndexRule;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.util.VectorTestUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KnnVectorValues.DocIndexIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * OffHeapQuantizedByteVectorValues is an abstract class with differing implementations for dense
 * and sparse vector fields. This class tests both via the `sparse` parameter.
 */
@RunWith(Parameterized.class)
public class OffHeapQuantizedByteVectorValuesTest {
  /** Dummy encoded bytes for document ID field. */
  private static final byte[] DUMMY_ENCODED_BYTES = new byte[8];

  /** The vector field as seen by the user. */
  private static final FieldPath FIELD_PATH = FieldPath.newRoot("field");

  /** The lucene field of the vector. */
  private static final String FIELD =
      FieldName.TypeField.KNN_F32_Q1.getLuceneFieldName(FIELD_PATH, Optional.empty());

  /** Dimension of generated vectors */
  public static final int VEC_DIM = 16;

  /** Number of documents with a vector value */
  public static final int NUM_VECS = 10;

  private Directory dir;

  private OffHeapQuantizedByteVectorValues vectorValues;

  @Parameterized.Parameter public TestSpec spec;

  public record TestSpec(boolean sparse, VectorSimilarity similarity) {}

  @Parameterized.Parameters(name = "{0}")
  public static List<TestSpec> params() {
    ArrayList<TestSpec> specs = new ArrayList<>();
    for (VectorSimilarity s : VectorSimilarity.values()) {
      for (boolean sparse : List.of(true, false)) {
        specs.add(new TestSpec(sparse, s));
      }
    }
    return specs;
  }

  @Before
  public void before() throws IOException {
    this.dir = LuceneIndexRule.newDirectoryForTest();

    try (IndexWriter w = new IndexWriter(this.dir, LuceneIndexRule.getIndexWriterConfig())) {
      for (int i = 0; i < NUM_VECS; ++i) {
        float[] vector = VectorTestUtils.createFloatVector(VEC_DIM);

        VectorIndexDocumentWrapper document =
            VectorIndexDocumentWrapper.createRoot(
                DUMMY_ENCODED_BYTES,
                SearchIndexCapabilities.CURRENT,
                new IndexMetricsUpdater.IndexingMetricsUpdater(
                    SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
                IndexingPolicyBuilderContext.builder().build());
        IndexableFieldFactory.addKnnVectorField(
            document,
            FIELD_PATH,
            Vector.fromFloats(vector, FloatVector.OriginalType.NATIVE),
            new VectorFieldSpecification(
                vector.length,
                this.spec.similarity,
                VectorQuantization.BINARY,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));
        w.addDocument(document.luceneDocument);

        if (this.spec.sparse) {
          w.addDocument(List.of(new StringField("_id", "1", Field.Store.NO)));
        }
      }

      w.commit();
    }
    var reader = Iterables.getOnlyElement(DirectoryReader.open(this.dir).leaves()).reader();
    this.vectorValues =
        (OffHeapQuantizedByteVectorValues) VectorTestUtils.getQuantizedReader(reader, FIELD);
  }

  @After
  public void after() throws IOException {
    this.dir.close();
  }

  @Test
  public void docID_uninitialized_returnsNegativeOne() {
    DocIndexIterator iter = this.vectorValues.iterator();
    int currentDoc = iter.docID();

    assertEquals(-1, currentDoc);
  }

  @Test
  public void docID_afterNextDoc_returnsSameDocId() throws IOException {
    DocIndexIterator iter = this.vectorValues.iterator();
    int firstDoc = iter.nextDoc();
    int currentDoc = iter.docID();

    assertEquals(0, firstDoc);
    assertEquals(firstDoc, currentDoc);
  }

  @Test
  public void vectorValue_calledTwice_returnsCachedValue() throws IOException {
    DocIndexIterator iter = this.vectorValues.iterator();
    iter.nextDoc();

    byte[] ref1 = this.vectorValues.vectorValue(iter.index());
    byte[] value1 = ref1.clone();
    byte[] ref2 = this.vectorValues.vectorValue(iter.index());

    assertSame(ref1, ref2);
    assertArrayEquals(value1, ref2);
  }

  @Test
  public void vectorValue_subsequentVectors_shareBuffer() throws IOException {
    DocIndexIterator iter = this.vectorValues.iterator();
    iter.nextDoc();

    byte[] ref1 = this.vectorValues.vectorValue(iter.index());
    byte[] value1 = ref1.clone();
    iter.nextDoc();
    byte[] ref2 = this.vectorValues.vectorValue(iter.index());

    assertSame(ref1, ref2);
    assertThat(ref2).isNotEqualTo(value1);
  }

  @Test
  public void dimension_returnsNumBits() {
    int numBits = this.vectorValues.dimension();

    assertEquals(VEC_DIM, numBits);
  }

  @Test
  public void getScoreCorrectionConstant_isIdempotent() throws IOException {
    DocIndexIterator iter = this.vectorValues.iterator();
    iter.nextDoc();

    float correction = this.vectorValues.getScoreCorrectionConstant(iter.index());
    float correctionCopy = this.vectorValues.getScoreCorrectionConstant(iter.index());

    assertEquals(correction, correctionCopy, TestUtils.EPSILON);
  }

  @Test
  public void getScoreCorrectionConstant_doesNotDiscardVector() throws IOException {
    DocIndexIterator iter = this.vectorValues.iterator();
    iter.nextDoc();
    byte[] ref1 = this.vectorValues.vectorValue(iter.index());
    byte[] value1 = ref1.clone();

    this.vectorValues.getScoreCorrectionConstant(iter.index());
    byte[] ref2 = this.vectorValues.vectorValue(iter.index());

    assertSame(ref1, ref2);
    assertArrayEquals(value1, ref2);
  }

  @Test
  public void size_returnsNumVectors() {
    int numVectors = this.vectorValues.size();

    assertEquals(NUM_VECS, numVectors);
  }

  @Test
  public void scorer_withoutNextDoc_returnsNegativeOne() throws IOException {
    DocIndexIterator iter = this.vectorValues.iterator();
    iter.nextDoc();

    VectorScorer scorer = this.vectorValues.scorer(new float[VEC_DIM]);

    assertEquals(-1, scorer.iterator().docID());
  }

  @Test
  public void byteQuery_scorer_withoutNextDoc_returnsNegativeOne() throws IOException {
    DocIndexIterator iter = this.vectorValues.iterator();
    iter.nextDoc();

    VectorScorer scorer = this.vectorValues.scorer(new byte[VEC_DIM]);

    assertEquals(-1, scorer.iterator().docID());
  }

  @Test
  public void scorer_exactMatch_returnsOne() throws IOException {
    DocIndexIterator iter = this.vectorValues.iterator();
    iter.nextDoc();
    float[] query = new float[this.vectorValues.dimension()];
    // Create query vector that scores perfectly against first vector.
    BinaryQuantizationUtils.dequantize(this.vectorValues.vectorValue(iter.index()), query);

    VectorScorer scorer = this.vectorValues.scorer(query);
    scorer.iterator().nextDoc();

    assertEquals(1f, scorer.score(), TestUtils.EPSILON);
  }

  @Test
  public void byteQuery_scorer_exactMatch_returnsOne() throws IOException {
    DocIndexIterator iter = this.vectorValues.iterator();
    iter.nextDoc();
    byte[] query = new byte[this.vectorValues.dimension()];
    // Create query vector that scores perfectly against first vector.
    BinaryQuantizationUtils.dequantize(this.vectorValues.vectorValue(iter.index()), query);

    VectorScorer scorer = this.vectorValues.scorer(query);
    scorer.iterator().nextDoc();

    assertEquals(1, scorer.score(), TestUtils.EPSILON);
  }

  @Test
  public void scorer_exactInverse_returnsMinScore() throws IOException {
    DocIndexIterator iter = this.vectorValues.iterator();
    iter.nextDoc();
    float[] query = new float[this.vectorValues.dimension()];
    // Create query vector that complements first vector
    BinaryQuantizationUtils.dequantize(this.vectorValues.vectorValue(iter.index()), query);
    for (int i = 0; i < query.length; ++i) {
      query[i] *= -1;
    }

    VectorScorer scorer = this.vectorValues.scorer(query);
    scorer.iterator().nextDoc();

    // See explanation for score in BitRandomVectorScorer.
    assertEquals(1 - VEC_DIM / (VEC_DIM * 8f), scorer.score(), TestUtils.EPSILON);
  }

  @Test
  public void byteQuery_scorer_exactInverse_returnsMinScore() throws IOException {
    DocIndexIterator iter = this.vectorValues.iterator();
    iter.nextDoc();
    byte[] query = new byte[this.vectorValues.dimension()];
    // Create query vector that complements first vector
    BinaryQuantizationUtils.dequantize(this.vectorValues.vectorValue(iter.index()), query);
    for (int i = 0; i < query.length; ++i) {
      query[i] *= -1;
    }

    VectorScorer scorer = this.vectorValues.scorer(query);
    scorer.iterator().nextDoc();

    // See explanation for score in BitRandomVectorScorer.
    assertEquals(1 - VEC_DIM / (VEC_DIM * 8f), scorer.score(), TestUtils.EPSILON);
  }

  @Test
  public void scorer_scoreAllDocs_returnsValidScore() throws IOException {
    DocIndexIterator iter = this.vectorValues.iterator();
    iter.nextDoc();
    float[] query = VectorTestUtils.createUnitVector(VEC_DIM);
    VectorScorer scorer = this.vectorValues.scorer(query);

    for (var itr = scorer.iterator(); itr.nextDoc() != NO_MORE_DOCS; ) {
      assertThat(scorer.score()).isIn(Range.closed(0f, 1f));
    }
  }

  @Test
  public void byteQuery_scorer_scoreAllDocs_returnsValidScore() throws IOException {
    DocIndexIterator iter = this.vectorValues.iterator();
    iter.nextDoc();
    byte[] query = new byte[VEC_DIM];
    for (int i = 0; i < query.length; ++i) {
      query[i] = (byte) (i % 2 == 0 ? 1 : -1);
    }
    VectorScorer scorer = this.vectorValues.scorer(query);

    for (var itr = scorer.iterator(); itr.nextDoc() != NO_MORE_DOCS; ) {
      assertThat(scorer.score()).isIn(Range.closed(0f, 1f));
    }
  }
}
