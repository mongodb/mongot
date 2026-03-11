package com.xgen.mongot.index.lucene.quantization;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.lucene.document.context.IndexingPolicyBuilderContext;
import com.xgen.mongot.index.lucene.document.single.IndexableFieldFactory;
import com.xgen.mongot.index.lucene.document.single.VectorIndexDocumentWrapper;
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
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
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

  private static final String FIELD =
      FieldName.TypeField.KNN_F32_Q1.getLuceneFieldName(FieldPath.parse("field"), Optional.empty());

  private final IndexWriterConfig mongotConfig = LuceneIndexRule.getIndexWriterConfig();

  @Test
  public void mergeSegments_withGhostFields_doesNotThrow() throws IOException {
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
                .fieldInfo(FIELD)); // Check to see if fieldInfo is actually present
      }
    }
  }

  @Theory
  public void mergeSegments_preservesQuantization(boolean sparse, VectorSimilarity similarity)
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
      List<BytesRef> initialValues = getAllVectors(DirectoryReader.open(w), FIELD);

      w.forceMerge(1);

      List<BytesRef> postMergeValues = getAllVectors(DirectoryReader.open(w), FIELD);
      assertThat(postMergeValues).containsExactlyElementsIn(initialValues);
      assertEquals(numVectors, initialValues.size());
      assertThat(DirectoryReader.open(w).leaves()).hasSize(1);
    }
  }

  /** Return a list of all quantized vector values from all segments in the index. */
  private static List<BytesRef> getAllVectors(IndexReader reader, String field) throws Exception {
    List<BytesRef> result = new ArrayList<>();

    for (var leaf : reader.leaves()) {
      LeafReader leafReader = leaf.reader();
      if (leafReader.getFloatVectorValues(field) == null) {
        continue;
      }

      QuantizedByteVectorValues vectorValues =
          VectorTestUtils.getQuantizedReader(leafReader, field);
      for (int i = 0; i < vectorValues.size(); ++i) {
        vectorValues.nextDoc();
        byte[] copy = vectorValues.vectorValue().clone();
        result.add(new BytesRef(copy));
      }
    }
    return result;
  }
}
