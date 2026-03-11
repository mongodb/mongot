package com.xgen.mongot.index.lucene.quantization;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Iterables;
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
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.LuceneIndexRule;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import com.xgen.testing.util.VectorTestUtils;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.bson.BsonInt32;
import org.junit.Assert;
import org.junit.Test;

public class Mongot01042BinaryQuantizedFlatVectorsReaderTest {
  private static final byte[] DUMMY_ENCODED_BYTES = new byte[8];

  @Test
  public void getFloatVectorValues_nonExistentField_throwsIllegalArgumentException()
      throws IOException {
    String existingField =
        FieldName.TypeField.KNN_F32_Q1.getLuceneFieldName(
            FieldPath.parse("existingField"), Optional.empty());
    try (Directory dir = LuceneIndexRule.newDirectoryForTest();
        IndexWriter w = new IndexWriter(dir, LuceneIndexRule.getIndexWriterConfig())) {
      float[] vector = VectorTestUtils.createFloatVector(30);

      VectorIndexDocumentWrapper document =
          VectorIndexDocumentWrapper.createRoot(
              DUMMY_ENCODED_BYTES,
              SearchIndexCapabilities.CURRENT,
              new IndexMetricsUpdater.IndexingMetricsUpdater(
                  SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
              IndexingPolicyBuilderContext.builder().build());

      IndexableFieldFactory.addKnnVectorField(
          document,
          FieldPath.newRoot("existingField"),
          Vector.fromFloats(vector, FloatVector.OriginalType.NATIVE),
          new VectorFieldSpecification(
              vector.length,
              VectorSimilarity.COSINE,
              VectorQuantization.BINARY,
              new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));
      w.addDocument(document.luceneDocument);
      w.commit();

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = Iterables.getOnlyElement(reader.leaves()).reader();
        KnnVectorsReader knnVectorsReader = ((CodecReader) leafReader).getVectorReader();
        KnnVectorsReader forField =
            ((PerFieldKnnVectorsFormat.FieldsReader) knnVectorsReader)
                .getFieldReader(existingField);
        Lucene99HnswVectorsReader hnswReader =
            Check.instanceOf(forField, Lucene99HnswVectorsReader.class);
        IllegalArgumentException exception =
            Assert.assertThrows(
                IllegalArgumentException.class,
                () -> hnswReader.getFloatVectorValues("missingField"));
        assertThat(exception).hasMessageThat().contains("not found");
      }
    }
  }

  @Test
  public void getQuantizedVectorValues_nonExistentField_throwsIllegalArgumentException()
      throws IOException {
    String existingField =
        FieldName.TypeField.KNN_F32_Q1.getLuceneFieldName(
            FieldPath.parse("existingField"), Optional.empty());
    try (Directory dir = LuceneIndexRule.newDirectoryForTest();
        IndexWriter w = new IndexWriter(dir, LuceneIndexRule.getIndexWriterConfig())) {
      float[] vector = VectorTestUtils.createFloatVector(30);

      VectorIndexDocumentWrapper document =
          VectorIndexDocumentWrapper.createRoot(
              DUMMY_ENCODED_BYTES,
              SearchIndexCapabilities.CURRENT,
              new IndexMetricsUpdater.IndexingMetricsUpdater(
                  SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
              IndexingPolicyBuilderContext.builder().build());

      IndexableFieldFactory.addKnnVectorField(
          document,
          FieldPath.newRoot("existingField"),
          Vector.fromFloats(vector, FloatVector.OriginalType.NATIVE),
          new VectorFieldSpecification(
              vector.length,
              VectorSimilarity.COSINE,
              VectorQuantization.BINARY,
              new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));
      w.addDocument(document.luceneDocument);
      w.commit();

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = Iterables.getOnlyElement(reader.leaves()).reader();
        KnnVectorsReader knnVectorsReader = ((CodecReader) leafReader).getVectorReader();
        KnnVectorsReader forField =
            ((PerFieldKnnVectorsFormat.FieldsReader) knnVectorsReader)
                .getFieldReader(existingField);
        Lucene99HnswVectorsReader hnswReader =
            Check.instanceOf(forField, Lucene99HnswVectorsReader.class);
        IllegalArgumentException exception =
            Assert.assertThrows(
                IllegalArgumentException.class,
                () -> hnswReader.getQuantizedVectorValues("missingField"));
        assertThat(exception).hasMessageThat().contains("not found");
      }
    }
  }

  @Test
  public void getQuantizationState_nonExistentField_throwsIllegalArgumentException()
      throws IOException {
    String existingField =
        FieldName.TypeField.KNN_F32_Q1.getLuceneFieldName(
            FieldPath.parse("existingField"), Optional.empty());
    try (Directory dir = LuceneIndexRule.newDirectoryForTest();
        IndexWriter w = new IndexWriter(dir, LuceneIndexRule.getIndexWriterConfig())) {
      float[] vector = VectorTestUtils.createFloatVector(30);

      VectorIndexDocumentWrapper document =
          VectorIndexDocumentWrapper.createRoot(
              DUMMY_ENCODED_BYTES,
              SearchIndexCapabilities.CURRENT,
              new IndexMetricsUpdater.IndexingMetricsUpdater(
                  SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
              IndexingPolicyBuilderContext.builder().build());

      IndexableFieldFactory.addKnnVectorField(
          document,
          FieldPath.newRoot("existingField"),
          Vector.fromFloats(vector, FloatVector.OriginalType.NATIVE),
          new VectorFieldSpecification(
              vector.length,
              VectorSimilarity.COSINE,
              VectorQuantization.BINARY,
              new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));
      w.addDocument(document.luceneDocument);
      w.commit();

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = Iterables.getOnlyElement(reader.leaves()).reader();
        KnnVectorsReader knnVectorsReader = ((CodecReader) leafReader).getVectorReader();
        KnnVectorsReader forField =
            ((PerFieldKnnVectorsFormat.FieldsReader) knnVectorsReader)
                .getFieldReader(existingField);
        Lucene99HnswVectorsReader hnswReader =
            Check.instanceOf(forField, Lucene99HnswVectorsReader.class);
        IllegalArgumentException exception =
            Assert.assertThrows(
                IllegalArgumentException.class,
                () -> hnswReader.getQuantizationState("missingField"));
        assertThat(exception).hasMessageThat().contains("not found");
      }
    }
  }

  @Test
  public void getFloatVectorValues_ghostFields_doesNotReturnNull() throws IOException {
    // Test case adapted from
    // https://github.com/apache/lucene/blob/4b47fb1a3113d22bca6cd8c1664529ef2d7f4877/lucene/core/src/test/org/apache/lucene/search/BaseKnnVectorQueryTestCase.java#L792-L827
    String field =
        FieldName.TypeField.KNN_F32_Q1.getLuceneFieldName(
            FieldPath.parse("field"), Optional.empty());
    try (Directory dir = LuceneIndexRule.newDirectoryForTest();
        IndexWriter w = new IndexWriter(dir, LuceneIndexRule.getIndexWriterConfig())) {
      float[] vector = VectorTestUtils.createFloatVector(30);

      Document doc = new Document();
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
          Vector.fromFloats(vector, FloatVector.OriginalType.NATIVE),
          new VectorFieldSpecification(
              vector.length,
              VectorSimilarity.COSINE,
              VectorQuantization.BINARY,
              new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));
      w.addDocument(wrappedDocument.luceneDocument);
      w.commit();
      w.deleteDocuments(new Term("id", "1"));
      w.forceMerge(1);

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = Iterables.getOnlyElement(reader.leaves()).reader();
        FieldInfo fi = leafReader.getFieldInfos().fieldInfo(field);
        FloatVectorValues vectorValues =
            switch (fi.getVectorEncoding()) {
              case BYTE -> throw new AssertionError();
              case FLOAT32 -> leafReader.getFloatVectorValues(field);
            };
        assertNotNull(vectorValues);
        assertEquals(NO_MORE_DOCS, vectorValues.nextDoc());
      }
    }
  }

  @Test
  public void getQuantizedVectorValues_ghostFields_doesNotReturnNull() throws IOException {
    // Test case adapted from
    // https://github.com/apache/lucene/blob/4b47fb1a3113d22bca6cd8c1664529ef2d7f4877/lucene/core/src/test/org/apache/lucene/search/BaseKnnVectorQueryTestCase.java#L792-L827
    String field =
        FieldName.TypeField.KNN_F32_Q1.getLuceneFieldName(
            FieldPath.parse("field"), Optional.empty());
    try (Directory dir = LuceneIndexRule.newDirectoryForTest();
        IndexWriter w = new IndexWriter(dir, LuceneIndexRule.getIndexWriterConfig())) {
      float[] vector = VectorTestUtils.createFloatVector(30);

      Document doc = new Document();
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
          Vector.fromFloats(vector, FloatVector.OriginalType.NATIVE),
          new VectorFieldSpecification(
              vector.length,
              VectorSimilarity.EUCLIDEAN,
              VectorQuantization.BINARY,
              new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));
      w.addDocument(wrappedDocument.luceneDocument);
      w.commit();
      w.deleteDocuments(new Term("id", "1"));
      w.forceMerge(1);

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = Iterables.getOnlyElement(reader.leaves()).reader();
        QuantizedByteVectorValues quantized = VectorTestUtils.getQuantizedReader(leafReader, field);

        assertNotNull(quantized);
        assertThat(quantized).isInstanceOf(OffHeapQuantizedByteVectorValues.class);
        assertEquals(NO_MORE_DOCS, quantized.nextDoc());
      }
    }
  }
}
