package com.xgen.mongot.index.lucene.document.single;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.BSON;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static org.apache.lucene.facet.taxonomy.FacetLabel.MAX_CATEGORY_PATH_LENGTH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.NumericFieldOptions.Representation;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.lucene.document.context.IndexingPolicyBuilderContext;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.LoggableIdUtils;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;

@RunWith(Theories.class)
public class IndexableFieldFactoryTest {

  @BeforeClass
  public static void setUpClass() {
    // Enable the loggable document ID feature for tests
    LoggableIdUtils.initialize(true);
  }

  private static final byte[] DUMMY_ENCODED_BYTES = { // corresponds to BsonInt32(13)
    14, 0, 0, 0, 16, 95, 105, 100, 0, 13, 0, 0, 0, 0
  };

  // Logger that directly connects to logBack of IndexableFieldFactory to verify that flogger is
  // routing through logBack
  private static final ch.qos.logback.classic.Logger indexableFieldFactoryLogger =
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(IndexableFieldFactory.class);

  private static final String facetPath = "path";

  @DataPoints("validFacet")
  public static final ImmutableList<String> validFacets =
      ImmutableList.of(
          "a",
          "a".repeat(MAX_CATEGORY_PATH_LENGTH - facetPath.length() - 1), // 1 char, 1 byte
          "å".repeat(MAX_CATEGORY_PATH_LENGTH - facetPath.length() - 1), // 1 char, 2 bytes
          "🤦".repeat((MAX_CATEGORY_PATH_LENGTH - facetPath.length() - 1) / 2) // 2 chars, 4 bytes
      );

  @DataPoints("invalidFacet")
  public static final ImmutableList<String> invalidFacets =
      ImmutableList.of(
          "a".repeat(MAX_CATEGORY_PATH_LENGTH), // 1 char, 1 byte
          "å".repeat(MAX_CATEGORY_PATH_LENGTH), // 1 char, 2 bytes
          "🤦".repeat(MAX_CATEGORY_PATH_LENGTH / 2), // 2 chars, 4 bytes
          "å".repeat(MAX_CATEGORY_PATH_LENGTH - facetPath.length() - 2) + "🤦");

  /**
   * Ensures that when a FieldName exceeds the allowable character limit, a flogger records a
   * warning log, which is both rate-limited and routed through logBack.
   */
  @Test
  public void testFieldNameExceedsLength() {
    // syntax necessary to track logs that route through logBack
    ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
    listAppender.start();
    indexableFieldFactoryLogger.addAppender(listAppender);
    DocumentWrapper myWrapper =
        DocumentWrapper.createRootStandalone(
            DUMMY_ENCODED_BYTES,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH));
    FieldPath longPath = FieldPath.newRoot("a".repeat(9001));
    IndexableFieldFactory.addFieldNamesField(myWrapper, longPath);
    IndexableFieldFactory.addFieldNamesField(myWrapper, longPath);
    List<ILoggingEvent> list = listAppender.list;
    String expectedLogFragment = "Unable to index a field name due to its length";
    long count =
        list.stream()
            .map(ILoggingEvent::getMessage)
            .filter(str -> str.contains(expectedLogFragment))
            .count();

    // two very-long messages sent, only the first should be logged due to rate-limit
    assertEquals(1L, count);
  }

  /**
   * Verifies that getLoggingId is NOT called during normal indexing when no warnings are triggered.
   *
   * <p>This validates the lazy evaluation pattern: lazy(() -> getLoggingId(wrapper)) should only
   * evaluate when a log statement actually fires. During normal indexing with valid data, no
   * warnings are logged, so the lazy supplier should never be invoked.
   */
  @Test
  public void addFieldNamesField_normalIndexing_getLoggingIdNotCalled() {
    // Arrange: create a spied wrapper to track method invocations
    DocumentWrapper realWrapper =
        DocumentWrapper.createRootStandalone(
            DUMMY_ENCODED_BYTES,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH));
    DocumentWrapper spyWrapper = spy(realWrapper);

    // Normal field name (not exceeding max length) - no warning should be triggered
    FieldPath normalPath = FieldPath.newRoot("normalFieldName");

    // Act: perform normal indexing
    IndexableFieldFactory.addFieldNamesField(spyWrapper, normalPath);

    // Assert: getRootId should never be called because no log was emitted,
    // so the lazy(() -> getLoggingId(wrapper)) supplier was never evaluated
    verify(spyWrapper, never()).getRootId();
  }

  @Test
  public void testTypeFieldNameForFloatVectors() {
    FieldPath rootPath = FieldPath.newRoot("root");
    Vector testFloatVector = Vector.fromFloats(new float[] {1f, 2f}, NATIVE);

    var wrapper =
        VectorIndexDocumentWrapper.createRoot(
            DUMMY_ENCODED_BYTES,
            SearchIndexCapabilities.CURRENT,
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
            IndexingPolicyBuilderContext.builder().build());

    // ID field and embedded root field should account for the 2 fields in the document.
    assertEquals(2, wrapper.luceneDocument.getFields().size());
    String expectedIdFieldName = FieldName.MetaField.ID.getLuceneFieldName();
    assertEquals(expectedIdFieldName, wrapper.luceneDocument.getFields().getFirst().name());

    IndexableFieldFactory.addKnnVectorField(
        wrapper,
        rootPath,
        testFloatVector,
        new VectorFieldSpecification(
            testFloatVector.numDimensions(),
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    // KNN vector field should account for the 3rd field in the document.
    assertEquals(3, wrapper.luceneDocument.getFields().size());
    String expectedFloatVectorFieldName =
        FieldName.TypeField.KNN_VECTOR.getLuceneFieldName(rootPath, Optional.empty());
    assertEquals(expectedFloatVectorFieldName, wrapper.luceneDocument.getFields().get(2).name());
  }

  @Test
  public void testTypeFieldNameForByteVectors() {
    FieldPath rootPath = FieldPath.newRoot("root");
    Vector testByteVector = Vector.fromBytes(new byte[] {(byte) 0x00, (byte) 0xFF});

    var wrapper =
        VectorIndexDocumentWrapper.createRoot(
            DUMMY_ENCODED_BYTES,
            SearchIndexCapabilities.CURRENT,
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
            IndexingPolicyBuilderContext.builder().build());

    // ID field and embedded root field should account for the 2 fields in the document.
    assertEquals(2, wrapper.luceneDocument.getFields().size());
    String expectedIdFieldName = FieldName.MetaField.ID.getLuceneFieldName();
    assertEquals(expectedIdFieldName, wrapper.luceneDocument.getFields().getFirst().name());

    IndexableFieldFactory.addKnnVectorField(
        wrapper,
        rootPath,
        testByteVector,
        new VectorFieldSpecification(
            testByteVector.numDimensions(),
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    // KNN vector field should account for the 3rd field in the document.
    assertEquals(3, wrapper.luceneDocument.getFields().size());
    String expectedFloatVectorFieldName =
        FieldName.TypeField.KNN_BYTE.getLuceneFieldName(rootPath, Optional.empty());
    assertEquals(expectedFloatVectorFieldName, wrapper.luceneDocument.getFields().get(2).name());
  }

  @Test
  public void testTypeFieldNameForFloatVectorsWithScalarQuantization() {
    FieldPath rootPath = FieldPath.newRoot("root");
    Vector testFloatVector = Vector.fromFloats(new float[] {1f, 2f}, NATIVE);

    var wrapper =
        VectorIndexDocumentWrapper.createRoot(
            DUMMY_ENCODED_BYTES,
            SearchIndexCapabilities.CURRENT,
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
            IndexingPolicyBuilderContext.builder().build());

    // ID field and embedded root field should account for the 2 fields in the document.
    assertEquals(2, wrapper.luceneDocument.getFields().size());
    String expectedIdFieldName = FieldName.MetaField.ID.getLuceneFieldName();
    assertEquals(expectedIdFieldName, wrapper.luceneDocument.getFields().getFirst().name());

    IndexableFieldFactory.addKnnVectorField(
        wrapper,
        rootPath,
        testFloatVector,
        new VectorFieldSpecification(
            testFloatVector.numDimensions(),
            VectorSimilarity.COSINE,
            VectorQuantization.SCALAR,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    // KNN vector field should account for the 3rd field in the document.
    assertEquals(3, wrapper.luceneDocument.getFields().size());
    String expectedFloatVectorFieldName =
        FieldName.TypeField.KNN_F32_Q7.getLuceneFieldName(rootPath, Optional.empty());
    assertEquals(expectedFloatVectorFieldName, wrapper.luceneDocument.getFields().get(2).name());
  }

  @Test
  public void testTypeFieldNameForByteVectorsWithScalarQuantization() {
    FieldPath rootPath = FieldPath.newRoot("root");
    Vector testByteVector = Vector.fromBytes(new byte[] {(byte) 0x00, (byte) 0xFF});

    var wrapper =
        VectorIndexDocumentWrapper.createRoot(
            DUMMY_ENCODED_BYTES,
            SearchIndexCapabilities.CURRENT,
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
            IndexingPolicyBuilderContext.builder().build());

    // ID field and embedded root field should account for the 2 fields in the document.
    assertEquals(2, wrapper.luceneDocument.getFields().size());
    String expectedIdFieldName = FieldName.MetaField.ID.getLuceneFieldName();
    assertEquals(expectedIdFieldName, wrapper.luceneDocument.getFields().getFirst().name());

    IndexableFieldFactory.addKnnVectorField(
        wrapper,
        rootPath,
        testByteVector,
        new VectorFieldSpecification(
            testByteVector.numDimensions(),
            VectorSimilarity.COSINE,
            VectorQuantization.SCALAR,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    // No fields should be added to the document.
    // ID field and embedded root field should account for the 2 fields in the document.
    assertEquals(2, wrapper.luceneDocument.getFields().size());
    assertEquals(expectedIdFieldName, wrapper.luceneDocument.getFields().getFirst().name());
  }

  @Test
  public void testTypeFieldNameForBitVectors() {
    FieldPath rootPath = FieldPath.newRoot("root");
    Vector testByteVector = Vector.fromBits(new byte[] {(byte) 0x00, (byte) 0xFF});

    var wrapper =
        VectorIndexDocumentWrapper.createRoot(
            DUMMY_ENCODED_BYTES,
            SearchIndexCapabilities.CURRENT,
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
            IndexingPolicyBuilderContext.builder().build());

    // ID field and embedded root field should account for the 2 fields in the document.
    assertEquals(2, wrapper.luceneDocument.getFields().size());
    String expectedIdFieldName = FieldName.MetaField.ID.getLuceneFieldName();
    assertEquals(expectedIdFieldName, wrapper.luceneDocument.getFields().getFirst().name());

    IndexableFieldFactory.addKnnVectorField(
        wrapper,
        rootPath,
        testByteVector,
        new VectorFieldSpecification(
            testByteVector.numDimensions(),
            VectorSimilarity.EUCLIDEAN,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    // KNN vector field should account for the 3rd field in the document.
    assertEquals(3, wrapper.luceneDocument.getFields().size());
    String expectedFloatVectorFieldName =
        FieldName.TypeField.KNN_BIT.getLuceneFieldName(rootPath, Optional.empty());
    assertEquals(expectedFloatVectorFieldName, wrapper.luceneDocument.getFields().get(2).name());
  }

  @Test
  public void addKnnVectorField_negativeInfinite_skipsDocument() {
    FieldPath rootPath = FieldPath.newRoot("root");
    Vector testFloatVector = Vector.fromFloats(new float[] {Float.NEGATIVE_INFINITY, 1f}, BSON);

    var wrapper =
        VectorIndexDocumentWrapper.createRoot(
            DUMMY_ENCODED_BYTES,
            SearchIndexCapabilities.CURRENT,
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
            IndexingPolicyBuilderContext.builder().build());

    // ID field and embedded root field should account for the 2 fields in the document.
    assertEquals(2, wrapper.luceneDocument.getFields().size());
    String expectedIdFieldName = FieldName.MetaField.ID.getLuceneFieldName();
    assertEquals(expectedIdFieldName, wrapper.luceneDocument.getFields().getFirst().name());

    IndexableFieldFactory.addKnnVectorField(
        wrapper,
        rootPath,
        testFloatVector,
        new VectorFieldSpecification(
            testFloatVector.numDimensions(),
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    // No fields should be added to the document.
    assertEquals(2, wrapper.luceneDocument.getFields().size());
    assertEquals(expectedIdFieldName, wrapper.luceneDocument.getFields().getFirst().name());
  }

  @Test
  public void addKnnVectorField_nanFloatVector_doesNotThrow() {
    FieldPath rootPath = FieldPath.newRoot("root");
    Vector testFloatVector = Vector.fromFloats(new float[] {Float.NaN, 1f}, BSON);

    var wrapper =
        VectorIndexDocumentWrapper.createRoot(
            DUMMY_ENCODED_BYTES,
            SearchIndexCapabilities.CURRENT,
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
            IndexingPolicyBuilderContext.builder().build());

    // ID field and embedded root field should account for the 2 fields in the document.
    assertEquals(2, wrapper.luceneDocument.getFields().size());
    String expectedIdFieldName = FieldName.MetaField.ID.getLuceneFieldName();
    assertEquals(expectedIdFieldName, wrapper.luceneDocument.getFields().getFirst().name());

    IndexableFieldFactory.addKnnVectorField(
        wrapper,
        rootPath,
        testFloatVector,
        new VectorFieldSpecification(
            testFloatVector.numDimensions(),
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    // No fields should be added to the document.
    assertEquals(2, wrapper.luceneDocument.getFields().size());
    assertEquals(expectedIdFieldName, wrapper.luceneDocument.getFields().getFirst().name());
  }

  @Test
  public void testBitVectorsWithoutEuclideanSimilarity() {
    FieldPath rootPath = FieldPath.newRoot("root");
    Vector testByteVector = Vector.fromBits(new byte[] {(byte) 0x00, (byte) 0xFF});

    var wrapper =
        VectorIndexDocumentWrapper.createRoot(
            DUMMY_ENCODED_BYTES,
            SearchIndexCapabilities.CURRENT,
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
            IndexingPolicyBuilderContext.builder().build());

    // ID field and embedded root field should account for the 2 fields in the document.
    assertEquals(2, wrapper.luceneDocument.getFields().size());
    String expectedIdFieldName = FieldName.MetaField.ID.getLuceneFieldName();
    assertEquals(expectedIdFieldName, wrapper.luceneDocument.getFields().getFirst().name());

    IndexableFieldFactory.addKnnVectorField(
        wrapper,
        rootPath,
        testByteVector,
        new VectorFieldSpecification(
            testByteVector.numDimensions(),
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    // No fields should be added to the document.
    assertEquals(2, wrapper.luceneDocument.getFields().size());
    assertEquals(expectedIdFieldName, wrapper.luceneDocument.getFields().getFirst().name());
  }

  @Test
  public void testTypeFieldNameForBitVectorsWithScalarQuantization() {
    FieldPath rootPath = FieldPath.newRoot("root");
    Vector testByteVector = Vector.fromBits(new byte[] {(byte) 0x00, (byte) 0xFF});

    var wrapper =
        VectorIndexDocumentWrapper.createRoot(
            DUMMY_ENCODED_BYTES,
            SearchIndexCapabilities.CURRENT,
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
            IndexingPolicyBuilderContext.builder().build());

    // ID field and embedded root field should account for the 2 fields in the document.
    assertEquals(2, wrapper.luceneDocument.getFields().size());
    String expectedIdFieldName = FieldName.MetaField.ID.getLuceneFieldName();
    assertEquals(expectedIdFieldName, wrapper.luceneDocument.getFields().getFirst().name());

    IndexableFieldFactory.addKnnVectorField(
        wrapper,
        rootPath,
        testByteVector,
        new VectorFieldSpecification(
            testByteVector.numDimensions(),
            VectorSimilarity.EUCLIDEAN,
            VectorQuantization.SCALAR,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    // No fields should be added to the document.
    assertEquals(2, wrapper.luceneDocument.getFields().size());
    assertEquals(expectedIdFieldName, wrapper.luceneDocument.getFields().getFirst().name());
  }

  @Test
  public void addKnnVectorField_dimensionMismatch_preventsSubsequentValidVector() {
    FieldPath rootPath = FieldPath.newRoot("description");
    // First vector has wrong dimensions (3 instead of expected 2)
    Vector invalidVector = Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE);
    // Second vector has correct dimensions (2)
    Vector validVector = Vector.fromFloats(new float[] {4f, 5f}, NATIVE);
    int expectedDimensions = 2;

    var wrapper =
        VectorIndexDocumentWrapper.createRoot(
            DUMMY_ENCODED_BYTES,
            SearchIndexCapabilities.CURRENT,
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
            IndexingPolicyBuilderContext.builder().build());

    String checkFieldName =
        FieldName.TypeField.KNN_VECTOR.getLuceneFieldName(rootPath, Optional.empty());

    // Initially, the field can be indexed
    assertTrue(wrapper.canIndexVectorField(checkFieldName));
    assertEquals(2, wrapper.luceneDocument.getFields().size());

    // First vector: dimension mismatch - should mark field as invalid
    IndexableFieldFactory.addKnnVectorField(
        wrapper,
        rootPath,
        invalidVector,
        new VectorFieldSpecification(
            expectedDimensions,
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    // No field should be added due to dimension mismatch
    assertEquals(2, wrapper.luceneDocument.getFields().size());
    // Field should be marked as invalid, preventing subsequent indexing
    assertFalse(wrapper.canIndexVectorField(checkFieldName));

    // Second vector: valid dimensions but should be prevented from indexing
    IndexableFieldFactory.addKnnVectorField(
        wrapper,
        rootPath,
        validVector,
        new VectorFieldSpecification(
            expectedDimensions,
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    // Still no field should be added because the field was marked invalid
    assertEquals(2, wrapper.luceneDocument.getFields().size());
    // Field should still be marked as invalid
    assertFalse(wrapper.canIndexVectorField(checkFieldName));
  }

  @Test
  public void addKnnVectorField_duplicateCheck_preventsSubsequentVector() {
    FieldPath rootPath = FieldPath.newRoot("description");
    // First vector - valid and should be indexed
    Vector firstVector = Vector.fromFloats(new float[] {1f, 2f}, NATIVE);
    // Second vector - also valid but should be prevented by duplicate check
    Vector secondVector = Vector.fromFloats(new float[] {3f, 4f}, NATIVE);
    int expectedDimensions = 2;

    // Setup logger to capture warning logs
    ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
    listAppender.start();
    indexableFieldFactoryLogger.addAppender(listAppender);

    var wrapper =
        VectorIndexDocumentWrapper.createRoot(
            DUMMY_ENCODED_BYTES,
            SearchIndexCapabilities.CURRENT,
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
            IndexingPolicyBuilderContext.builder().build());

    String checkFieldName =
        FieldName.TypeField.KNN_VECTOR.getLuceneFieldName(rootPath, Optional.empty());

    // Initially, the field can be indexed
    assertTrue(wrapper.canIndexVectorField(checkFieldName));
    assertEquals(2, wrapper.luceneDocument.getFields().size());

    // First vector: should be successfully indexed
    IndexableFieldFactory.addKnnVectorField(
        wrapper,
        rootPath,
        firstVector,
        new VectorFieldSpecification(
            expectedDimensions,
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    // First vector should be added to the document
    assertEquals(3, wrapper.luceneDocument.getFields().size());
    // Field should now be marked as indexed, preventing subsequent indexing
    assertFalse(wrapper.canIndexVectorField(checkFieldName));

    // Second vector: valid dimensions but should be prevented by duplicate check (line 342-348)
    // The duplicate check logs a warning and returns early, so no exception is thrown.
    IndexableFieldFactory.addKnnVectorField(
        wrapper,
        rootPath,
        secondVector,
        new VectorFieldSpecification(
            expectedDimensions,
            VectorSimilarity.COSINE,
            VectorQuantization.NONE,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    // Still only one vector field should be in the document (the first one)
    assertEquals(3, wrapper.luceneDocument.getFields().size());
    // Field should still be marked as indexed
    assertFalse(wrapper.canIndexVectorField(checkFieldName));

    // Verify that the duplicate check warning was logged (line 342-347)
    List<ILoggingEvent> list = listAppender.list;
    String expectedLogFragment = "nested embeddings are already indexed or invalid";
    long count =
        list.stream()
            .map(ILoggingEvent::getMessage)
            .filter(str -> str.contains(expectedLogFragment))
            .count();
    // The duplicate check should have logged a warning
    assertTrue("Expected duplicate check warning to be logged", count > 0);
  }

  @Test
  public void sortableNumericIndexedValue_asDoubles_preservesStrictOrder() {
    List<Number> input =
        List.of(
            Double.NaN,
            Double.NEGATIVE_INFINITY,
            -Double.MAX_VALUE,
            Long.MIN_VALUE,
            -1L,
            -Double.MIN_VALUE,
            -0.0,
            0.0,
            Double.MIN_VALUE,
            Double.MIN_NORMAL,
            1L,
            Long.MAX_VALUE,
            Double.MAX_VALUE,
            Double.POSITIVE_INFINITY);

    assertThat(
            input.stream()
                .mapToLong(
                    v ->
                        switch (v) {
                          case Double d ->
                              IndexableFieldFactory.sortableNumericIndexedValue(
                                  Representation.DOUBLE, d);
                          case Number l ->
                              IndexableFieldFactory.sortableNumericIndexedValue(
                                  Representation.DOUBLE, l.longValue());
                        }))
        .isInStrictOrder();
  }

  @Test
  public void sortableNumericIndexedValue_asInt64_preservesOrder() {
    List<Number> input =
        List.of(
            Double.NaN,
            Double.NEGATIVE_INFINITY,
            -Double.MAX_VALUE,
            Long.MIN_VALUE,
            -1L,
            -Double.MIN_VALUE,
            -0.0,
            0.0,
            Double.MIN_VALUE,
            Double.MIN_NORMAL,
            1L,
            Long.MAX_VALUE,
            Double.MAX_VALUE,
            Double.POSITIVE_INFINITY);

    assertThat(
            input.stream()
                .mapToLong(
                    v ->
                        switch (v) {
                          case Double d ->
                              IndexableFieldFactory.sortableNumericIndexedValue(
                                  Representation.INT64, d);
                          case Number l ->
                              IndexableFieldFactory.sortableNumericIndexedValue(
                                  Representation.INT64, l.longValue());
                        }))
        .isInOrder(); // Non-strict order because zeros and extrema are coalesced
  }

  @Test
  public void numericIndexedValue_asDoubles_preservesOrder() {
    List<Number> input =
        List.of(
            Double.NEGATIVE_INFINITY,
            -Double.MAX_VALUE,
            Long.MIN_VALUE,
            -1L,
            -Double.MIN_VALUE,
            -0.0,
            0.0,
            Double.MIN_VALUE,
            Double.MIN_NORMAL,
            1L,
            Long.MAX_VALUE,
            Double.MAX_VALUE,
            Double.POSITIVE_INFINITY,
            Double.NaN);

    assertThat(
            input.stream()
                .mapToLong(
                    v ->
                        switch (v) {
                          case Double d ->
                              IndexableFieldFactory.numericIndexedValue(Representation.DOUBLE, d);
                          case Number l ->
                              IndexableFieldFactory.numericIndexedValue(
                                  Representation.DOUBLE, l.longValue());
                        }))
        .isInOrder(); // double_V1 coalesces signed zero
  }

  @Test
  public void numericIndexedValue_asInt64_preservesOrder() {
    List<Number> input =
        List.of(
            Double.NEGATIVE_INFINITY,
            -Double.MAX_VALUE,
            Long.MIN_VALUE,
            -1L,
            -Double.MIN_VALUE,
            -0.0,
            Double.NaN,
            0.0,
            Double.MIN_VALUE,
            Double.MIN_NORMAL,
            1L,
            Long.MAX_VALUE,
            Double.MAX_VALUE,
            Double.POSITIVE_INFINITY);

    assertThat(
            input.stream()
                .mapToLong(
                    v ->
                        switch (v) {
                          case Double d ->
                              IndexableFieldFactory.numericIndexedValue(Representation.INT64, d);
                          case Number l ->
                              IndexableFieldFactory.numericIndexedValue(
                                  Representation.INT64, l.longValue());
                        }))
        .isInOrder(); // Non-strict order because zeros and extrema are coalesced
  }

  @Test
  public void addNullField_newField_addsStringFieldAndSortedDocValuesField() {
    DocumentWrapper wrapper =
        DocumentWrapper.createRootStandalone(
            DUMMY_ENCODED_BYTES,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH));

    FieldPath fieldPath = FieldPath.newRoot("testField");
    String luceneFieldName =
        FieldName.TypeField.NULL.getLuceneFieldName(fieldPath, Optional.empty());

    // Initially ID field exists (may include docValue if isMetaIdSortable is true)
    int initialFieldCount = wrapper.luceneDocument.getFields().size();
    assertThat(initialFieldCount).isAtLeast(1);

    IndexableFieldFactory.addNullField(wrapper, fieldPath);

    // Should have ID fields, StringField, and SortedDocValuesField
    assertEquals(initialFieldCount + 2, wrapper.luceneDocument.getFields().size());

    // Check StringField was added
    IndexableField stringField = wrapper.luceneDocument.getField(luceneFieldName);
    assertThat(stringField).isNotNull();
    assertThat(stringField.stringValue()).isEqualTo(FieldValue.NULL_FIELD_VALUE);

    // Check SortedDocValuesField was added
    List<IndexableField> fields = wrapper.luceneDocument.getFields();
    IndexableField docValuesField =
        fields.stream()
            .filter(f -> f.name().equals(luceneFieldName) && f instanceof SortedDocValuesField)
            .findFirst()
            .orElse(null);
    assertThat(docValuesField).isNotNull();
    assertThat(docValuesField.binaryValue().utf8ToString()).isEqualTo(FieldValue.NULL_FIELD_VALUE);
  }

  @Test
  public void addNullField_duplicateField_skipsSecondAdd() {
    DocumentWrapper wrapper =
        DocumentWrapper.createRootStandalone(
            DUMMY_ENCODED_BYTES,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH));

    FieldPath fieldPath = FieldPath.newRoot("testField");
    String luceneFieldName =
        FieldName.TypeField.NULL.getLuceneFieldName(fieldPath, Optional.empty());

    // First call - should add fields
    int initialFieldCount = wrapper.luceneDocument.getFields().size();
    IndexableFieldFactory.addNullField(wrapper, fieldPath);
    assertEquals(
        initialFieldCount + 2,
        wrapper
            .luceneDocument
            .getFields()
            .size()); // ID fields + StringField + SortedDocValuesField

    // Second call with same field name - should skip
    IndexableFieldFactory.addNullField(wrapper, fieldPath);
    assertEquals(
        initialFieldCount + 2,
        wrapper.luceneDocument.getFields().size()); // Should still be same, not increased

    // Verify only one StringField exists
    long stringFieldCount =
        wrapper.luceneDocument.getFields().stream()
            .filter(f -> f.name().equals(luceneFieldName) && f instanceof StringField)
            .count();
    assertEquals(1, stringFieldCount);

    // Verify only one SortedDocValuesField exists
    long docValuesFieldCount =
        wrapper.luceneDocument.getFields().stream()
            .filter(f -> f.name().equals(luceneFieldName) && f instanceof SortedDocValuesField)
            .count();
    assertEquals(1, docValuesFieldCount);
  }

  @Test
  public void addNullField_differentFields_addsBoth() {
    DocumentWrapper wrapper =
        DocumentWrapper.createRootStandalone(
            DUMMY_ENCODED_BYTES,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH));

    FieldPath fieldPath1 = FieldPath.newRoot("field1");
    FieldPath fieldPath2 = FieldPath.newRoot("field2");
    String luceneFieldName1 =
        FieldName.TypeField.NULL.getLuceneFieldName(fieldPath1, Optional.empty());
    String luceneFieldName2 =
        FieldName.TypeField.NULL.getLuceneFieldName(fieldPath2, Optional.empty());

    // Add first null field
    int initialFieldCount = wrapper.luceneDocument.getFields().size();
    IndexableFieldFactory.addNullField(wrapper, fieldPath1);
    int afterFirstAdd = wrapper.luceneDocument.getFields().size();
    assertEquals(
        initialFieldCount + 2, afterFirstAdd); // ID fields + StringField1 + SortedDocValuesField1

    // Add second null field with different name
    IndexableFieldFactory.addNullField(wrapper, fieldPath2);
    assertEquals(
        afterFirstAdd + 2,
        wrapper.luceneDocument.getFields().size()); // + StringField2 + SortedDocValuesField2

    // Verify both fields exist
    assertThat(wrapper.luceneDocument.getField(luceneFieldName1)).isNotNull();
    assertThat(wrapper.luceneDocument.getField(luceneFieldName2)).isNotNull();
  }

  @Test
  public void addNullField_vectorIndexDocumentWrapper_worksCorrectly() {
    VectorIndexDocumentWrapper wrapper =
        VectorIndexDocumentWrapper.createRoot(
            DUMMY_ENCODED_BYTES,
            SearchIndexCapabilities.CURRENT,
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
            IndexingPolicyBuilderContext.builder().build());

    FieldPath fieldPath = FieldPath.newRoot("testField");

    // Initially ID field and embedded root field exist
    int initialFieldCount = wrapper.luceneDocument.getFields().size();
    assertEquals(2, initialFieldCount);

    IndexableFieldFactory.addNullField(wrapper, fieldPath);

    // Should have ID field, embedded root field, StringField, and SortedDocValuesField
    assertEquals(initialFieldCount + 2, wrapper.luceneDocument.getFields().size());

    // Verify duplicate call is skipped
    IndexableFieldFactory.addNullField(wrapper, fieldPath);
    assertEquals(
        initialFieldCount + 2, wrapper.luceneDocument.getFields().size()); // Should still be same
  }

  @Test
  public void addNullField_existingDocumentWrapper_worksCorrectly() {
    org.apache.lucene.document.Document luceneDocument = new org.apache.lucene.document.Document();
    ExistingDocumentWrapper wrapper =
        ExistingDocumentWrapper.create(
            luceneDocument,
            SearchIndexCapabilities.CURRENT,
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH));

    FieldPath fieldPath = FieldPath.newRoot("testField");
    String luceneFieldName =
        FieldName.TypeField.NULL.getLuceneFieldName(fieldPath, Optional.empty());

    // Initially document is empty
    assertEquals(0, wrapper.luceneDocument.getFields().size());

    IndexableFieldFactory.addNullField(wrapper, fieldPath);

    // Should have StringField and SortedDocValuesField
    assertEquals(2, wrapper.luceneDocument.getFields().size());

    // Verify duplicate call is skipped
    IndexableFieldFactory.addNullField(wrapper, fieldPath);
    assertEquals(2, wrapper.luceneDocument.getFields().size()); // Should still be 2

    // Verify fields were added correctly
    IndexableField stringField = wrapper.luceneDocument.getField(luceneFieldName);
    assertThat(stringField).isNotNull();
    assertThat(stringField.stringValue()).isEqualTo(FieldValue.NULL_FIELD_VALUE);
  }

  @Test
  public void addNullField_embeddedDocument_usesFieldPathForTracking() {
    DocumentWrapper wrapper =
        DocumentWrapper.createRootStandalone(
            DUMMY_ENCODED_BYTES,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH));

    FieldPath fieldPath = FieldPath.newRoot("testField");
    String luceneFieldName =
        FieldName.TypeField.NULL.getLuceneFieldName(fieldPath, Optional.empty());

    // Add null field first time
    int initialFieldCount = wrapper.luceneDocument.getFields().size();
    IndexableFieldFactory.addNullField(wrapper, fieldPath);
    assertEquals(initialFieldCount + 2, wrapper.luceneDocument.getFields().size());

    // Add same field again - should be skipped due to duplicate tracking
    IndexableFieldFactory.addNullField(wrapper, fieldPath);
    assertEquals(
        initialFieldCount + 2, wrapper.luceneDocument.getFields().size()); // Should still be same

    // Verify only one StringField and one SortedDocValuesField exist
    long stringFieldCount =
        wrapper.luceneDocument.getFields().stream()
            .filter(f -> f.name().equals(luceneFieldName) && f instanceof StringField)
            .count();
    assertEquals(1, stringFieldCount);

    long docValuesFieldCount =
        wrapper.luceneDocument.getFields().stream()
            .filter(f -> f.name().equals(luceneFieldName) && f instanceof SortedDocValuesField)
            .count();
    assertEquals(1, docValuesFieldCount);
  }

  @Test
  public void addNullField_sameFieldPathDifferentLuceneName_tracksSeparately() {
    DocumentWrapper wrapper =
        DocumentWrapper.createRootStandalone(
            DUMMY_ENCODED_BYTES,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH));

    // Create two different field paths
    // This tests that tracking is based on the actual lucene field name, not just field path
    FieldPath fieldPath1 = FieldPath.newRoot("field1");
    FieldPath fieldPath2 = FieldPath.newRoot("field2");
    String luceneFieldName1 =
        FieldName.TypeField.NULL.getLuceneFieldName(fieldPath1, Optional.empty());
    String luceneFieldName2 =
        FieldName.TypeField.NULL.getLuceneFieldName(fieldPath2, Optional.empty());

    // Add first null field
    int initialFieldCount = wrapper.luceneDocument.getFields().size();
    IndexableFieldFactory.addNullField(wrapper, fieldPath1);
    int afterFirstAdd = wrapper.luceneDocument.getFields().size();
    assertEquals(
        initialFieldCount + 2, afterFirstAdd); // ID fields + StringField1 + SortedDocValuesField1

    // Add second null field with different name
    IndexableFieldFactory.addNullField(wrapper, fieldPath2);
    assertEquals(
        afterFirstAdd + 2,
        wrapper.luceneDocument.getFields().size()); // + StringField2 + SortedDocValuesField2

    // Both should exist
    assertThat(wrapper.luceneDocument.getField(luceneFieldName1)).isNotNull();
    assertThat(wrapper.luceneDocument.getField(luceneFieldName2)).isNotNull();
  }

  @Test
  public void getLoggingId_withValidIdField_returnsUnloggable() {
    // Test with DocumentWrapper that has an ID field
    BsonValue testId = new BsonInt32(42);
    byte[] encodedId = LuceneDocumentIdEncoder.encodeDocumentId(testId);
    DocumentWrapper wrapper =
        DocumentWrapper.createRootStandalone(
            encodedId,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH));

    String loggingId = IndexableFieldFactory.getLoggingId(wrapper);
    // Non-UUID/ObjectId types return "unloggable"
    assertThat(loggingId).isNotNull();
    assertThat(loggingId).isNotEmpty();
    assertThat(loggingId).isEqualTo(LoggableIdUtils.UNLOGGABLE_ID_TYPE);
  }

  @Test
  public void getLoggingId_withObjectId_returnsHexString() {
    // Test with ObjectId
    org.bson.types.ObjectId objectIdValue = new org.bson.types.ObjectId();
    BsonObjectId objectId = new BsonObjectId(objectIdValue);
    byte[] encodedId = LuceneDocumentIdEncoder.encodeDocumentId(objectId);
    VectorIndexDocumentWrapper wrapper =
        VectorIndexDocumentWrapper.createRoot(
            encodedId,
            SearchIndexCapabilities.CURRENT,
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
            IndexingPolicyBuilderContext.builder().build());

    String loggingId = IndexableFieldFactory.getLoggingId(wrapper);
    // ObjectId is returned as hex string
    assertThat(loggingId).isNotNull();
    assertThat(loggingId).isNotEmpty();
    assertThat(loggingId).isEqualTo(objectIdValue.toHexString());
  }

  @Test
  public void getLoggingId_withStringId_returnsUnloggable() {
    // Test with String ID
    BsonValue testId = new BsonString("test-id-123");
    byte[] encodedId = LuceneDocumentIdEncoder.encodeDocumentId(testId);
    DocumentWrapper wrapper =
        DocumentWrapper.createRootStandalone(
            encodedId,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH));

    String loggingId = IndexableFieldFactory.getLoggingId(wrapper);
    // Non-UUID/ObjectId string IDs return "unloggable"
    assertThat(loggingId).isNotNull();
    assertThat(loggingId).isNotEmpty();
    assertThat(loggingId).isEqualTo(LoggableIdUtils.UNLOGGABLE_ID_TYPE);
  }

  @Test
  public void getLoggingId_withLegacyUuid_returnsUnloggable() {
    // Test with legacy UUID (subtype 3) - returns "unloggable" because
    // asUuid() can't determine byte order for legacy UUIDs without explicit UuidRepresentation
    byte[] uuidBytes =
        new byte[] {
          (byte) 0xeb, (byte) 0x6c, (byte) 0x40, (byte) 0xca,
          (byte) 0xf2, (byte) 0x5e, (byte) 0x47, (byte) 0xe8,
          (byte) 0xb4, (byte) 0x8c, (byte) 0x02, (byte) 0xa0,
          (byte) 0x5b, (byte) 0x64, (byte) 0xa5, (byte) 0xaa
        };
    BsonBinary legacyUuid = new BsonBinary(BsonBinarySubType.UUID_LEGACY, uuidBytes);
    byte[] encodedId = LuceneDocumentIdEncoder.encodeDocumentId(legacyUuid);
    DocumentWrapper wrapper =
        DocumentWrapper.createRootStandalone(
            encodedId,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH));

    // Legacy UUID returns "unloggable" because byte order is ambiguous
    String loggingId = IndexableFieldFactory.getLoggingId(wrapper);
    assertThat(loggingId).isNotNull();
    assertThat(loggingId).isNotEmpty();
    assertThat(loggingId).isEqualTo(LoggableIdUtils.UNLOGGABLE_ID_TYPE);
  }

  @Test
  public void getLoggingId_withStandardUuid_returnsUuidString() {
    // Test with standard UUID (subtype 4) - should return the UUID string
    java.util.UUID uuid = java.util.UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa");
    BsonBinary standardUuid = new BsonBinary(uuid);
    byte[] encodedId = LuceneDocumentIdEncoder.encodeDocumentId(standardUuid);
    DocumentWrapper wrapper =
        DocumentWrapper.createRootStandalone(
            encodedId,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH));

    // Standard UUID should return the UUID string
    String loggingId = IndexableFieldFactory.getLoggingId(wrapper);
    assertThat(loggingId).isNotNull();
    assertThat(loggingId).isNotEmpty();
    assertThat(loggingId).isEqualTo(uuid.toString());
  }

  @Test
  public void getLoggingId_withOtherBinarySubtype_returnsUnloggable() {
    // Test with generic binary data (subtype 0) - should return "unloggable"
    byte[] binaryData = new byte[] {0x01, 0x02, 0x03, 0x04};
    BsonBinary genericBinary = new BsonBinary(BsonBinarySubType.BINARY, binaryData);
    byte[] encodedId = LuceneDocumentIdEncoder.encodeDocumentId(genericBinary);
    DocumentWrapper wrapper =
        DocumentWrapper.createRootStandalone(
            encodedId,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH));

    // Non-UUID binary types should return "unloggable"
    String loggingId = IndexableFieldFactory.getLoggingId(wrapper);
    assertThat(loggingId).isNotNull();
    assertThat(loggingId).isNotEmpty();
    assertThat(loggingId).isEqualTo(LoggableIdUtils.UNLOGGABLE_ID_TYPE);
  }

  @Test
  public void getLoggingId_withoutIdField_returnsUnknown() {
    // Test with ExistingDocumentWrapper that wraps an empty Document (no ID field)
    org.apache.lucene.document.Document emptyDocument = new org.apache.lucene.document.Document();
    ExistingDocumentWrapper wrapper =
        ExistingDocumentWrapper.create(
            emptyDocument,
            SearchIndexCapabilities.CURRENT,
            new IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH));

    String loggingId = IndexableFieldFactory.getLoggingId(wrapper);
    // Should return UNKNOWN_LOGGABLE_ID when ID field is missing
    assertThat(loggingId).isEqualTo(LoggableIdUtils.UNKNOWN_LOGGABLE_ID);
  }
}
