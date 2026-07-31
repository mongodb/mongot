package com.xgen.mongot.index.lucene.document.single;

import static com.xgen.testing.BsonTestUtils.bsonBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition.Type;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.junit.Test;

public class DocumentWrapperTest {

  private static final byte[] ENCODED_ID = bsonBytes("{_id: 1}");

  private final IndexMetricsUpdater.IndexingMetricsUpdater metrics =
      new IndexingMetricsUpdater(SearchIndex.mockMetricsFactory(), Type.SEARCH);

  @Test
  public void createRootStandalone_currentIfv_indexesNumberV2() {
    DocumentWrapper wrapper =
        DocumentWrapper.createRootStandalone(
            ENCODED_ID,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            new IndexingMetricsUpdater(SearchIndex.mockMetricsFactory(), Type.SEARCH),
            FeatureFlags.getDefault());

    assertTrue(wrapper.isNumberAndDateSortable);
  }

  @Test
  public void createRootStandalone_oldestIfv_indexesNumberV2() {
    DocumentWrapper wrapper =
        DocumentWrapper.createRootStandalone(
            ENCODED_ID,
            new KeywordAnalyzer(),
            SearchIndex.mockDefinitionBuilder()
                .indexFeatureVersion(3)
                .build()
                .createFieldDefinitionResolver(IndexFormatVersion.MIN_SUPPORTED_VERSION),
            new IndexingMetricsUpdater(SearchIndex.mockMetricsFactory(), Type.SEARCH),
            FeatureFlags.getDefault());

    assertTrue(wrapper.isNumberAndDateSortable);
  }

  @Test
  public void createEmbeddedRoot_currentIfv_indexesNumberV2() {
    DocumentWrapper wrapper =
        DocumentWrapper.createEmbeddedRoot(
            ENCODED_ID,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            new IndexingMetricsUpdater(SearchIndex.mockMetricsFactory(), Type.SEARCH),
            FeatureFlags.getDefault());

    assertTrue(wrapper.isNumberAndDateSortable);
  }

  @Test
  public void createEmbeddedRoot_oldestIfv_indexesNumberV2() {
    DocumentWrapper wrapper =
        DocumentWrapper.createEmbeddedRoot(
            ENCODED_ID,
            new KeywordAnalyzer(),
            SearchIndex.mockDefinitionBuilder()
                .indexFeatureVersion(3)
                .build()
                .createFieldDefinitionResolver(IndexFormatVersion.MIN_SUPPORTED_VERSION),
            new IndexingMetricsUpdater(SearchIndex.mockMetricsFactory(), Type.SEARCH),
            FeatureFlags.getDefault());

    assertTrue(wrapper.isNumberAndDateSortable);
  }

  @Test
  public void createEmbedded_nextIfv_indexesNumberV2() {
    DocumentWrapper wrapper =
        DocumentWrapper.createEmbedded(
            ENCODED_ID,
            FieldPath.newRoot("child"),
            new KeywordAnalyzer(),
            SearchIndex.mockDefinitionBuilder()
                .indexFeatureVersion(4)
                .build()
                .createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            new IndexingMetricsUpdater(SearchIndex.mockMetricsFactory(), Type.SEARCH));

    assertTrue(wrapper.isNumberAndDateSortable);
  }

  @Test
  public void createEmbedded_currentIfv_skipsNumberV2() {
    DocumentWrapper wrapper =
        DocumentWrapper.createEmbedded(
            ENCODED_ID,
            FieldPath.newRoot("child"),
            new KeywordAnalyzer(),
            SearchIndex.mockDefinitionBuilder()
                .indexFeatureVersion(3)
                .build()
                .createFieldDefinitionResolver(IndexFormatVersion.SIX),
            new IndexingMetricsUpdater(SearchIndex.mockMetricsFactory(), Type.SEARCH));

    assertFalse(wrapper.isNumberAndDateSortable);
  }

  @Test
  public void addIndexedVectorField_newField_incrementsCounter() {
    DocumentWrapper wrapper =
        DocumentWrapper.createRootStandalone(
            ENCODED_ID,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            this.metrics,
            FeatureFlags.getDefault());
    assertTrue(wrapper.canIndexVectorField("vector"));
    assertEquals(0.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);

    wrapper.addIndexedVectorField("vector");

    assertFalse(wrapper.canIndexVectorField("vector"));
    assertEquals(1.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);
  }

  @Test
  public void addIndexedVectorField_duplicateField_isNoOp() {
    DocumentWrapper wrapper =
        DocumentWrapper.createRootStandalone(
            ENCODED_ID,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            this.metrics,
            FeatureFlags.getDefault());
    wrapper.addIndexedVectorField("vector");
    assertFalse(wrapper.canIndexVectorField("vector"));
    assertEquals(1.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);

    wrapper.addIndexedVectorField("vector");

    assertFalse(wrapper.canIndexVectorField("vector"));
    assertEquals(1.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);
  }

  @Test
  public void addIndexedVectorField_twoFields_isNoOp() {
    DocumentWrapper wrapper =
        DocumentWrapper.createRootStandalone(
            ENCODED_ID,
            new KeywordAnalyzer(),
            SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT),
            this.metrics,
            FeatureFlags.getDefault());
    wrapper.addIndexedVectorField("vector");
    assertEquals(1.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);

    wrapper.addIndexedVectorField("byteVector");

    assertFalse(wrapper.canIndexVectorField("byteVector"));
    assertEquals(1.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);
  }
}
