package com.xgen.mongot.index.lucene.document.single;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexCapabilities;
import com.xgen.mongot.index.lucene.document.context.IndexingPolicyBuilderContext;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

public class VectorIndexDocumentWrapperTest {

  private static final VectorIndexCapabilities CURRENT_CAPABILITIES =
      new VectorIndexCapabilities(
          IndexFormatVersion.CURRENT, VectorIndexCapabilities.CURRENT_FEATURE_VERSION);

  private final IndexMetricsUpdater.IndexingMetricsUpdater metrics =
      new IndexMetricsUpdater.IndexingMetricsUpdater(
          SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH);

  @Test
  public void addIndexedVectorField_newField_incrementsCounter() {
    VectorIndexDocumentWrapper wrapper =
        VectorIndexDocumentWrapper.createRoot(
            new byte[8],
            CURRENT_CAPABILITIES,
            this.metrics,
            IndexingPolicyBuilderContext.builder().build());
    assertTrue(wrapper.canIndexVectorField("vector"));
    assertEquals(0.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);

    wrapper.addIndexedVectorField("vector");

    assertFalse(wrapper.canIndexVectorField("vector"));
    assertEquals(1.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);
  }

  @Test
  public void addIndexedVectorField_duplicateField_isNoOp() {
    VectorIndexDocumentWrapper wrapper =
        VectorIndexDocumentWrapper.createRoot(
            new byte[8],
            CURRENT_CAPABILITIES,
            this.metrics,
            IndexingPolicyBuilderContext.builder().build());
    wrapper.addIndexedVectorField("vector");
    assertFalse(wrapper.canIndexVectorField("vector"));
    assertEquals(1.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);

    wrapper.addIndexedVectorField("vector");

    assertFalse(wrapper.canIndexVectorField("vector"));
    assertEquals(1.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);
  }

  @Test
  public void addIndexedVectorField_twoFields_isNoOp() {
    VectorIndexDocumentWrapper wrapper =
        VectorIndexDocumentWrapper.createRoot(
            new byte[8],
            CURRENT_CAPABILITIES,
            this.metrics,
            IndexingPolicyBuilderContext.builder().build());
    wrapper.addIndexedVectorField("vector");
    assertEquals(1.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);

    wrapper.addIndexedVectorField("byteVector");

    assertFalse(wrapper.canIndexVectorField("byteVector"));
    assertEquals(1.0, this.metrics.getVectorFieldsIndexed().count(), TestUtils.EPSILON);
  }

  @Test
  public void createRoot_addsEmbeddedRootField() {
    byte[] id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};

    VectorIndexDocumentWrapper wrapper =
        VectorIndexDocumentWrapper.createRoot(
            id, CURRENT_CAPABILITIES, this.metrics, IndexingPolicyBuilderContext.builder().build());

    assertNotNull(wrapper);
    assertNotNull(wrapper.luceneDocument);

    Document doc = wrapper.luceneDocument;

    // Verify document ID field exists
    IndexableField[] idFields = doc.getFields(FieldName.MetaField.ID.getLuceneFieldName());
    assertEquals(1, idFields.length);
    assertNotNull(idFields[0]);

    // Verify embedded root field exists to mark this as a root document
    IndexableField[] embeddedRootFields =
        doc.getFields(FieldName.MetaField.EMBEDDED_ROOT.getLuceneFieldName());
    assertEquals(1, embeddedRootFields.length);
    assertNotNull(embeddedRootFields[0]);
  }

  @Test
  public void createEmbedded_addsDocumentIdAndEmbeddedPathFields() {
    byte[] id = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    FieldPath embeddedRoot = FieldPath.parse("sections");

    VectorIndexDocumentWrapper wrapper =
        VectorIndexDocumentWrapper.createEmbedded(
            id, embeddedRoot, CURRENT_CAPABILITIES, this.metrics);

    assertNotNull(wrapper);
    assertNotNull(wrapper.luceneDocument);

    Document doc = wrapper.luceneDocument;

    // Verify document ID field exists (StoredField only, no SortedDocValuesField)
    // Vector indexes don't support sorting on _id, so includeDocValue is false
    IndexableField[] idFields = doc.getFields(FieldName.MetaField.ID.getLuceneFieldName());
    assertEquals(1, idFields.length);
    assertNotNull(idFields[0]);

    // Verify embedded path field exists
    IndexableField[] embeddedPathFields =
        doc.getFields(FieldName.MetaField.EMBEDDED_PATH.getLuceneFieldName());
    assertEquals(1, embeddedPathFields.length);
    assertNotNull(embeddedPathFields[0]);
    assertEquals("sections", embeddedPathFields[0].stringValue());
  }
}
