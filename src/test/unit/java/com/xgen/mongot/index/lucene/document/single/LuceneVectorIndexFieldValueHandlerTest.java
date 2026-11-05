package com.xgen.mongot.index.lucene.document.single;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexCapabilities;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.lucene.document.context.IndexingPolicyBuilderContext;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.VectorDataFieldDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

public class LuceneVectorIndexFieldValueHandlerTest {

  private static final VectorIndexCapabilities CURRENT_CAPABILITIES =
      new VectorIndexCapabilities(
          IndexFormatVersion.CURRENT, VectorIndexCapabilities.CURRENT_FEATURE_VERSION);

  private static final FieldPath FILTER_PATH = FieldPath.newRoot("filter");

  @Test
  public void handleDouble_indexesSortableFieldAsDoubleV2WithLosslessEncoding() throws IOException {
    Document doc = buildDocumentHandlingDouble(1.5);

    // The sortable doc-values field must use the DOUBLE_V2 field name with the lossless MQL
    // sortable encoding, not the INT64_V2 name with a truncating cast.
    IndexableField[] doubleV2Fields =
        doc.getFields(
            FieldName.TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(FILTER_PATH, Optional.empty()));
    assertEquals(1, doubleV2Fields.length);
    assertEquals(
        LuceneDoubleConversionUtils.toMqlSortableLong(1.5),
        doubleV2Fields[0].numericValue().longValue());

    IndexableField[] int64V2Fields =
        doc.getFields(
            FieldName.TypeField.NUMBER_INT64_V2.getLuceneFieldName(FILTER_PATH, Optional.empty()));
    assertEquals(0, int64V2Fields.length);
  }

  @Test
  public void handleDouble_encodesNanBelowAllOtherDoubles() throws IOException {
    Document doc = buildDocumentHandlingDouble(Double.NaN);

    IndexableField[] doubleV2Fields =
        doc.getFields(
            FieldName.TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(FILTER_PATH, Optional.empty()));
    assertEquals(1, doubleV2Fields.length);
    assertEquals(
        LuceneDoubleConversionUtils.NAN_SENTINEL, doubleV2Fields[0].numericValue().longValue());
  }

  @Test
  public void handleDouble_indexesPointFieldAsDoubleMultiple() throws IOException {
    Document doc = buildDocumentHandlingDouble(1.5);

    // Vector indexes index numbers as multi-valued, so the point field uses the legacy
    // DOUBLE_MULTIPLE name and encoding; this is unchanged by the DOUBLE_V2 fix.
    IndexableField[] pointFields =
        doc.getFields(
            FieldName.TypeField.NUMBER_DOUBLE_MULTIPLE.getLuceneFieldName(
                FILTER_PATH, Optional.empty()));
    assertEquals(1, pointFields.length);
    assertEquals(
        LuceneDoubleConversionUtils.toLong(1.5), pointFields[0].numericValue().longValue());
  }

  private static Document buildDocumentHandlingDouble(double value) throws IOException {
    LuceneVectorIndexDocumentBuilder builder =
        LuceneVectorIndexDocumentBuilder.createRoot(
            new byte[8],
            mappingWithFilterField(),
            CURRENT_CAPABILITIES,
            new IndexMetricsUpdater.IndexingMetricsUpdater(
                SearchIndex.mockMetricsFactory(), IndexDefinition.Type.SEARCH),
            IndexingPolicyBuilderContext.builder().build(),
            FeatureFlags.getDefault());
    LuceneVectorIndexFieldValueHandler handler =
        (LuceneVectorIndexFieldValueHandler) builder.valueHandler("filter").get();
    handler.handleDouble(() -> value);
    return builder.build();
  }

  private static VectorIndexFieldMapping mappingWithFilterField() {
    return VectorIndexFieldMapping.create(
        List.of(
            VectorDataFieldDefinitionBuilder.builder()
                .path(FieldPath.newRoot("vector"))
                .numDimensions(1)
                .similarity(VectorSimilarity.EUCLIDEAN)
                .quantization(VectorQuantization.NONE)
                .build(),
            VectorIndexFilterFieldDefinition.create(FILTER_PATH)),
        Optional.empty());
  }
}
