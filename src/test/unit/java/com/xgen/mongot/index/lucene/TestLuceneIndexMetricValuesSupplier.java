package com.xgen.mongot.index.lucene;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.DocCounts;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.IndexMetricValuesSupplier.MetricNames;
import com.xgen.mongot.index.IndexReader;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.WriterClosedException;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.testing.mongot.index.analyzer.custom.TokenizerDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.definition.CustomAnalyzerDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.LuceneIndexMetricsSupplierBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestLuceneIndexMetricValuesSupplier {

  /** Creates a feature flag registry with numFieldsPerDatatype metric enabled. */
  private static DynamicFeatureFlagRegistry createEnabledFeatureFlagRegistry() {
    ObjectId clusterId = new ObjectId();
    DynamicFeatureFlagConfig enabledConfig =
        new DynamicFeatureFlagConfig(
            LuceneSearchIndexMetricValuesSupplier.NUM_FIELDS_PER_DATATYPE_METRIC_FLAG,
            DynamicFeatureFlagConfig.Phase.ENABLED,
            List.of(),
            List.of(),
            100,
            DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER);
    return new DynamicFeatureFlagRegistry(
        Optional.of(List.of(enabledConfig)),
        Optional.empty(),
        Optional.empty(),
        Optional.of(clusterId));
  }

  @Test
  public void testNumFieldsPerDatatypeCountsFields() throws ReaderClosedException, IOException {
    var indexReader = Mockito.mock(LuceneSearchIndexReader.class);
    var indexWriter = Mockito.mock(SingleLuceneIndexWriter.class);

    // Create fields of various types
    List<FieldInfo> fieldInfos = new ArrayList<>();
    // 2 autocomplete fields
    fieldInfos.add(createFieldInfo("$type:autocomplete/field1", 1));
    fieldInfos.add(createFieldInfo("$type:autocomplete/field2", 2));
    // 1 string field
    fieldInfos.add(createFieldInfo("$type:string/stringField", 3));
    // 3 token fields
    fieldInfos.add(createFieldInfo("$type:token/token1", 4));
    fieldInfos.add(createFieldInfo("$type:token/token2", 5));
    fieldInfos.add(createFieldInfo("$type:token/token3", 6));
    // Field that doesn't match any type (should be ignored)
    fieldInfos.add(createFieldInfo("$meta/someMetaField", 7));

    // Use thenAnswer to return a fresh spliterator each time (spliterators are single-use)
    FieldInfos mockedFieldInfos = Mockito.mock(FieldInfos.class);
    Mockito.when(mockedFieldInfos.spliterator()).thenAnswer(inv -> fieldInfos.spliterator());
    Mockito.when(indexReader.getFieldInfos()).thenReturn(List.of(mockedFieldInfos));

    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    LuceneIndexMetricValuesSupplier luceneIndexMetricsSupplier =
        LuceneIndexMetricsSupplierBuilder.builder()
            .isEmbedded(true)
            .indexWriter(indexWriter)
            .indexReader(indexReader)
            .resolver(
                SearchIndexDefinitionBuilder.VALID_INDEX.createFieldDefinitionResolver(
                    IndexFormatVersion.CURRENT))
            .meterRegistry(meterRegistry)
            .dynamicFeatureFlagRegistry(createEnabledFeatureFlagRegistry())
            .build();

    Map<FieldName.TypeField, Double> result = luceneIndexMetricsSupplier.getNumFieldsPerDatatype();

    // Verify counts
    Assert.assertEquals(2.0, result.get(FieldName.TypeField.AUTOCOMPLETE), 0.001);
    Assert.assertEquals(1.0, result.get(FieldName.TypeField.STRING), 0.001);
    Assert.assertEquals(3.0, result.get(FieldName.TypeField.TOKEN), 0.001);

    // Verify no unexpected types are present (only AUTOCOMPLETE, STRING, TOKEN should have counts)
    Assert.assertEquals(
        "Expected exactly 3 field types with non-zero counts", 3, result.size());
  }

  @Test
  public void testNumFieldsPerDatatypeOnlyConsidersFirstPartition()
      throws ReaderClosedException, IOException {
    var indexReader = Mockito.mock(LuceneSearchIndexReader.class);
    var indexWriter = Mockito.mock(SingleLuceneIndexWriter.class);

    // First partition's fields - these should be counted
    List<FieldInfo> firstPartitionFields = new ArrayList<>();
    firstPartitionFields.add(createFieldInfo("$type:autocomplete/field1", 1));
    firstPartitionFields.add(createFieldInfo("$type:string/stringField", 2));

    // Second partition's fields - these should be IGNORED
    // (all partitions have same fields, so we only process the first one)
    List<FieldInfo> secondPartitionFields = new ArrayList<>();
    secondPartitionFields.add(createFieldInfo("$type:autocomplete/field1", 1));
    secondPartitionFields.add(createFieldInfo("$type:string/stringField", 2));
    // Add extra fields that would change the count if processed
    secondPartitionFields.add(createFieldInfo("$type:token/extraToken1", 3));
    secondPartitionFields.add(createFieldInfo("$type:token/extraToken2", 4));

    FieldInfos firstPartitionFieldInfos = Mockito.mock(FieldInfos.class);
    Mockito.when(firstPartitionFieldInfos.spliterator())
        .thenAnswer(inv -> firstPartitionFields.spliterator());

    FieldInfos secondPartitionFieldInfos = Mockito.mock(FieldInfos.class);
    Mockito.when(secondPartitionFieldInfos.spliterator())
        .thenAnswer(inv -> secondPartitionFields.spliterator());

    // Return multiple FieldInfos (simulating multiple partitions)
    Mockito.when(indexReader.getFieldInfos())
        .thenReturn(List.of(firstPartitionFieldInfos, secondPartitionFieldInfos));

    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    LuceneIndexMetricValuesSupplier luceneIndexMetricsSupplier =
        LuceneIndexMetricsSupplierBuilder.builder()
            .isEmbedded(false)
            .indexWriter(indexWriter)
            .indexReader(indexReader)
            .resolver(
                SearchIndexDefinitionBuilder.VALID_INDEX.createFieldDefinitionResolver(
                    IndexFormatVersion.CURRENT))
            .meterRegistry(meterRegistry)
            .dynamicFeatureFlagRegistry(createEnabledFeatureFlagRegistry())
            .build();

    Map<FieldName.TypeField, Double> result = luceneIndexMetricsSupplier.getNumFieldsPerDatatype();

    // Only first partition's fields should be counted:
    // - 1 AUTOCOMPLETE field
    // - 1 STRING field
    // The extra TOKEN fields from the second partition should NOT be counted
    Assert.assertEquals(
        "Expected 1 AUTOCOMPLETE field from first partition only",
        1.0,
        result.get(FieldName.TypeField.AUTOCOMPLETE),
        0.001);
    Assert.assertEquals(
        "Expected 1 STRING field from first partition only",
        1.0,
        result.get(FieldName.TypeField.STRING),
        0.001);
    Assert.assertNull(
        "TOKEN fields from second partition should not be counted",
        result.get(FieldName.TypeField.TOKEN));
    Assert.assertEquals(
        "Expected exactly 2 field types (only from first partition)", 2, result.size());
  }

  @Test
  public void testNumFieldsPerDatatypeHandlesException() throws ReaderClosedException, IOException {
    var indexReader = Mockito.mock(LuceneSearchIndexReader.class);
    var indexWriter = Mockito.mock(SingleLuceneIndexWriter.class);

    // Simulate an IOException when getting field infos
    Mockito.when(indexReader.getFieldInfos()).thenThrow(new IOException("Test exception"));

    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    LuceneIndexMetricValuesSupplier luceneIndexMetricsSupplier =
        LuceneIndexMetricsSupplierBuilder.builder()
            .isEmbedded(false)
            .indexWriter(indexWriter)
            .indexReader(indexReader)
            .resolver(
                SearchIndexDefinitionBuilder.VALID_INDEX.createFieldDefinitionResolver(
                    IndexFormatVersion.CURRENT))
            .meterRegistry(meterRegistry)
            .dynamicFeatureFlagRegistry(createEnabledFeatureFlagRegistry())
            .build();

    Map<FieldName.TypeField, Double> result = luceneIndexMetricsSupplier.getNumFieldsPerDatatype();

    // Should return empty map on exception
    Assert.assertEquals(Collections.emptyMap(), result);
  }

  @Test
  public void testNumFieldsPerDatatypeCachesResultAndRecomputesAfterExpiration()
      throws ReaderClosedException, IOException, InterruptedException {
    var indexReader = Mockito.mock(LuceneSearchIndexReader.class);
    var indexWriter = Mockito.mock(SingleLuceneIndexWriter.class);

    List<FieldInfo> fieldInfos = new ArrayList<>();
    fieldInfos.add(createFieldInfo("$type:autocomplete/field1", 1));

    // Use thenAnswer to return a fresh spliterator each time
    FieldInfos mockedFieldInfos = Mockito.mock(FieldInfos.class);
    Mockito.when(mockedFieldInfos.spliterator()).thenAnswer(inv -> fieldInfos.spliterator());
    Mockito.when(indexReader.getFieldInfos()).thenReturn(List.of(mockedFieldInfos));

    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    // Use a very short cache duration (100ms) to test expiration
    LuceneIndexMetricValuesSupplier luceneIndexMetricsSupplier =
        LuceneIndexMetricsSupplierBuilder.builder()
            .isEmbedded(false)
            .indexWriter(indexWriter)
            .indexReader(indexReader)
            .resolver(
                SearchIndexDefinitionBuilder.VALID_INDEX.createFieldDefinitionResolver(
                    IndexFormatVersion.CURRENT))
            .meterRegistry(meterRegistry)
            .numFieldsCacheDuration(java.time.Duration.ofMillis(100))
            .dynamicFeatureFlagRegistry(createEnabledFeatureFlagRegistry())
            .build();

    // First call - should compute (slow path)
    luceneIndexMetricsSupplier.getNumFieldsPerDatatype();
    Mockito.verify(indexReader, Mockito.times(1)).getFieldInfos();

    // Second call within cache window - should use cached value (fast path)
    luceneIndexMetricsSupplier.getNumFieldsPerDatatype();
    Mockito.verify(indexReader, Mockito.times(1)).getFieldInfos();

    // Wait for cache to expire
    Thread.sleep(150);

    // Third call after cache expiration - should recompute (slow path)
    luceneIndexMetricsSupplier.getNumFieldsPerDatatype();
    Mockito.verify(indexReader, Mockito.times(2)).getFieldInfos();

    // Fourth call within new cache window - should use cached value (fast path)
    luceneIndexMetricsSupplier.getNumFieldsPerDatatype();
    Mockito.verify(indexReader, Mockito.times(2)).getFieldInfos();
  }

  @Test
  public void testStringFieldWithAutocompleteLikeCustomAnalyzerCountedAsAutocomplete()
      throws ReaderClosedException, IOException {
    var indexReader = Mockito.mock(LuceneSearchIndexReader.class);
    var indexWriter = Mockito.mock(SingleLuceneIndexWriter.class);

    // Create a string field that uses the autocomplete-like custom analyzer
    List<FieldInfo> fieldInfos = new ArrayList<>();
    fieldInfos.add(createFieldInfo("$type:string/stringFieldWithAutocomplete", 1));
    fieldInfos.add(createFieldInfo("$type:string/regularStringField", 2));
    fieldInfos.add(createFieldInfo("$type:autocomplete/nativeAutocomplete", 3));

    FieldInfos mockedFieldInfos = Mockito.mock(FieldInfos.class);
    Mockito.when(mockedFieldInfos.spliterator()).thenAnswer(inv -> fieldInfos.spliterator());
    Mockito.when(indexReader.getFieldInfos()).thenReturn(List.of(mockedFieldInfos));

    // Create an index definition with:
    // - A string field using an autocomplete-like custom analyzer (edge_gram)
    // - A regular string field using the standard lucene.standard analyzer
    SearchIndexDefinition searchIndexDefinition =
        SearchIndexDefinitionBuilder.from(SearchIndexDefinitionBuilder.VALID_INDEX)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        "stringFieldWithAutocomplete",
                        FieldDefinitionBuilder.builder()
                            .string(
                                StringFieldDefinitionBuilder.builder()
                                    .analyzerName("edgeGramAnalyzer")
                                    .build())
                            .build())
                    .field(
                        "regularStringField",
                        FieldDefinitionBuilder.builder()
                            .string(
                                StringFieldDefinitionBuilder.builder()
                                    .analyzerName("lucene.standard")
                                    .build())
                            .build())
                    .dynamic(false)
                    .build())
            .analyzers(
                List.of(
                    CustomAnalyzerDefinitionBuilder.builder(
                            "edgeGramAnalyzer",
                            TokenizerDefinitionBuilder.EdgeGramTokenizer.builder()
                                .maxGram(15)
                                .minGram(2)
                                .build())
                        .build()))
            .build();

    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    LuceneIndexMetricValuesSupplier luceneIndexMetricsSupplier =
        LuceneIndexMetricsSupplierBuilder.builder()
            .isEmbedded(false)
            .indexWriter(indexWriter)
            .indexReader(indexReader)
            .resolver(
                searchIndexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT))
            .customAnalyzerDefinitions(searchIndexDefinition.getAnalyzers())
            .meterRegistry(meterRegistry)
            .dynamicFeatureFlagRegistry(createEnabledFeatureFlagRegistry())
            .build();

    Map<FieldName.TypeField, Double> result = luceneIndexMetricsSupplier.getNumFieldsPerDatatype();

    // stringFieldWithAutocomplete should be counted as AUTOCOMPLETE because it uses
    // edgeGramAnalyzer
    // regularStringField should be counted as STRING because it uses lucene.standard
    // nativeAutocomplete should be counted as AUTOCOMPLETE (native autocomplete type)
    Assert.assertEquals(
        "Expected 2 AUTOCOMPLETE fields (1 native + 1 string with edge_gram analyzer)",
        2.0,
        result.get(FieldName.TypeField.AUTOCOMPLETE),
        0.001);
    Assert.assertEquals(
        "Expected 1 STRING field (regular string with standard analyzer)",
        1.0,
        result.get(FieldName.TypeField.STRING),
        0.001);
  }

  private static FieldInfo createFieldInfo(String name, int fieldNumber) {
    return new FieldInfo(
        name,
        fieldNumber,
        false,
        false,
        false,
        IndexOptions.NONE,
        DocValuesType.NONE,
        -1,
        Collections.emptyMap(),
        0,
        0,
        0,
        0,
        VectorEncoding.BYTE,
        VectorSimilarityFunction.COSINE,
        false,
        false);
  }

  @Test
  public void testMissingFieldDefinition() throws ReaderClosedException, IOException {
    var indexReader = Mockito.mock(LuceneSearchIndexReader.class);
    var indexWriter = Mockito.mock(SingleLuceneIndexWriter.class);

    List<FieldInfo> oneFieldInfos = new ArrayList<>();
    oneFieldInfos.add(
        new FieldInfo(
            "$multi/fieldWithMultiAnalyzer.autocompleteMulti",
            1,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NONE,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.BYTE,
            VectorSimilarityFunction.COSINE,
            false,
            false));
    FieldInfos mockedFieldInfos = Mockito.mock(FieldInfos.class);
    Mockito.when(mockedFieldInfos.spliterator()).thenReturn(oneFieldInfos.spliterator());
    Mockito.when(indexReader.getFieldInfos()).thenReturn(List.of(mockedFieldInfos));

    SearchIndexDefinition searchIndexDefinition =
        SearchIndexDefinitionBuilder.from(SearchIndexDefinitionBuilder.VALID_INDEX)
            .mappings(DocumentFieldDefinitionBuilder.builder().dynamic(false).build())
            .analyzers(
                List.of(
                    CustomAnalyzerDefinitionBuilder.builder(
                            "customAutocomplete",
                            TokenizerDefinitionBuilder.NGramTokenizer.builder()
                                .maxGram(5)
                                .minGram(3)
                                .build())
                        .build()))
            .build();

    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    LuceneIndexMetricValuesSupplier luceneIndexMetricsSupplier =
        LuceneIndexMetricsSupplierBuilder.builder()
            .isEmbedded(true)
            .indexWriter(indexWriter)
            .indexReader(indexReader)
            .resolver(
                searchIndexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT))
            .customAnalyzerDefinitions(searchIndexDefinition.getAnalyzers())
            .meterRegistry(meterRegistry)
            .dynamicFeatureFlagRegistry(createEnabledFeatureFlagRegistry())
            .build();

    Map<FieldName.TypeField, Double> result = luceneIndexMetricsSupplier.getNumFieldsPerDatatype();
    Assert.assertEquals(Collections.emptyMap(), result);
  }

  @Test
  public void testCannotParseFieldName() throws ReaderClosedException, IOException {
    var indexReader = Mockito.mock(LuceneSearchIndexReader.class);
    var indexWriter = Mockito.mock(SingleLuceneIndexWriter.class);
    List<FieldInfo> oneFieldInfos = new ArrayList<>();
    oneFieldInfos.add(
        new FieldInfo(
            "$embedded:8/teachers/teachers.firstName",
            1,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NONE,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.BYTE,
            VectorSimilarityFunction.COSINE,
            false,
            false));
    FieldInfos mockedFieldInfos = Mockito.mock(FieldInfos.class);
    Mockito.when(mockedFieldInfos.spliterator()).thenReturn(oneFieldInfos.spliterator());
    Mockito.when(indexReader.getFieldInfos()).thenReturn(List.of(mockedFieldInfos));

    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    LuceneIndexMetricValuesSupplier luceneIndexMetricsSupplier =
        LuceneIndexMetricsSupplierBuilder.builder()
            .isEmbedded(true)
            .indexWriter(indexWriter)
            .indexReader(indexReader)
            .resolver(
                SearchIndexDefinitionBuilder.VALID_INDEX.createFieldDefinitionResolver(
                    IndexFormatVersion.CURRENT))
            .meterRegistry(meterRegistry)
            .dynamicFeatureFlagRegistry(createEnabledFeatureFlagRegistry())
            .build();

    Map<FieldName.TypeField, Double> result = luceneIndexMetricsSupplier.getNumFieldsPerDatatype();
    Assert.assertEquals(Collections.emptyMap(), result);
  }

  @Test
  public void testNonEmbedded() throws ReaderClosedException, IOException, WriterClosedException {
    var statusContainer = IndexStatus.steady();
    var indexWriter = Mockito.mock(SingleLuceneIndexWriter.class);
    Mockito.doReturn(24L).when(indexWriter).getNumLuceneMaxDocs();
    Mockito.doReturn(18).when(indexWriter).getMaxLuceneMaxDocs();
    Mockito.doReturn(23L).when(indexWriter).getNumDocs();
    Mockito.doReturn(1000).when(indexWriter).getNumFields();
    Mockito.doReturn(1).when(indexWriter).getNumWriters();
    IndexBackingStrategy indexBackingStrategy = mock(IndexBackingStrategy.class);
    when(indexBackingStrategy.getIndexSize()).thenReturn(1234L);
    when(indexBackingStrategy.getLargestIndexFileSize()).thenReturn(1111L);
    when(indexBackingStrategy.getNumFilesInIndex()).thenReturn(1112L);

    for (IndexDefinition.Type type : IndexDefinition.Type.values()) {
      @Var IndexReader indexReader = Mockito.mock(LuceneVectorIndexReader.class);
      if (type == IndexDefinition.Type.SEARCH) {
        indexReader = Mockito.mock(LuceneSearchIndexReader.class);
        Mockito.doReturn(25L)
            .when((LuceneSearchIndexReader) indexReader)
            .getNumEmbeddedRootDocuments();
      }
      Mockito.doReturn(24L).when(indexReader).getRequiredMemoryForVectorData();
      MeterRegistry meterRegistry = new SimpleMeterRegistry();
      LuceneIndexMetricsSupplierBuilder luceneIndexMetricsSupplierBuilder =
          LuceneIndexMetricsSupplierBuilder.builder()
              .type(type)
              .isEmbedded(false)
              .indexWriter(indexWriter)
              .indexReader(indexReader)
              .indexStatusSupplier(() -> statusContainer)
              .indexBackingStrategy(indexBackingStrategy)
              .meterRegistry(meterRegistry);

      LuceneIndexMetricValuesSupplier luceneIndexMetricsSupplier =
          type == IndexDefinition.Type.VECTOR_SEARCH
              ? luceneIndexMetricsSupplierBuilder.build()
              : luceneIndexMetricsSupplierBuilder
                  .resolver(
                      SearchIndexDefinitionBuilder.VALID_INDEX.createFieldDefinitionResolver(
                          IndexFormatVersion.CURRENT))
                  .build();

      DocCounts docCounts = luceneIndexMetricsSupplier.getDocCounts();
      // non-embedded case doesnt use embedded root document
      if (type == IndexDefinition.Type.SEARCH) {
        verify((LuceneSearchIndexReader) indexReader, never()).getNumEmbeddedRootDocuments();
      }
      Assert.assertEquals(23, docCounts.numMongoDbDocs);
      Assert.assertEquals(24, docCounts.numLuceneMaxDocs);
      Assert.assertEquals(18, docCounts.maxLuceneMaxDocs);
      Assert.assertEquals(23, docCounts.numDocs);
      Assert.assertEquals(24, luceneIndexMetricsSupplier.getRequiredMemoryForVectorData());
      Assert.assertEquals(1000, luceneIndexMetricsSupplier.getNumFields());
      Assert.assertEquals(1234L, luceneIndexMetricsSupplier.computeIndexSize());
      Assert.assertEquals(statusContainer, luceneIndexMetricsSupplier.getIndexStatus());
      Assert.assertEquals(
          1,
          getPerIndexGaugeMetricsValue(
              meterRegistry,
              IndexMetricValuesSupplier.MetricNames.INDEX_STATUS_CODE,
              Tags.of("status", IndexStatus.StatusCode.STEADY.name())));
      Assert.assertEquals(
          0,
          getPerIndexGaugeMetricsValue(
              meterRegistry,
              IndexMetricValuesSupplier.MetricNames.INDEX_STATUS_CODE,
              Tags.of("status", IndexStatus.StatusCode.FAILED.name())));
      Assert.assertEquals(
          1234,
          getPerIndexGaugeMetricsValue(
              meterRegistry, IndexMetricValuesSupplier.MetricNames.INDEX_SIZE_BYTES));
      Assert.assertEquals(
          1111,
          getPerIndexGaugeMetricsValue(
              meterRegistry, IndexMetricValuesSupplier.MetricNames.LARGEST_INDEX_FILE_SIZE_BYTES));
      Assert.assertEquals(
          1112, getPerIndexGaugeMetricsValue(meterRegistry, MetricNames.NUMBER_OF_FILES_IN_INDEX));
      Assert.assertEquals(
          23,
          getPerIndexGaugeMetricsValue(
              meterRegistry, IndexMetricValuesSupplier.MetricNames.NUM_LUCENE_DOCS));
      Assert.assertEquals(
          1000,
          getPerIndexGaugeMetricsValue(
              meterRegistry, IndexMetricValuesSupplier.MetricNames.NUM_LUCENE_FIELDS));
      Assert.assertEquals(
          24,
          getPerIndexGaugeMetricsValue(
              meterRegistry,
              IndexMetricValuesSupplier.MetricNames.NUM_LUCENE_MAX_DOCS,
              Tags.of("numPartitions", "1")));
      Assert.assertEquals(
          24, getPerIndexGaugeMetricsValue(meterRegistry, MetricNames.REQUIRED_MEMORY));
    }
  }

  @Test
  public void testEmbedded() throws ReaderClosedException, IOException, WriterClosedException {
    var indexReader = Mockito.mock(LuceneSearchIndexReader.class);
    Mockito.doReturn(25L).when(indexReader).getNumEmbeddedRootDocuments();
    var indexWriter = Mockito.mock(SingleLuceneIndexWriter.class);
    Mockito.doReturn(24L).when(indexWriter).getNumLuceneMaxDocs();
    Mockito.doReturn(18).when(indexWriter).getMaxLuceneMaxDocs();
    Mockito.doReturn(23L).when(indexWriter).getNumDocs();
    Mockito.doReturn(24L).when(indexReader).getRequiredMemoryForVectorData();
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    LuceneIndexMetricValuesSupplier luceneIndexMetricsSupplier =
        LuceneIndexMetricsSupplierBuilder.builder()
            .isEmbedded(true)
            .indexWriter(indexWriter)
            .indexReader(indexReader)
            .resolver(
                SearchIndexDefinitionBuilder.VALID_INDEX.createFieldDefinitionResolver(
                    IndexFormatVersion.CURRENT))
            .meterRegistry(meterRegistry)
            .build();

    DocCounts docCounts = luceneIndexMetricsSupplier.getDocCounts();
    Assert.assertEquals(25L, docCounts.numMongoDbDocs);
    Assert.assertEquals(24, docCounts.numLuceneMaxDocs);
    Assert.assertEquals(18, docCounts.maxLuceneMaxDocs);
    Assert.assertEquals(23, docCounts.numDocs);
    Assert.assertEquals(24, luceneIndexMetricsSupplier.getRequiredMemoryForVectorData());
    Assert.assertEquals(
        25,
        getPerIndexGaugeMetricsValue(
            meterRegistry,
            LuceneSearchIndexMetricValuesSupplier.MetricNames.NUM_EMBEDDED_ROOT_DOCS));
  }

  @Test
  public void testIndexPartitions()
      throws ReaderClosedException, IOException, WriterClosedException {
    var statusContainer = IndexStatus.steady();
    var numPartitions = 2;
    var indexReader = Mockito.mock(LuceneSearchIndexReader.class);
    Mockito.doReturn(25L).when(indexReader).getNumEmbeddedRootDocuments();
    var indexPartitionWriter1 = Mockito.mock(SingleLuceneIndexWriter.class);
    Mockito.doReturn(11L).when(indexPartitionWriter1).getNumLuceneMaxDocs();
    Mockito.doReturn(10L).when(indexPartitionWriter1).getNumDocs();
    Mockito.doReturn(900).when(indexPartitionWriter1).getNumFields();
    var indexPartitionWriter2 = Mockito.mock(SingleLuceneIndexWriter.class);
    Mockito.doReturn(13L).when(indexPartitionWriter2).getNumLuceneMaxDocs();
    Mockito.doReturn(12L).when(indexPartitionWriter2).getNumDocs();
    Mockito.doReturn(950).when(indexPartitionWriter2).getNumFields();
    Mockito.doReturn(24L).when(indexReader).getRequiredMemoryForVectorData();
    var indexWriter = Mockito.mock(MultiLuceneIndexWriter.class);
    when(indexWriter.getSingleLuceneIndexWriters())
        .thenReturn(List.of(indexPartitionWriter1, indexPartitionWriter2));
    Mockito.doReturn(2).when(indexWriter).getNumWriters();
    Mockito.doReturn(24L).when(indexWriter).getNumLuceneMaxDocs();
    IndexBackingStrategy indexBackingStrategy = mock(IndexBackingStrategy.class);
    when(indexBackingStrategy.getIndexSize()).thenReturn(1234L);
    when(indexBackingStrategy.getIndexSizeForIndexPartition(0)).thenReturn(610L);
    when(indexBackingStrategy.getIndexSizeForIndexPartition(1)).thenReturn(611L);
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    LuceneIndexMetricsSupplierBuilder.builder()
        .isEmbedded(false)
        .indexWriter(indexWriter)
        .indexReader(indexReader)
        .indexStatusSupplier(() -> statusContainer)
        .indexBackingStrategy(indexBackingStrategy)
        .meterRegistry(meterRegistry)
        .resolver(
            SearchIndexDefinitionBuilder.VALID_INDEX.createFieldDefinitionResolver(
                IndexFormatVersion.CURRENT))
        .build();
    Assert.assertEquals(
        610,
        getPerIndexPartitionGaugeMetricsValue(
            meterRegistry, IndexMetricValuesSupplier.MetricNames.INDEX_SIZE_BYTES, 0));
    Assert.assertEquals(
        611,
        getPerIndexPartitionGaugeMetricsValue(
            meterRegistry, IndexMetricValuesSupplier.MetricNames.INDEX_SIZE_BYTES, 1));
    Assert.assertEquals(
        10,
        getPerIndexPartitionGaugeMetricsValue(
            meterRegistry, IndexMetricValuesSupplier.MetricNames.NUM_LUCENE_DOCS, 0));
    Assert.assertEquals(
        12,
        getPerIndexPartitionGaugeMetricsValue(
            meterRegistry, IndexMetricValuesSupplier.MetricNames.NUM_LUCENE_DOCS, 1));
    Assert.assertEquals(
        900,
        getPerIndexPartitionGaugeMetricsValue(
            meterRegistry, IndexMetricValuesSupplier.MetricNames.NUM_LUCENE_FIELDS, 0));
    Assert.assertEquals(
        950,
        getPerIndexPartitionGaugeMetricsValue(
            meterRegistry, IndexMetricValuesSupplier.MetricNames.NUM_LUCENE_FIELDS, 1));
    Assert.assertEquals(
        11,
        getPerIndexPartitionGaugeMetricsValue(
            meterRegistry, IndexMetricValuesSupplier.MetricNames.NUM_LUCENE_MAX_DOCS, 0));
    Assert.assertEquals(
        13,
        getPerIndexPartitionGaugeMetricsValue(
            meterRegistry, IndexMetricValuesSupplier.MetricNames.NUM_LUCENE_MAX_DOCS, 1));
    Assert.assertEquals(
        24,
        getPerIndexGaugeMetricsValue(
            meterRegistry,
            IndexMetricValuesSupplier.MetricNames.NUM_LUCENE_MAX_DOCS,
            Tags.of("numPartitions", "2")));
    for (var i = 0; i < numPartitions; i++) {
      Assert.assertEquals(
          1,
          getPerIndexPartitionGaugeMetricsValue(
              meterRegistry,
              IndexMetricValuesSupplier.MetricNames.INDEX_STATUS_CODE,
              i,
              Tag.of("status", IndexStatus.StatusCode.STEADY.name())));
      Assert.assertEquals(
          0,
          getPerIndexPartitionGaugeMetricsValue(
              meterRegistry,
              IndexMetricValuesSupplier.MetricNames.INDEX_STATUS_CODE,
              i,
              Tag.of("status", IndexStatus.StatusCode.FAILED.name())));
    }
  }

  private static long getPerIndexGaugeMetricsValue(MeterRegistry meterRegistry, String metricName) {
    MetricsFactory metricsFactory =
        new MetricsFactory(LuceneIndexMetricsSupplierBuilder.METRICS_NAMESPACE, meterRegistry);
    return (long) metricsFactory.get(metricName).gauge().value();
  }

  private static long getPerIndexGaugeMetricsValue(
      MeterRegistry meterRegistry, String metricName, Tags tags) {
    MetricsFactory metricsFactory =
        new MetricsFactory(LuceneIndexMetricsSupplierBuilder.METRICS_NAMESPACE, meterRegistry);
    return (long) metricsFactory.get(metricName, tags).gauge().value();
  }

  private static long getPerIndexPartitionGaugeMetricsValue(
      MeterRegistry meterRegistry, String metricName, int indexPartitionId, Tag... additionalTags) {
    MetricsFactory metricsFactory =
        new MetricsFactory(LuceneIndexMetricsSupplierBuilder.METRICS_NAMESPACE, meterRegistry)
            .childMetricsFactory(LuceneIndexMetricValuesSupplier.INDEX_PARTITIONS_NAMESPACE);
    var tags =
        LuceneIndexMetricValuesSupplier.getIndexPartitionTags(indexPartitionId).and(additionalTags);
    return (long) metricsFactory.get(metricName, tags).gauge().value();
  }
}
