package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.CountResult;
import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.index.query.counts.Count;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFacetFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.bson.BsonInt64;
import org.junit.Test;

public class LuceneFacetResultUtilTest {

  private static final String PRICE_FIELD = "price";
  private static final String GROUP_FIELD = "group";

  @Test
  public void getCount_totalType_returnsTotal() {
    CountResult result = LuceneFacetResultUtil.getCount(42, Count.Type.TOTAL);

    assertThat(result.getTotal()).hasValue(42L);
    assertThat(result.getLowerBound()).isEmpty();
  }

  @Test
  public void getCount_lowerBoundType_returnsLowerBound() {
    CountResult result = LuceneFacetResultUtil.getCount(100, Count.Type.LOWER_BOUND);

    assertThat(result.getTotal()).isEmpty();
    assertThat(result.getLowerBound()).hasValue(100L);
  }

  @Test
  public void getCount_zeroValue_returnsCorrectResult() {
    CountResult totalResult = LuceneFacetResultUtil.getCount(0, Count.Type.TOTAL);
    CountResult lowerBoundResult = LuceneFacetResultUtil.getCount(0, Count.Type.LOWER_BOUND);

    assertThat(totalResult.getTotal()).hasValue(0L);
    assertThat(lowerBoundResult.getLowerBound()).hasValue(0L);
  }

  @Test
  public void groupFacetableStringDefinitions_emptyMap_returnsEmptyGroups()
      throws InvalidQueryException {
    LuceneFacetContext facetContext = createFacetContextWithTokenField("color");
    Map<String, FacetDefinition> emptyDefinitions = new HashMap<>();

    Map<FieldTypeDefinition.Type, Map<String, FacetDefinition.StringFacetDefinition>> result =
        LuceneFacetResultUtil.groupFacetableStringDefinitions(
            facetContext, emptyDefinitions, Optional.empty());

    assertThat(result).containsKey(FieldTypeDefinition.Type.TOKEN);
    assertThat(result).containsKey(FieldTypeDefinition.Type.STRING_FACET);
    assertThat(result.get(FieldTypeDefinition.Type.TOKEN)).isEmpty();
    assertThat(result.get(FieldTypeDefinition.Type.STRING_FACET)).isEmpty();
  }

  @Test
  public void groupFacetableStringDefinitions_singleTokenFacet_groupsToTokenType()
      throws InvalidQueryException {
    LuceneFacetContext facetContext = createFacetContextWithTokenField("color");
    Map<String, FacetDefinition> definitions = new HashMap<>();
    FacetDefinition.StringFacetDefinition colorFacet =
        new FacetDefinition.StringFacetDefinition("color", 10);
    definitions.put("colorFacet", colorFacet);

    Map<FieldTypeDefinition.Type, Map<String, FacetDefinition.StringFacetDefinition>> result =
        LuceneFacetResultUtil.groupFacetableStringDefinitions(
            facetContext, definitions, Optional.empty());

    assertThat(result.get(FieldTypeDefinition.Type.TOKEN)).hasSize(1);
    assertThat(result.get(FieldTypeDefinition.Type.TOKEN)).containsEntry("colorFacet", colorFacet);
    assertThat(result.get(FieldTypeDefinition.Type.STRING_FACET)).isEmpty();
  }

  @Test
  public void groupFacetableStringDefinitions_singleStringFacetField_groupsToStringFacetType()
      throws InvalidQueryException {
    LuceneFacetContext facetContext = createFacetContextWithStringFacetField("category");
    Map<String, FacetDefinition> definitions = new HashMap<>();
    FacetDefinition.StringFacetDefinition categoryFacet =
        new FacetDefinition.StringFacetDefinition("category", 5);
    definitions.put("categoryFacet", categoryFacet);

    Map<FieldTypeDefinition.Type, Map<String, FacetDefinition.StringFacetDefinition>> result =
        LuceneFacetResultUtil.groupFacetableStringDefinitions(
            facetContext, definitions, Optional.empty());

    assertThat(result.get(FieldTypeDefinition.Type.TOKEN)).isEmpty();
    assertThat(result.get(FieldTypeDefinition.Type.STRING_FACET)).hasSize(1);
    assertThat(result.get(FieldTypeDefinition.Type.STRING_FACET))
        .containsEntry("categoryFacet", categoryFacet);
  }

  @Test
  public void groupFacetableStringDefinitions_multipleFieldTypes_groupsByType()
      throws InvalidQueryException {
    LuceneFacetContext facetContext = createFacetContextWithBothFieldTypes("color", "category");
    Map<String, FacetDefinition> definitions = new HashMap<>();
    FacetDefinition.StringFacetDefinition colorFacet =
        new FacetDefinition.StringFacetDefinition("color", 10);
    FacetDefinition.StringFacetDefinition categoryFacet =
        new FacetDefinition.StringFacetDefinition("category", 5);
    definitions.put("colorFacet", colorFacet);
    definitions.put("categoryFacet", categoryFacet);

    Map<FieldTypeDefinition.Type, Map<String, FacetDefinition.StringFacetDefinition>> result =
        LuceneFacetResultUtil.groupFacetableStringDefinitions(
            facetContext, definitions, Optional.empty());

    assertThat(result.get(FieldTypeDefinition.Type.TOKEN)).hasSize(1);
    assertThat(result.get(FieldTypeDefinition.Type.TOKEN)).containsEntry("colorFacet", colorFacet);
    assertThat(result.get(FieldTypeDefinition.Type.STRING_FACET)).hasSize(1);
    assertThat(result.get(FieldTypeDefinition.Type.STRING_FACET))
        .containsEntry("categoryFacet", categoryFacet);
  }

  @Test
  public void groupFacetableStringDefinitions_mixedFacetTypes_skipsNonStringFacets()
      throws InvalidQueryException {
    LuceneFacetContext facetContext = createFacetContextWithTokenField("color");
    Map<String, FacetDefinition> definitions = new HashMap<>();
    FacetDefinition.StringFacetDefinition colorFacet =
        new FacetDefinition.StringFacetDefinition("color", 10);
    definitions.put("colorFacet", colorFacet);
    FacetDefinition.NumericFacetDefinition numericFacet =
        new FacetDefinition.NumericFacetDefinition(
            "price", Optional.empty(), List.of(new BsonInt64(0)));
    definitions.put("priceFacet", numericFacet);

    Map<FieldTypeDefinition.Type, Map<String, FacetDefinition.StringFacetDefinition>> result =
        LuceneFacetResultUtil.groupFacetableStringDefinitions(
            facetContext, definitions, Optional.empty());

    assertThat(result.get(FieldTypeDefinition.Type.TOKEN)).hasSize(1);
    assertThat(result.get(FieldTypeDefinition.Type.TOKEN)).containsEntry("colorFacet", colorFacet);
  }

  @Test
  public void getBoundaryFacetResult_numericFacet_returnsCorrectBuckets()
      throws IOException, InvalidQueryException {
    // Create facet context with number field first, then get the exact field name it will use
    LuceneFacetContext facetContext = createFacetContextWithNumberField(PRICE_FIELD);

    // Create numeric facet definition with boundaries: [0, 10, 20, 30]
    // This creates 3 ranges: [0-10), [10-20), [20-30)
    FacetDefinition.NumericFacetDefinition facetDefinition =
        new FacetDefinition.NumericFacetDefinition(
            PRICE_FIELD,
            Optional.empty(),
            List.of(new BsonInt64(0), new BsonInt64(10), new BsonInt64(20), new BsonInt64(30)));

    // Get the exact Lucene field name that will be used by getBoundaryFacetResult
    String luceneFieldName = facetContext.getBoundaryFacetPath(facetDefinition, Optional.empty());

    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {

      // Add documents with numeric values in different ranges
      // Range 0-10: 3 docs (values: 5, 7, 9)
      // Range 10-20: 2 docs (values: 12, 15)
      // Range 20-30: 1 doc (value: 25)
      writer.addDocument(createNumericDoc(luceneFieldName, 5, "hit"));
      writer.addDocument(createNumericDoc(luceneFieldName, 7, "hit"));
      writer.addDocument(createNumericDoc(luceneFieldName, 9, "hit"));
      writer.addDocument(createNumericDoc(luceneFieldName, 12, "hit"));
      writer.addDocument(createNumericDoc(luceneFieldName, 15, "hit"));
      writer.addDocument(createNumericDoc(luceneFieldName, 25, "hit"));
      // Add some docs that won't match our query
      writer.addDocument(createNumericDoc(luceneFieldName, 100, "miss"));
      writer.commit();

      try (var reader = DirectoryReader.open(directory)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        // Collect facets for "hit" documents
        var collector = new FacetsCollector();
        var query = new TermQuery(new Term(GROUP_FIELD, "hit"));
        var topDocs = FacetsCollector.search(searcher, query, 100, collector);

        // Verify we found the expected number of documents
        assertThat(topDocs.totalHits.value).isEqualTo(6);

        // Call the method under test
        FacetResult result =
            LuceneFacetResultUtil.getBoundaryFacetResult(
                facetDefinition, facetContext, collector, Optional.empty());

        // Verify results
        assertThat(result).isNotNull();
        assertThat(result.labelValues).hasLength(3);

        // Bucket [0-10): 3 docs
        // Labels are BsonValue.toString(), e.g. "BsonInt64{value=0}"
        assertThat(result.labelValues[0].value.intValue()).isEqualTo(3);

        // Bucket [10-20): 2 docs
        assertThat(result.labelValues[1].value.intValue()).isEqualTo(2);

        // Bucket [20-30): 1 doc
        assertThat(result.labelValues[2].value.intValue()).isEqualTo(1);
      }
    }
  }

  @Test
  public void getBoundaryFacetResult_emptyCollection_returnsZeroCounts()
      throws IOException, InvalidQueryException {
    LuceneFacetContext facetContext = createFacetContextWithNumberField(PRICE_FIELD);

    FacetDefinition.NumericFacetDefinition facetDefinition =
        new FacetDefinition.NumericFacetDefinition(
            PRICE_FIELD,
            Optional.empty(),
            List.of(new BsonInt64(0), new BsonInt64(10), new BsonInt64(20)));

    String luceneFieldName = facetContext.getBoundaryFacetPath(facetDefinition, Optional.empty());

    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {

      // Add documents that won't match our query
      writer.addDocument(createNumericDoc(luceneFieldName, 15, "miss"));
      writer.commit();

      try (var reader = DirectoryReader.open(directory)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        // Collect facets for "hit" documents (none exist)
        var collector = new FacetsCollector();
        var query = new TermQuery(new Term(GROUP_FIELD, "hit"));
        FacetsCollector.search(searcher, query, 100, collector);

        FacetResult result =
            LuceneFacetResultUtil.getBoundaryFacetResult(
                facetDefinition, facetContext, collector, Optional.empty());

        assertThat(result).isNotNull();
        assertThat(result.labelValues).hasLength(2);
        assertThat(result.labelValues[0].value.intValue()).isEqualTo(0);
        assertThat(result.labelValues[1].value.intValue()).isEqualTo(0);
      }
    }
  }

  private static Document createNumericDoc(String fieldName, long value, String group) {
    Document doc = new Document();
    doc.add(new NumericDocValuesField(fieldName, value));
    doc.add(new StringField(GROUP_FIELD, group, Field.Store.NO));
    return doc;
  }

  private static LuceneFacetContext createFacetContextWithNumberField(String fieldName) {
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        fieldName,
                        FieldDefinitionBuilder.builder()
                            .number(
                                NumericFieldDefinitionBuilder.builder()
                                    .representation(NumericFieldOptions.Representation.INT64)
                                    .buildNumberField())
                            .build())
                    .build())
            .build();

    return new LuceneFacetContext(
        indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
        indexDefinition.getIndexCapabilities(IndexFormatVersion.CURRENT));
  }

  private static LuceneFacetContext createFacetContextWithTokenField(String fieldName) {
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        fieldName,
                        FieldDefinitionBuilder.builder()
                            .token(TokenFieldDefinitionBuilder.builder().build())
                            .build())
                    .build())
            .build();

    return new LuceneFacetContext(
        indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
        indexDefinition.getIndexCapabilities(IndexFormatVersion.CURRENT));
  }

  private static LuceneFacetContext createFacetContextWithStringFacetField(String fieldName) {
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        fieldName,
                        FieldDefinitionBuilder.builder()
                            .stringFacet(StringFacetFieldDefinitionBuilder.builder().build())
                            .build())
                    .build())
            .build();

    return new LuceneFacetContext(
        indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
        indexDefinition.getIndexCapabilities(IndexFormatVersion.CURRENT));
  }

  private static LuceneFacetContext createFacetContextWithBothFieldTypes(
      String tokenFieldName, String stringFacetFieldName) {
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        tokenFieldName,
                        FieldDefinitionBuilder.builder()
                            .token(TokenFieldDefinitionBuilder.builder().build())
                            .build())
                    .field(
                        stringFacetFieldName,
                        FieldDefinitionBuilder.builder()
                            .stringFacet(StringFacetFieldDefinitionBuilder.builder().build())
                            .build())
                    .build())
            .build();

    return new LuceneFacetContext(
        indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
        indexDefinition.getIndexCapabilities(IndexFormatVersion.CURRENT));
  }
}
