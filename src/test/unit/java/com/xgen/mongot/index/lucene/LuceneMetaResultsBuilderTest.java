package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.FacetBucket;
import com.xgen.mongot.index.FacetInfo;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.NumericFieldOptions;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.codec.LuceneCodec;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.index.query.counts.Count;
import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFacetFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.query.CollectorQueryBuilder;
import com.xgen.testing.mongot.index.query.collectors.FacetCollectorBuilder;
import com.xgen.testing.mongot.index.query.counts.CountBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.junit.Test;

public class LuceneMetaResultsBuilderTest {

  private static final String PRICE_FIELD = "price";
  private static final String CATEGORY_FIELD = "category";
  private static final String GROUP_FIELD = "group";

  @Test
  public void getCountMetaResults_totalType_returnsTotal() {
    LuceneFacetContext facetContext = createFacetContextWithNumberField(PRICE_FIELD);
    var builder = new LuceneMetaResultsBuilder(facetContext, Optional.empty());

    TopDocs topDocs = new TopDocs(new TotalHits(42, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
    MetaResults result = builder.getCountMetaResults(topDocs, Count.Type.TOTAL);

    assertThat(result.count().getTotal()).hasValue(42L);
    assertThat(result.count().getLowerBound()).isEmpty();
  }

  @Test
  public void getCountMetaResults_lowerBoundType_returnsLowerBound() {
    LuceneFacetContext facetContext = createFacetContextWithNumberField(PRICE_FIELD);
    var builder = new LuceneMetaResultsBuilder(facetContext, Optional.empty());

    TopDocs topDocs = new TopDocs(new TotalHits(100, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
    MetaResults result = builder.getCountMetaResults(topDocs, Count.Type.LOWER_BOUND);

    assertThat(result.count().getTotal()).isEmpty();
    assertThat(result.count().getLowerBound()).hasValue(100L);
  }

  @Test
  public void getCountMetaResults_zeroCount_returnsZero() {
    LuceneFacetContext facetContext = createFacetContextWithNumberField(PRICE_FIELD);
    var builder = new LuceneMetaResultsBuilder(facetContext, Optional.empty());

    TopDocs topDocs = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
    MetaResults totalResult = builder.getCountMetaResults(topDocs, Count.Type.TOTAL);
    MetaResults lowerBoundResult = builder.getCountMetaResults(topDocs, Count.Type.LOWER_BOUND);

    assertThat(totalResult.count().getTotal()).hasValue(0L);
    assertThat(lowerBoundResult.count().getLowerBound()).hasValue(0L);
  }

  @Test
  public void buildFacetMetaResults_numericFacet_returnsCorrectBuckets()
      throws IOException, InterruptedException, InvalidQueryException {
    // Create facet context with number field
    LuceneFacetContext facetContext = createFacetContextWithNumberField(PRICE_FIELD);

    // Create numeric facet definition with boundaries: [0, 10, 20, 30]
    // This creates 3 ranges: [0-10), [10-20), [20-30)
    FacetDefinition.NumericFacetDefinition numericFacet =
        new FacetDefinition.NumericFacetDefinition(
            PRICE_FIELD,
            Optional.empty(), // no default bucket
            List.of(new BsonInt64(0), new BsonInt64(10), new BsonInt64(20), new BsonInt64(30)));

    // Get the Lucene field name for this facet
    String luceneFieldName = facetContext.getBoundaryFacetPath(numericFacet, Optional.empty());

    // Create CollectorQuery using builders
    FacetCollector collector =
        new FacetCollectorBuilder()
            .operator(AllDocumentsOperator.INSTANCE)
            .facetDefinitions(Map.of("priceFacet", numericFacet))
            .build();
    CollectorQuery collectorQuery =
        CollectorQueryBuilder.builder()
            .collector(collector)
            .count(CountBuilder.builder().type(Count.Type.TOTAL).build())
            .returnStoredSource(false)
            .build();

    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {

      // Add documents with numeric values in different ranges
      // Range [0-10): 3 docs (values: 5, 7, 9)
      // Range [10-20): 2 docs (values: 12, 15)
      // Range [20-30): 1 doc (value: 25)
      writer.addDocument(createNumericDoc(luceneFieldName, 5, "hit"));
      writer.addDocument(createNumericDoc(luceneFieldName, 7, "hit"));
      writer.addDocument(createNumericDoc(luceneFieldName, 9, "hit"));
      writer.addDocument(createNumericDoc(luceneFieldName, 12, "hit"));
      writer.addDocument(createNumericDoc(luceneFieldName, 15, "hit"));
      writer.addDocument(createNumericDoc(luceneFieldName, 25, "hit"));
      // Add some docs that won't match our query
      writer.addDocument(createNumericDoc(luceneFieldName, 100, "miss"));
      writer.commit();

      // Create searcher infrastructure
      var searcherManager =
          new LuceneSearcherManager(
              writer,
              new LuceneSearcherFactory(
                  SearchIndex.MOCK_FACET_INDEX_DEFINITION,
                  false,
                  new QueryCacheProvider.DefaultQueryCacheProvider(),
                  Optional.empty(),
                  SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH)),
              SearchIndex.mockMetricsFactory());
      var searcherReference =
          LuceneIndexSearcherReference.create(
              searcherManager,
              SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH),
              FeatureFlags.getDefault());

      // Collect facets
      var facetsCollector = new FacetsCollector();
      var query = new TermQuery(new Term(GROUP_FIELD, "hit"));
      var topDocs =
          FacetsCollector.search(searcherReference.getIndexSearcher(), query, 100, facetsCollector);

      // Verify we found the expected number of documents
      assertEquals(6, topDocs.totalHits.value);

      // Create and invoke LuceneMetaResultsBuilder
      var metaResultsBuilder = new LuceneMetaResultsBuilder(facetContext, Optional.empty());
      MetaResults metaResults =
          metaResultsBuilder.buildFacetMetaResults(
              searcherReference, topDocs, facetsCollector, collectorQuery, false);

      // Assert count results
      assertThat(metaResults.count().getTotal()).hasValue(6L);

      // Assert facet results
      assertThat(metaResults.facet()).isPresent();
      Map<String, FacetInfo> facets = metaResults.facet().get();
      assertThat(facets).containsKey("priceFacet");

      FacetInfo priceFacetInfo = facets.get("priceFacet");
      List<FacetBucket> buckets = priceFacetInfo.buckets();
      assertEquals(3, buckets.size());

      // Bucket [0-10): 3 docs
      assertEquals(new BsonInt64(0), buckets.get(0).getId());
      assertEquals(3L, buckets.get(0).getCount());

      // Bucket [10-20): 2 docs
      assertEquals(new BsonInt64(10), buckets.get(1).getId());
      assertEquals(2L, buckets.get(1).getCount());

      // Bucket [20-30): 1 doc
      assertEquals(new BsonInt64(20), buckets.get(2).getId());
      assertEquals(1L, buckets.get(2).getCount());
    }
  }

  @Test
  public void buildFacetMetaResults_numericFacetWithDefaultBucket_calculatesDefaultCorrectly()
      throws IOException, InterruptedException, InvalidQueryException {
    LuceneFacetContext facetContext = createFacetContextWithNumberField(PRICE_FIELD);

    // Create numeric facet with a default bucket for values outside boundaries
    FacetDefinition.NumericFacetDefinition numericFacet =
        new FacetDefinition.NumericFacetDefinition(
            PRICE_FIELD,
            Optional.of("other"), // default bucket name
            List.of(new BsonInt64(0), new BsonInt64(10), new BsonInt64(20)));

    String luceneFieldName = facetContext.getBoundaryFacetPath(numericFacet, Optional.empty());

    FacetCollector collector =
        new FacetCollectorBuilder()
            .operator(AllDocumentsOperator.INSTANCE)
            .facetDefinitions(Map.of("priceFacet", numericFacet))
            .build();
    CollectorQuery collectorQuery =
        CollectorQueryBuilder.builder()
            .collector(collector)
            .count(CountBuilder.builder().type(Count.Type.TOTAL).build())
            .returnStoredSource(false)
            .build();

    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {

      // Add documents:
      // Range [0-10): 2 docs
      // Range [10-20): 1 doc
      // Default bucket (outside ranges): 3 docs (values: -5, 25, 100)
      writer.addDocument(createNumericDoc(luceneFieldName, 5, "hit"));
      writer.addDocument(createNumericDoc(luceneFieldName, 8, "hit"));
      writer.addDocument(createNumericDoc(luceneFieldName, 15, "hit"));
      writer.addDocument(createNumericDoc(luceneFieldName, -5, "hit")); // default bucket
      writer.addDocument(createNumericDoc(luceneFieldName, 25, "hit")); // default bucket
      writer.addDocument(createNumericDoc(luceneFieldName, 100, "hit")); // default bucket
      writer.commit();

      var searcherManager =
          new LuceneSearcherManager(
              writer,
              new LuceneSearcherFactory(
                  SearchIndex.MOCK_FACET_INDEX_DEFINITION,
                  false,
                  new QueryCacheProvider.DefaultQueryCacheProvider(),
                  Optional.empty(),
                  SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH)),
              SearchIndex.mockMetricsFactory());
      var searcherReference =
          LuceneIndexSearcherReference.create(
              searcherManager,
              SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH),
              FeatureFlags.getDefault());

      var facetsCollector = new FacetsCollector();
      var query = new TermQuery(new Term(GROUP_FIELD, "hit"));
      var topDocs =
          FacetsCollector.search(searcherReference.getIndexSearcher(), query, 100, facetsCollector);

      assertEquals(6, topDocs.totalHits.value);

      var metaResultsBuilder = new LuceneMetaResultsBuilder(facetContext, Optional.empty());
      MetaResults metaResults =
          metaResultsBuilder.buildFacetMetaResults(
              searcherReference, topDocs, facetsCollector, collectorQuery, false);

      assertThat(metaResults.count().getTotal()).hasValue(6L);
      assertThat(metaResults.facet()).isPresent();

      FacetInfo priceFacetInfo = metaResults.facet().get().get("priceFacet");
      List<FacetBucket> buckets = priceFacetInfo.buckets();

      // Should have 3 buckets: [0-10), [10-20), and default
      assertEquals(3, buckets.size());

      // Bucket [0-10): 2 docs
      assertEquals(new BsonInt64(0), buckets.get(0).getId());
      assertEquals(2L, buckets.get(0).getCount());

      // Bucket [10-20): 1 doc
      assertEquals(new BsonInt64(10), buckets.get(1).getId());
      assertEquals(1L, buckets.get(1).getCount());

      // Default bucket: 3 docs (totalCount - facetDocsCount = 6 - 3 = 3)
      assertEquals(new BsonString("other"), buckets.get(2).getId());
      assertEquals(3L, buckets.get(2).getCount());
    }
  }

  @Test
  public void buildFacetMetaResults_emptyResults_returnsZeroCounts()
      throws IOException, InterruptedException, InvalidQueryException {
    LuceneFacetContext facetContext = createFacetContextWithNumberField(PRICE_FIELD);

    FacetDefinition.NumericFacetDefinition numericFacet =
        new FacetDefinition.NumericFacetDefinition(
            PRICE_FIELD,
            Optional.empty(),
            List.of(new BsonInt64(0), new BsonInt64(10), new BsonInt64(20)));

    String luceneFieldName = facetContext.getBoundaryFacetPath(numericFacet, Optional.empty());

    FacetCollector collector =
        new FacetCollectorBuilder()
            .operator(AllDocumentsOperator.INSTANCE)
            .facetDefinitions(Map.of("priceFacet", numericFacet))
            .build();
    CollectorQuery collectorQuery =
        CollectorQueryBuilder.builder()
            .collector(collector)
            .count(CountBuilder.builder().type(Count.Type.TOTAL).build())
            .returnStoredSource(false)
            .build();

    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {

      // Add only documents that won't match our query
      writer.addDocument(createNumericDoc(luceneFieldName, 15, "miss"));
      writer.commit();

      var searcherManager =
          new LuceneSearcherManager(
              writer,
              new LuceneSearcherFactory(
                  SearchIndex.MOCK_FACET_INDEX_DEFINITION,
                  false,
                  new QueryCacheProvider.DefaultQueryCacheProvider(),
                  Optional.empty(),
                  SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH)),
              SearchIndex.mockMetricsFactory());
      var searcherReference =
          LuceneIndexSearcherReference.create(
              searcherManager,
              SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH),
              FeatureFlags.getDefault());

      var facetsCollector = new FacetsCollector();
      var query = new TermQuery(new Term(GROUP_FIELD, "hit"));
      var topDocs =
          FacetsCollector.search(searcherReference.getIndexSearcher(), query, 100, facetsCollector);

      assertEquals(0, topDocs.totalHits.value);

      var metaResultsBuilder = new LuceneMetaResultsBuilder(facetContext, Optional.empty());
      MetaResults metaResults =
          metaResultsBuilder.buildFacetMetaResults(
              searcherReference, topDocs, facetsCollector, collectorQuery, false);

      assertThat(metaResults.count().getTotal()).hasValue(0L);
      assertThat(metaResults.facet()).isPresent();

      FacetInfo priceFacetInfo = metaResults.facet().get().get("priceFacet");
      List<FacetBucket> buckets = priceFacetInfo.buckets();

      // Should have 2 buckets with zero counts
      assertEquals(2, buckets.size());
      assertEquals(0L, buckets.get(0).getCount());
      assertEquals(0L, buckets.get(1).getCount());
    }
  }

  @Test
  public void buildFacetMetaResults_stringFacet_returnsCorrectBuckets()
      throws IOException, InterruptedException, InvalidQueryException {
    LuceneFacetContext facetContext = createFacetContextWithStringFacetField(CATEGORY_FIELD);

    FacetDefinition.StringFacetDefinition stringFacet =
        new FacetDefinition.StringFacetDefinition(CATEGORY_FIELD, 10);

    FacetCollector collector =
        new FacetCollectorBuilder()
            .operator(AllDocumentsOperator.INSTANCE)
            .facetDefinitions(Map.of("categoryFacet", stringFacet))
            .build();
    CollectorQuery collectorQuery =
        CollectorQueryBuilder.builder()
            .collector(collector)
            .count(CountBuilder.builder().type(Count.Type.TOTAL).build())
            .returnStoredSource(false)
            .build();

    var facetsConfig = new FacetsConfig();

    try (var directory = new ByteBuffersDirectory();
        var writer =
            new IndexWriter(directory, new IndexWriterConfig().setCodec(new LuceneCodec()))) {

      // Add documents with different category values
      // electronics: 3 docs
      // books: 2 docs
      // clothing: 1 doc
      writer.addDocument(
          facetsConfig.build(createStringFacetDoc(CATEGORY_FIELD, "electronics", "hit")));
      writer.addDocument(
          facetsConfig.build(createStringFacetDoc(CATEGORY_FIELD, "electronics", "hit")));
      writer.addDocument(
          facetsConfig.build(createStringFacetDoc(CATEGORY_FIELD, "electronics", "hit")));
      writer.addDocument(facetsConfig.build(createStringFacetDoc(CATEGORY_FIELD, "books", "hit")));
      writer.addDocument(facetsConfig.build(createStringFacetDoc(CATEGORY_FIELD, "books", "hit")));
      writer.addDocument(
          facetsConfig.build(createStringFacetDoc(CATEGORY_FIELD, "clothing", "hit")));
      // Add doc that won't match
      writer.addDocument(facetsConfig.build(createStringFacetDoc(CATEGORY_FIELD, "toys", "miss")));
      writer.commit();

      var searcherManager =
          new LuceneSearcherManager(
              writer,
              new LuceneSearcherFactory(
                  SearchIndex.MOCK_FACET_INDEX_DEFINITION,
                  false,
                  new QueryCacheProvider.DefaultQueryCacheProvider(),
                  Optional.empty(),
                  SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH)),
              SearchIndex.mockMetricsFactory());
      var searcherReference =
          LuceneIndexSearcherReference.create(
              searcherManager,
              SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH),
              FeatureFlags.getDefault());

      var facetsCollector = new FacetsCollector();
      var query = new TermQuery(new Term(GROUP_FIELD, "hit"));
      var topDocs =
          FacetsCollector.search(searcherReference.getIndexSearcher(), query, 100, facetsCollector);

      assertEquals(6, topDocs.totalHits.value);

      var metaResultsBuilder = new LuceneMetaResultsBuilder(facetContext, Optional.empty());
      MetaResults metaResults =
          metaResultsBuilder.buildFacetMetaResults(
              searcherReference, topDocs, facetsCollector, collectorQuery, false);

      assertThat(metaResults.count().getTotal()).hasValue(6L);
      assertThat(metaResults.facet()).isPresent();

      FacetInfo categoryFacetInfo = metaResults.facet().get().get("categoryFacet");
      List<FacetBucket> buckets = categoryFacetInfo.buckets();

      // Should have 3 buckets sorted by count (descending)
      assertEquals(3, buckets.size());

      // electronics: 3 docs (highest count)
      assertEquals(new BsonString("electronics"), buckets.get(0).getId());
      assertEquals(3L, buckets.get(0).getCount());

      // books: 2 docs
      assertEquals(new BsonString("books"), buckets.get(1).getId());
      assertEquals(2L, buckets.get(1).getCount());

      // clothing: 1 doc
      assertEquals(new BsonString("clothing"), buckets.get(2).getId());
      assertEquals(1L, buckets.get(2).getCount());
    }
  }

  @Test
  public void buildFacetMetaResults_stringFacet_respectsNumBuckets()
      throws IOException, InterruptedException, InvalidQueryException {
    LuceneFacetContext facetContext = createFacetContextWithStringFacetField(CATEGORY_FIELD);

    // Request only top 2 buckets
    FacetDefinition.StringFacetDefinition stringFacet =
        new FacetDefinition.StringFacetDefinition(CATEGORY_FIELD, 2);

    FacetCollector collector =
        new FacetCollectorBuilder()
            .operator(AllDocumentsOperator.INSTANCE)
            .facetDefinitions(Map.of("categoryFacet", stringFacet))
            .build();
    CollectorQuery collectorQuery =
        CollectorQueryBuilder.builder()
            .collector(collector)
            .count(CountBuilder.builder().type(Count.Type.TOTAL).build())
            .returnStoredSource(false)
            .build();

    var facetsConfig = new FacetsConfig();

    try (var directory = new ByteBuffersDirectory();
        var writer =
            new IndexWriter(directory, new IndexWriterConfig().setCodec(new LuceneCodec()))) {

      // Add documents with different category values
      // electronics: 4 docs
      // books: 3 docs
      // clothing: 2 docs
      // toys: 1 doc
      for (int i = 0; i < 4; i++) {
        writer.addDocument(
            facetsConfig.build(createStringFacetDoc(CATEGORY_FIELD, "electronics", "hit")));
      }
      for (int i = 0; i < 3; i++) {
        writer.addDocument(
            facetsConfig.build(createStringFacetDoc(CATEGORY_FIELD, "books", "hit")));
      }
      for (int i = 0; i < 2; i++) {
        writer.addDocument(
            facetsConfig.build(createStringFacetDoc(CATEGORY_FIELD, "clothing", "hit")));
      }
      writer.addDocument(facetsConfig.build(createStringFacetDoc(CATEGORY_FIELD, "toys", "hit")));
      writer.commit();

      var searcherManager =
          new LuceneSearcherManager(
              writer,
              new LuceneSearcherFactory(
                  SearchIndex.MOCK_FACET_INDEX_DEFINITION,
                  false,
                  new QueryCacheProvider.DefaultQueryCacheProvider(),
                  Optional.empty(),
                  SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH)),
              SearchIndex.mockMetricsFactory());
      var searcherReference =
          LuceneIndexSearcherReference.create(
              searcherManager,
              SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH),
              FeatureFlags.getDefault());

      var facetsCollector = new FacetsCollector();
      var topDocs =
          FacetsCollector.search(
              searcherReference.getIndexSearcher(), new MatchAllDocsQuery(), 100, facetsCollector);

      assertEquals(10, topDocs.totalHits.value);

      var metaResultsBuilder = new LuceneMetaResultsBuilder(facetContext, Optional.empty());
      MetaResults metaResults =
          metaResultsBuilder.buildFacetMetaResults(
              searcherReference, topDocs, facetsCollector, collectorQuery, false);

      assertThat(metaResults.facet()).isPresent();

      FacetInfo categoryFacetInfo = metaResults.facet().get().get("categoryFacet");
      List<FacetBucket> buckets = categoryFacetInfo.buckets();

      // Should only return top 2 buckets due to numBuckets limit
      assertEquals(2, buckets.size());

      // electronics: 4 docs (highest count)
      assertEquals(new BsonString("electronics"), buckets.get(0).getId());
      assertEquals(4L, buckets.get(0).getCount());

      // books: 3 docs (second highest)
      assertEquals(new BsonString("books"), buckets.get(1).getId());
      assertEquals(3L, buckets.get(1).getCount());
    }
  }

  private static Document createNumericDoc(String fieldName, long value, String group) {
    Document doc = new Document();
    doc.add(new NumericDocValuesField(fieldName, value));
    doc.add(new StringField(GROUP_FIELD, group, Field.Store.NO));
    return doc;
  }

  private static Document createStringFacetDoc(String facetDim, String facetValue, String group) {
    Document doc = new Document();
    doc.add(new SortedSetDocValuesFacetField(facetDim, facetValue));
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
}
