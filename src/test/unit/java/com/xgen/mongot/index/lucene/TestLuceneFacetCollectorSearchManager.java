package com.xgen.mongot.index.lucene;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.codec.LuceneCodec;
import com.xgen.mongot.index.lucene.explain.explainers.SortFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.information.CollectorExplainInformation;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.sort.LuceneSortFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.lucene.synonym.SynonymRegistryBuilder;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.bson.BsonString;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestLuceneFacetCollectorSearchManager {

  private final IndexWriter indexWriter;
  private final Directory directory;
  private final SearchQueryFactoryContext queryFactoryContext;

  public TestLuceneFacetCollectorSearchManager() throws IOException {
    this.directory = new ByteBuffersDirectory();
    this.indexWriter =
        new IndexWriter(
            this.directory,
            new org.apache.lucene.index.IndexWriterConfig(
                    LuceneAnalyzer.indexAnalyzer(
                        SearchIndex.MOCK_INDEX_DEFINITION, AnalyzerRegistryBuilder.empty()))
                .setCodec(new LuceneCodec()));
    var fieldDefinitionResolver =
        SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
            SearchIndex.MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion);

    this.queryFactoryContext =
        new SearchQueryFactoryContext(
            AnalyzerRegistryBuilder.empty(),
            LuceneAnalyzer.queryAnalyzer(
                SearchIndex.MOCK_INDEX_DEFINITION, AnalyzerRegistryBuilder.empty()),
            fieldDefinitionResolver,
            SynonymRegistryBuilder.empty(),
            new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
            FeatureFlags.getDefault());
  }

  @After
  public void after() throws IOException {
    this.indexWriter.close();
    this.directory.close();
  }

  /** Create a document with facet field "facetField" and value {@code facetValue}. */
  private static Document createSimpleDoc(String facetValue) {
    return createSortableDoc("sortField", "0", facetValue);
  }

  /** Create a document that has a sortable string field with the given name, value, and facet. */
  private static Document createSortableDoc(
      String sortableFieldName, String sortValue, String facetValue) {
    Document doc = new Document();
    var docTerm = new Term(sortableFieldName, sortValue).bytes();
    doc.add(new SortedSetDocValuesField(sortableFieldName, docTerm));
    doc.add(new StringField(sortableFieldName, docTerm, Field.Store.NO));
    doc.add(new SortedSetDocValuesFacetField("facetField", facetValue));
    return doc;
  }

  @Test
  public void testFacetSearchWithoutSort() throws IOException {
    var manager =
        new LuceneFacetCollectorSearchManager(
            new MatchAllDocsQuery(), Optional.empty(), Optional.empty());

    var config = new FacetsConfig();
    var doc1 = createSimpleDoc("a");
    var doc2 = createSimpleDoc("a");
    var doc3 = createSimpleDoc("b");
    this.indexWriter.addDocument(config.build(doc1));
    this.indexWriter.addDocument(config.build(doc2));
    this.indexWriter.addDocument(config.build(doc3));
    this.indexWriter.commit();

    var searcherManager =
        new LuceneSearcherManager(
            this.indexWriter,
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

    var queryInfo = manager.initialSearch(searcherReference, 2);

    var readerState = searcherReference.getIndexSearcher().getFacetsState().get();
    var counts = new SortedSetDocValuesFacetCounts(readerState, queryInfo.collector);

    var initialScoreDocs = queryInfo.topDocs.scoreDocs;
    assertEquals(2, counts.getSpecificValue("facetField", "a").intValue());
    assertEquals(1, counts.getSpecificValue("facetField", "b").intValue());
    TestUtils.assertHasValidScores(initialScoreDocs);
    TestUtils.assertHasDocIds(initialScoreDocs, 0, 1);

    var getMoreDocs =
        manager.getMoreTopDocs(searcherReference, initialScoreDocs[initialScoreDocs.length - 1], 2);
    TestUtils.assertHasValidScores(getMoreDocs.scoreDocs);
    TestUtils.assertHasDocIds(getMoreDocs.scoreDocs, 2);
  }

  @Test
  public void testFacetSortWithSearchAfter() throws IOException {
    var config = new FacetsConfig();
    FieldPath fieldPath = FieldPath.newRoot("token");
    String sortableFieldName = TypeField.TOKEN.getLuceneFieldName(fieldPath, Optional.empty());
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(new MongotSortField(fieldPath, UserFieldSortOptions.DEFAULT_ASC))
            .buildSort();

    var doc1 = createSortableDoc(sortableFieldName, "3", "a");
    var doc2 = createSortableDoc(sortableFieldName, "2", "a");
    var doc3 = createSortableDoc(sortableFieldName, "1", "b");
    this.indexWriter.addDocument(config.build(doc1));
    this.indexWriter.addDocument(config.build(doc2));
    this.indexWriter.addDocument(config.build(doc3));
    this.indexWriter.commit();

    var searcherManager =
        new LuceneSearcherManager(
            this.indexWriter,
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

    Optional<SequenceToken> searchAfter = Optional.empty();
    Sort sort =
        new LuceneSortFactory(this.queryFactoryContext)
            .createLuceneSort(
                sortSpec,
                searchAfter,
                searcherReference.getIndexSearcher().getFieldToSortableTypesMapping(),
                Optional.empty(),
                Optional.empty());
    var firstPageManager =
        new LuceneFacetCollectorSearchManager(
            new MatchAllDocsQuery(), Optional.of(sort), searchAfter);
    var firstPageQueryInfo = firstPageManager.initialSearch(searcherReference, 2);
    var initialScoreDocs = firstPageQueryInfo.topDocs.scoreDocs;
    {
      var readerState = searcherReference.getIndexSearcher().getFacetsState().get();
      var counts = new SortedSetDocValuesFacetCounts(readerState, firstPageQueryInfo.collector);
      assertEquals(2, counts.getSpecificValue("facetField", "a").intValue());
      assertEquals(1, counts.getSpecificValue("facetField", "b").intValue());
      TestUtils.assertHasValidScores(initialScoreDocs);
      TestUtils.assertHasDocIds(initialScoreDocs, 2, 1);
    }

    // Get second page should keep same metadata but return next documents
    var nextPageToken =
        Optional.of(
            SequenceToken.of(
                new BsonString("test"), initialScoreDocs[initialScoreDocs.length - 1]));

    var nextSort =
        new LuceneSortFactory(this.queryFactoryContext)
            .createLuceneSort(
                sortSpec,
                nextPageToken,
                searcherReference.getIndexSearcher().getFieldToSortableTypesMapping(),
                Optional.empty(),
                Optional.empty());
    var secondPageManager =
        new LuceneFacetCollectorSearchManager(
            new MatchAllDocsQuery(), Optional.of(nextSort), nextPageToken);
    var secondPageQueryInfo = secondPageManager.initialSearch(searcherReference, 2);
    var secondPageScoreDocs = secondPageQueryInfo.topDocs.scoreDocs;
    var secondPageFacts = searcherReference.getIndexSearcher().getFacetsState().get();
    var secondPageCounts =
        new SortedSetDocValuesFacetCounts(secondPageFacts, secondPageQueryInfo.collector);

    assertEquals(2, secondPageCounts.getSpecificValue("facetField", "a").intValue());
    assertEquals(1, secondPageCounts.getSpecificValue("facetField", "b").intValue());
    TestUtils.assertHasValidScores(secondPageScoreDocs);
    TestUtils.assertHasDocIds(secondPageScoreDocs, 0);
  }

  @Test
  public void testFacetSortWithSearchAfterLargeDocId() throws IOException {
    var config = new FacetsConfig();
    FieldPath fieldPath = FieldPath.newRoot("token");
    String sortableFieldName = TypeField.TOKEN.getLuceneFieldName(fieldPath, Optional.empty());
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(new MongotSortField(fieldPath, UserFieldSortOptions.DEFAULT_ASC))
            .buildSort();

    var doc0 = createSortableDoc(sortableFieldName, "3", "a");
    var doc1 = createSortableDoc(sortableFieldName, "2", "a");
    var doc2 = createSortableDoc(sortableFieldName, "1", "b");
    this.indexWriter.addDocument(config.build(doc0));
    this.indexWriter.addDocument(config.build(doc1));
    this.indexWriter.addDocument(config.build(doc2));
    this.indexWriter.commit();

    var searcherManager =
        new LuceneSearcherManager(
            this.indexWriter,
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

    var searchAfter =
        Optional.of(
            SequenceToken.of(
                new BsonString("test"),
                new FieldDoc(9001, Float.MAX_VALUE, new Object[] {BsonUtils.MIN_KEY})));
    Sort sort =
        new LuceneSortFactory(this.queryFactoryContext)
            .createLuceneSort(
                sortSpec,
                searchAfter,
                searcherReference.getIndexSearcher().getFieldToSortableTypesMapping(),
                Optional.empty(),
                Optional.empty());
    var firstPageManager =
        new LuceneFacetCollectorSearchManager(
            new MatchAllDocsQuery(), Optional.of(sort), searchAfter);
    var firstPageQueryInfo = firstPageManager.initialSearch(searcherReference, 2);
    var initialScoreDocs = firstPageQueryInfo.topDocs.scoreDocs;

    var readerState = searcherReference.getIndexSearcher().getFacetsState().get();
    var counts = new SortedSetDocValuesFacetCounts(readerState, firstPageQueryInfo.collector);
    assertEquals(2, counts.getSpecificValue("facetField", "a").intValue());
    assertEquals(1, counts.getSpecificValue("facetField", "b").intValue());
    TestUtils.assertHasValidScores(initialScoreDocs);
    TestUtils.assertHasDocIds(initialScoreDocs, 2, 1);
  }

  @Test
  public void testFacetSearchAfter() throws IOException {
    var config = new FacetsConfig();
    var doc1 = createSimpleDoc("a");
    var doc2 = createSimpleDoc("a");
    var doc3 = createSimpleDoc("b");
    this.indexWriter.addDocument(config.build(doc1));
    this.indexWriter.addDocument(config.build(doc2));
    this.indexWriter.addDocument(config.build(doc3));
    this.indexWriter.commit();

    var searcherManager =
        new LuceneSearcherManager(
            this.indexWriter,
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

    var firstPageManager =
        new LuceneFacetCollectorSearchManager(
            new MatchAllDocsQuery(), Optional.empty(), Optional.empty());
    var firstPageQueryInfo = firstPageManager.initialSearch(searcherReference, 2);
    var initialScoreDocs = firstPageQueryInfo.topDocs.scoreDocs;
    {
      var readerState = searcherReference.getIndexSearcher().getFacetsState().get();
      var counts = new SortedSetDocValuesFacetCounts(readerState, firstPageQueryInfo.collector);
      assertEquals(2, counts.getSpecificValue("facetField", "a").intValue());
      assertEquals(1, counts.getSpecificValue("facetField", "b").intValue());
      TestUtils.assertHasValidScores(initialScoreDocs);
      TestUtils.assertHasDocIds(initialScoreDocs, 0, 1);
    }

    // Get second page should keep same metadata but return next documents
    var nextPageToken =
        SequenceToken.of(new BsonString("test"), initialScoreDocs[initialScoreDocs.length - 1]);
    var secondPageManager =
        new LuceneFacetCollectorSearchManager(
            new MatchAllDocsQuery(), Optional.empty(), Optional.of(nextPageToken));
    var secondPageQueryInfo = secondPageManager.initialSearch(searcherReference, 2);
    var secondPageScoreDocs = secondPageQueryInfo.topDocs.scoreDocs;
    var secondPageFacts = searcherReference.getIndexSearcher().getFacetsState().get();
    var secondPageCounts =
        new SortedSetDocValuesFacetCounts(secondPageFacts, secondPageQueryInfo.collector);

    assertEquals(2, secondPageCounts.getSpecificValue("facetField", "a").intValue());
    assertEquals(1, secondPageCounts.getSpecificValue("facetField", "b").intValue());
    TestUtils.assertHasValidScores(secondPageScoreDocs);
    TestUtils.assertHasDocIds(secondPageScoreDocs, 2);
  }

  @Test
  public void testFacetSearchAfterWithLargeDocId() throws IOException {
    var config = new FacetsConfig();
    var doc1 = createSimpleDoc("a");
    var doc2 = createSimpleDoc("a");
    var doc3 = createSimpleDoc("b");
    this.indexWriter.addDocument(config.build(doc1));
    this.indexWriter.addDocument(config.build(doc2));
    this.indexWriter.addDocument(config.build(doc3));
    this.indexWriter.commit();

    var searcherManager =
        new LuceneSearcherManager(
            this.indexWriter,
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

    SequenceToken searchAfter =
        SequenceToken.of(new BsonString("test"), new ScoreDoc(9001, Float.MAX_VALUE));
    var firstPageManager =
        new LuceneFacetCollectorSearchManager(
            new MatchAllDocsQuery(), Optional.empty(), Optional.of(searchAfter));
    var firstPageQueryInfo = firstPageManager.initialSearch(searcherReference, 2);

    var readerState = searcherReference.getIndexSearcher().getFacetsState().get();
    var counts = new SortedSetDocValuesFacetCounts(readerState, firstPageQueryInfo.collector);

    var initialScoreDocs = firstPageQueryInfo.topDocs.scoreDocs;
    assertEquals(2, counts.getSpecificValue("facetField", "a").intValue());
    assertEquals(1, counts.getSpecificValue("facetField", "b").intValue());
    TestUtils.assertHasValidScores(initialScoreDocs);
    TestUtils.assertHasDocIds(initialScoreDocs, 0, 1);
  }

  @Test
  public void testFacetSearchWithSort() throws IOException {
    FieldPath fieldPath = FieldPath.newRoot("field");
    String sortableFieldName =
        TypeField.SORTABLE_STRING_BETA_V1.getLuceneFieldName(fieldPath, Optional.empty());

    var config = new FacetsConfig();

    var doc1 = createSortableDoc(sortableFieldName, "4", "a");
    var doc2 = createSortableDoc(sortableFieldName, "3", "a");
    var doc3 = createSortableDoc(sortableFieldName, "2", "a");
    var doc4 = createSortableDoc(sortableFieldName, "1", "b");
    this.indexWriter.addDocument(config.build(doc1));
    this.indexWriter.addDocument(config.build(doc2));
    this.indexWriter.addDocument(config.build(doc3));
    this.indexWriter.addDocument(config.build(doc4));
    this.indexWriter.commit();

    var searcherManager =
        new LuceneSearcherManager(
            this.indexWriter,
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

    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(new MongotSortField(fieldPath, UserFieldSortOptions.DEFAULT_ASC))
            .buildSortBetaV1();

    Sort sort =
        new LuceneSortFactory(this.queryFactoryContext)
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                searcherReference.getIndexSearcher().getFieldToSortableTypesMapping(),
                Optional.empty(),
                Optional.empty());
    var manager =
        new LuceneFacetCollectorSearchManager(
            new MatchAllDocsQuery(), Optional.of(sort), Optional.empty());

    var queryInfo = manager.initialSearch(searcherReference, 2);

    var readerState = searcherReference.getIndexSearcher().getFacetsState().get();
    var counts = new SortedSetDocValuesFacetCounts(readerState, queryInfo.collector);

    var initialScoreDocs = queryInfo.topDocs.scoreDocs;
    assertEquals(3, counts.getSpecificValue("facetField", "a").intValue());
    assertEquals(1, counts.getSpecificValue("facetField", "b").intValue());
    TestUtils.assertHasValidScores(initialScoreDocs);
    TestUtils.assertHasDocIds(initialScoreDocs, 3, 2);

    var getMoreDocs =
        manager.getMoreTopDocs(searcherReference, initialScoreDocs[initialScoreDocs.length - 1], 2);
    TestUtils.assertHasValidScores(getMoreDocs.scoreDocs);
    TestUtils.assertHasDocIds(getMoreDocs.scoreDocs, 1, 0);
  }

  @Test
  public void testFacetExplain() throws IOException {
    FieldPath fieldPath = FieldPath.newRoot("field");
    String sortableFieldName = TypeField.TOKEN.getLuceneFieldName(fieldPath, Optional.empty());

    var config = new FacetsConfig();

    var doc1 = createSortableDoc(sortableFieldName, "4", "a");
    var doc2 = createSortableDoc(sortableFieldName, "3", "a");
    var doc3 = createSortableDoc(sortableFieldName, "2", "a");
    var doc4 = createSortableDoc(sortableFieldName, "1", "b");
    this.indexWriter.addDocument(config.build(doc1));
    this.indexWriter.addDocument(config.build(doc2));
    this.indexWriter.addDocument(config.build(doc3));
    this.indexWriter.addDocument(config.build(doc4));
    this.indexWriter.commit();

    var searcherManager =
        new LuceneSearcherManager(
            this.indexWriter,
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

    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(new MongotSortField(fieldPath, UserFieldSortOptions.DEFAULT_ASC))
            .buildSort();

    Sort sort =
        new LuceneSortFactory(this.queryFactoryContext)
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                searcherReference.getIndexSearcher().getFieldToSortableTypesMapping(),
                Optional.empty(),
                Optional.empty());
    var manager =
        new LuceneFacetCollectorSearchManager(
            new MatchAllDocsQuery(), Optional.of(sort), Optional.empty());

    try (var unused =
        Explain.setup(
            Optional.of(Explain.Verbosity.EXECUTION_STATS),
            Optional.of(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()))) {

      Explain.getQueryInfo()
          .get()
          .getFeatureExplainer(
              SortFeatureExplainer.class,
              () ->
                  new SortFeatureExplainer(
                      sortSpec,
                      searcherReference
                          .getIndexSearcher()
                          .getFieldToSortableTypesMapping()
                          .rootFieldsToSortableTypes()));

      manager.initialSearch(searcherReference, 2);

      var result = Explain.collect().get();
      Assert.assertTrue(
          result.collectStats().flatMap(CollectorExplainInformation::facetStats).isPresent());
      Assert.assertTrue(
          result.collectStats().flatMap(CollectorExplainInformation::sortStats).isPresent());
    }
  }
}
