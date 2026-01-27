package com.xgen.mongot.index.lucene;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.codec.LuceneCodec;
import com.xgen.mongot.index.lucene.field.FieldName;
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
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.lucene.synonym.SynonymRegistryBuilder;
import com.xgen.testing.mongot.index.query.counts.CountBuilder;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.bson.BsonString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLuceneOperatorSearchManager {

  private IndexWriter indexWriter;
  private Directory directory;
  private SearchQueryFactoryContext queryFactoryContext;

  @Before
  public void before() throws Exception {
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

  private LuceneIndexSearcherReference getSearcherReference() throws IOException {
    var searcherManager =
        new LuceneSearcherManager(
            this.indexWriter,
            new LuceneSearcherFactory(
                SearchIndex.MOCK_INDEX_DEFINITION,
                false,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH)),
            SearchIndex.mockMetricsFactory());
    return LuceneIndexSearcherReference.create(
        searcherManager,
        SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH),
        FeatureFlags.getDefault());
  }

  /** Create a document that has a sortable string field with the given name and value. */
  private static Document createSimpleDoc(String luceneFieldName, String value) {
    Document doc = new Document();
    var docTerm = new Term(luceneFieldName, value).bytes();
    doc.add(new SortedSetDocValuesField(luceneFieldName, docTerm));
    doc.add(new StringField(luceneFieldName, docTerm, Field.Store.NO));
    return doc;
  }

  @Test
  public void testSortScoresDocuments() throws IOException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                new MongotSortField(FieldPath.newRoot("field"), UserFieldSortOptions.DEFAULT_ASC))
            .buildSortBetaV1();

    String luceneFieldName =
        FieldName.TypeField.SORTABLE_STRING_BETA_V1.getLuceneFieldName(
            FieldPath.newRoot("field"), Optional.empty());

    // insert 4 documents into Lucene
    Document doc0 = createSimpleDoc(luceneFieldName, "D");
    Document doc1 = createSimpleDoc(luceneFieldName, "C");
    Document doc2 = createSimpleDoc(luceneFieldName, "B");
    Document doc3 = createSimpleDoc(luceneFieldName, "A");
    this.indexWriter.addDocument(doc0);
    this.indexWriter.addDocument(doc1);
    this.indexWriter.addDocument(doc2);
    this.indexWriter.addDocument(doc3);
    this.indexWriter.commit();
    var searcherReference = getSearcherReference();

    Optional<SequenceToken> searchAfter = Optional.empty();
    var sort =
        new LuceneSortFactory(this.queryFactoryContext)
            .createLuceneSort(
                sortSpec,
                searchAfter,
                searcherReference.getIndexSearcher().getFieldToSortableTypesMapping(),
                Optional.empty(),
                Optional.empty());
    LuceneOperatorSearchManager manager =
        new LuceneOperatorSearchManager(
            new MatchAllDocsQuery(),
            CountBuilder.builder().build(),
            Optional.of(sort),
            searchAfter);

    // run initial search with batchSize=2
    var firstPageInfo = manager.initialSearch(searcherReference, 2);
    var initialScoreDocs = firstPageInfo.topDocs.scoreDocs;
    TestUtils.assertHasDocIds(initialScoreDocs, 3, 2);
    TestUtils.assertHasValidScores(initialScoreDocs);

    // get 2 more documents
    var getMoreDocs =
        manager.getMoreTopDocs(searcherReference, initialScoreDocs[initialScoreDocs.length - 1], 2)
            .scoreDocs;
    TestUtils.assertHasDocIds(getMoreDocs, 1, 0);

    // check all docs have scores
    TestUtils.assertHasValidScores(initialScoreDocs);
  }

  @Test
  public void testSearchAfterWithSort() throws IOException {
    FieldPath fieldPath = FieldPath.newRoot("field");
    String luceneFieldName = TypeField.TOKEN.getLuceneFieldName(fieldPath, Optional.empty());
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(new MongotSortField(fieldPath, UserFieldSortOptions.DEFAULT_ASC))
            .buildSort();

    // insert 4 documents into Lucene
    Document doc0 = createSimpleDoc(luceneFieldName, "D");
    Document doc1 = createSimpleDoc(luceneFieldName, "C");
    Document doc2 = createSimpleDoc(luceneFieldName, "B");
    Document doc3 = createSimpleDoc(luceneFieldName, "A");
    this.indexWriter.addDocument(doc0);
    this.indexWriter.addDocument(doc1);
    this.indexWriter.addDocument(doc2);
    this.indexWriter.addDocument(doc3);
    this.indexWriter.commit();
    var searcherReference = getSearcherReference();

    Optional<SequenceToken> searchAfter = Optional.empty();
    var sort =
        new LuceneSortFactory(this.queryFactoryContext)
            .createLuceneSort(
                sortSpec,
                searchAfter,
                searcherReference.getIndexSearcher().getFieldToSortableTypesMapping(),
                Optional.empty(),
                Optional.empty());

    // Get first page of size 2. Expected docId result order: [3, 2, 1, 0]
    LuceneOperatorSearchManager firstPageManager =
        new LuceneOperatorSearchManager(
            new MatchAllDocsQuery(),
            CountBuilder.builder().build(),
            Optional.of(sort),
            searchAfter);
    var firstPageInfo = firstPageManager.initialSearch(searcherReference, 2);
    var firstPageDocs = firstPageInfo.topDocs.scoreDocs;
    TestUtils.assertHasDocIds(firstPageDocs, 3, 2);
    TestUtils.assertHasValidScores(firstPageDocs);

    // Get second page of size 2
    ScoreDoc lastDoc = firstPageDocs[firstPageDocs.length - 1];
    Optional<SequenceToken> lastToken =
        Optional.of(SequenceToken.of(new BsonString("test"), lastDoc));
    var nextSort =
        new LuceneSortFactory(this.queryFactoryContext)
            .createLuceneSort(
                sortSpec,
                lastToken,
                searcherReference.getIndexSearcher().getFieldToSortableTypesMapping(),
                Optional.empty(),
                Optional.empty());
    LuceneOperatorSearchManager nextPageManager =
        new LuceneOperatorSearchManager(
            new MatchAllDocsQuery(),
            CountBuilder.builder().build(),
            Optional.of(nextSort),
            lastToken);
    // get 2 more documents
    var nextPageDocs = nextPageManager.initialSearch(searcherReference, 2).topDocs.scoreDocs;
    TestUtils.assertHasDocIds(nextPageDocs, 1, 0);
    TestUtils.assertHasValidScores(nextPageDocs);
  }

  @Test
  public void testSearchAfterWithoutSort() throws IOException {
    String luceneFieldName =
        TypeField.TOKEN.getLuceneFieldName(FieldPath.newRoot("field"), Optional.empty());
    // insert 4 documents into Lucene
    Document doc0 = createSimpleDoc(luceneFieldName, "D");
    Document doc1 = createSimpleDoc(luceneFieldName, "C");
    Document doc2 = createSimpleDoc(luceneFieldName, "B");
    Document doc3 = createSimpleDoc(luceneFieldName, "A");
    this.indexWriter.addDocument(doc0);
    this.indexWriter.addDocument(doc1);
    this.indexWriter.addDocument(doc2);
    this.indexWriter.addDocument(doc3);
    this.indexWriter.commit();
    var searcherReference = getSearcherReference();

    // Get first page of size 2. Results are sorted by docId: [0, 1, 2, 3]
    LuceneOperatorSearchManager firstPageManager =
        new LuceneOperatorSearchManager(
            new MatchAllDocsQuery(),
            CountBuilder.builder().build(),
            Optional.empty(),
            Optional.empty());
    var firstPageInfo = firstPageManager.initialSearch(searcherReference, 2);
    var firstPageDocs = firstPageInfo.topDocs.scoreDocs;
    TestUtils.assertHasDocIds(firstPageDocs, 0, 1);
    TestUtils.assertHasValidScores(firstPageDocs);

    // Get second page of size 2
    ScoreDoc lastDoc = firstPageDocs[firstPageDocs.length - 1];
    SequenceToken lastToken = SequenceToken.of(new BsonString("test"), lastDoc);
    LuceneOperatorSearchManager nextPageManager =
        new LuceneOperatorSearchManager(
            new MatchAllDocsQuery(),
            CountBuilder.builder().build(),
            Optional.empty(),
            Optional.of(lastToken));
    // get 2 more documents
    var nextPageDocs = nextPageManager.initialSearch(searcherReference, 2).topDocs.scoreDocs;
    TestUtils.assertHasDocIds(nextPageDocs, 2, 3);
    TestUtils.assertHasValidScores(nextPageDocs);
  }
}
