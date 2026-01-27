package com.xgen.mongot.index.lucene.query;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.lucene.query.util.BooleanComposer.filterClause;
import static com.xgen.mongot.index.lucene.query.util.BooleanComposer.shouldClause;
import static com.xgen.mongot.index.query.QueryOptimizationFlags.DEFAULT_OPTIONS;

import com.google.common.base.Ascii;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.custom.WrappedQuery;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.lucene.query.util.SafeTermAutomatonQueryWrapper;
import com.xgen.mongot.index.lucene.query.util.WrappedToParentBlockJoinQuery;
import com.xgen.mongot.index.lucene.searcher.FieldToSortableTypesMapping;
import com.xgen.mongot.index.lucene.synonym.LuceneSynonymRegistry;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.InOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.QueryStringOperator;
import com.xgen.mongot.index.query.operators.SearchOperator;
import com.xgen.mongot.index.query.operators.TermOperator;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.index.synonym.SynonymMapping;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SynonymMappingDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.synonym.SynonymRegistryBuilder;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import com.xgen.testing.mongot.index.query.QueryOptimizationFlagsBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.TermOperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import com.xgen.testing.mongot.index.query.sort.SortFieldBuilder;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import com.xgen.testing.mongot.index.synonym.SynonymDocumentBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.sandbox.search.TokenStreamToTermAutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.bson.types.ObjectId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LuceneSearchQueryFactoryDistributorTest {

  private static Directory directory;
  private static IndexWriter writer;

  /** set up an index. */
  @BeforeClass
  public static void setUp() throws IOException {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    writer = new IndexWriter(directory, new IndexWriterConfig());
    writer.commit();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    writer.close();
    directory.close();
  }

  private AnalyzerRegistry getAnalyzerRegistry() throws InvalidAnalyzerDefinitionException {
    var analyzers = new ArrayList<AnalyzerDefinition>();
    for (String name :
        Arrays.asList("search", "index", "fieldAnalyzer", "fieldSearchAnalyzer", "multiAnalyzer")) {
      analyzers.add(
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
              .name(name)
              .build());
    }
    return AnalyzerRegistry.factory().create(analyzers, true);
  }

  private SearchIndexDefinitionBuilder baseIndexDefinitionBuilder() {
    var fieldWithAnalyzer =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .string(StringFieldDefinitionBuilder.builder().analyzerName("fieldAnalyzer").build())
            .build();

    var fieldWithSearchAnalyzer =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .string(
                StringFieldDefinitionBuilder.builder()
                    .analyzerName("fieldAnalyzer")
                    .searchAnalyzerName("fieldSearchAnalyzer")
                    .build())
            .build();

    var fieldWithMultiAnalyzer =
        FieldDefinitionBuilder.builder()
            .document(DocumentFieldDefinitionBuilder.builder().build())
            .string(
                StringFieldDefinitionBuilder.builder()
                    .analyzerName("fieldAnalyzer")
                    .multi(
                        "multi",
                        StringFieldDefinitionBuilder.builder()
                            .analyzerName("multiAnalyzer")
                            .build())
                    .build())
            .build();

    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field("fieldWithAnalyzer", fieldWithAnalyzer)
            .field("fieldWithSearchAnalyzer", fieldWithSearchAnalyzer)
            .field("fieldWithMultiAnalyzer", fieldWithMultiAnalyzer)
            .build();

    return SearchIndexDefinitionBuilder.builder()
        .defaultMetadata()
        .mappings(mappings)
        .analyzerName("index")
        .searchAnalyzerName("search");
  }

  private SearchIndexDefinition getIndexDefinition(List<SynonymMappingDefinition> synonyms) {
    return baseIndexDefinitionBuilder().synonyms(synonyms).build();
  }

  @Test
  public void testCompoundDefinition() throws Exception {
    CompoundOperator definition =
        OperatorBuilder.compound()
            .should(OperatorBuilder.term().path("title").query("godfather").build())
            .build();

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(shouldClause(new TermQuery(new Term("$type:string/title", "godfather"))));
    Query expected = builder.build();
    LuceneSearchTranslation.get().assertTranslatedTo(definition, expected);
  }

  @Test
  public void testCompoundDefinitionShouldMinimumMatch() throws InvalidQueryException, IOException {
    CompoundOperator definition =
        OperatorBuilder.compound()
            .should(OperatorBuilder.term().path("title").query("godfather").build())
            .should(OperatorBuilder.term().path("title").query("nemo").build())
            .minimumShouldMatch(1)
            .build();

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.setMinimumNumberShouldMatch(1);
    builder.add(shouldClause(new TermQuery(new Term("$type:string/title", "godfather"))));
    builder.add(shouldClause(new TermQuery(new Term("$type:string/title", "nemo"))));
    Query expected = builder.build();
    LuceneSearchTranslation.get().assertTranslatedTo(definition, expected);
  }

  @Test
  public void testCompoundDefinitionMustNot() throws Exception {
    CompoundOperator definition =
        OperatorBuilder.compound()
            .mustNot(OperatorBuilder.term().path("title").query("godfather").build())
            .build();

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER);
    builder.add(
        BooleanComposer.mustNotClause(new TermQuery(new Term("$type:string/title", "godfather"))));
    Query expected = builder.build();
    LuceneSearchTranslation.get().assertTranslatedTo(definition, expected);
  }

  @Test
  public void testCompoundMustNotNonEmbeddedQueryToRootDocs() throws Exception {
    CompoundOperator operator =
        OperatorBuilder.compound()
            .mustNot(OperatorBuilder.term().path("title").query("godfather").build())
            .build();

    BooleanQuery.Builder builder = new BooleanQuery.Builder();

    builder.add(new TermQuery(new Term("$meta/embeddedRoot", "T")), BooleanClause.Occur.FILTER);
    builder.add(
        BooleanComposer.mustNotClause(new TermQuery(new Term("$type:string/title", "godfather"))));
    Query expected = builder.build();

    LuceneSearchTranslation.mapped(
            DocumentFieldDefinitionBuilder.builder()
                .field(
                    "foo",
                    FieldDefinitionBuilder.builder()
                        .embeddedDocuments(
                            EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build())
                        .build())
                .build())
        .assertTranslatedTo(operator, expected);
  }

  @Test
  public void testCompoundMustNotEmbeddedQueryToRootDocs() throws Exception {
    CompoundOperator operator =
        OperatorBuilder.compound()
            .mustNot(
                OperatorBuilder.embeddedDocument()
                    .path("foo")
                    .operator(OperatorBuilder.term().path("foo.title").query("godfather").build())
                    .build())
            .build();

    BooleanQuery.Builder builder = new BooleanQuery.Builder();

    builder.add(new TermQuery(new Term("$meta/embeddedRoot", "T")), BooleanClause.Occur.FILTER);
    builder.add(
        BooleanComposer.mustNotClause(
            new WrappedToParentBlockJoinQuery(
                new TermQuery(new Term("$embedded:3/foo/$type:string/foo.title", "godfather")),
                new QueryBitSetProducer(new TermQuery(new Term("$meta/embeddedRoot", "T"))),
                ScoreMode.Total)));

    Query expected = builder.build();

    LuceneSearchTranslation.mapped(
            DocumentFieldDefinitionBuilder.builder()
                .field(
                    "foo",
                    FieldDefinitionBuilder.builder()
                        .embeddedDocuments(
                            EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build())
                        .build())
                .build())
        .assertTranslatedTo(operator, expected);
  }

  @Test
  public void testCompoundDefinitionMustNotWithMust() throws Exception {
    CompoundOperator definition =
        OperatorBuilder.compound()
            .must(OperatorBuilder.term().path("title").query("nemo").build())
            .mustNot(OperatorBuilder.term().path("title").query("godfather").build())
            .build();

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(BooleanComposer.mustClause(new TermQuery(new Term("$type:string/title", "nemo"))));
    builder.add(
        BooleanComposer.mustNotClause(new TermQuery(new Term("$type:string/title", "godfather"))));
    Query expected = builder.build();
    LuceneSearchTranslation.get().assertTranslatedTo(definition, expected);
  }

  @Test
  public void testCompoundDefinitionMultiple() throws Exception {
    CompoundOperator definition =
        OperatorBuilder.compound()
            .should(OperatorBuilder.term().path("title").query("star wars").build())
            .must(OperatorBuilder.term().path("title").query("godfather").build())
            .mustNot(OperatorBuilder.term().path("director").query("james cameroon").build())
            .filter(OperatorBuilder.term().path("director").query("steven spielberg").build())
            .build();

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(filterClause(new TermQuery(new Term("$type:string/director", "steven spielberg"))));
    builder.add(
        BooleanComposer.mustClause(new TermQuery(new Term("$type:string/title", "godfather"))));
    builder.add(
        BooleanComposer.mustNotClause(
            new TermQuery(new Term("$type:string/director", "james cameroon"))));
    builder.add(shouldClause(new TermQuery(new Term("$type:string/title", "star wars"))));

    Query expected = builder.build();
    LuceneSearchTranslation.get().assertTranslatedTo(definition, expected);
  }

  @Test
  public void testCompoundDefinitionScoreConstant() throws InvalidQueryException, IOException {
    CompoundOperator definition =
        OperatorBuilder.compound()
            .must(OperatorBuilder.term().path("title").query("godfather").build())
            .score(ScoreBuilder.constant().value(2).build())
            .build();

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    Query expected =
        new BoostQuery(
            new ConstantScoreQuery(
                builder
                    .add(
                        BooleanComposer.mustClause(
                            new TermQuery(new Term("$type:string/title", "godfather"))))
                    .build()),
            2);

    LuceneSearchTranslation.get().assertTranslatedTo(definition, expected);
  }

  @Test
  public void testCompoundDefinitionScoreBoost() throws InvalidQueryException, IOException {
    CompoundOperator definition =
        OperatorBuilder.compound()
            .must(OperatorBuilder.term().path("title").query("godfather").build())
            .score(ScoreBuilder.valueBoost().value(2).build())
            .build();

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    Query expected =
        new BoostQuery(
            builder
                .add(
                    BooleanComposer.mustClause(
                        new TermQuery(new Term("$type:string/title", "godfather"))))
                .build(),
            2);

    LuceneSearchTranslation.get().assertTranslatedTo(definition, expected);
  }

  @Test
  public void testCompoundDefinitionScoreDismax() throws InvalidQueryException, IOException {
    Operator definition =
        OperatorBuilder.compound()
            .should(OperatorBuilder.term().path("title").query("godfather").build())
            .should(OperatorBuilder.term().path("title").query("nemo").build())
            .score(ScoreBuilder.dismax().tieBreakerScore(0.2f).build())
            .build();

    List<Query> shouldQueries =
        Arrays.asList(
            new TermQuery(new Term("$type:string/title", "godfather")),
            new TermQuery(new Term("$type:string/title", "nemo")));

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(shouldClause(new DisjunctionMaxQuery(shouldQueries, 0.2f)));
    Query expected = builder.build();

    LuceneSearchTranslation.get().assertTranslatedTo(definition, expected);
  }

  @Test
  public void testCompoundDefinitionDismaxSubclauseScores()
      throws InvalidQueryException, IOException {
    CompoundOperator definition =
        OperatorBuilder.compound()
            .should(
                OperatorBuilder.term()
                    .path("title")
                    .query("john")
                    .score(ScoreBuilder.valueBoost().value(2).build())
                    .build())
            .should(
                OperatorBuilder.term()
                    .path("title")
                    .query("wick")
                    .score(ScoreBuilder.valueBoost().value(4).build())
                    .build())
            .score(ScoreBuilder.dismax().tieBreakerScore(0.2f).build())
            .build();

    List<Query> shouldQueries =
        Arrays.asList(
            new BoostQuery(new TermQuery(new Term("$type:string/title", "john")), 2),
            new BoostQuery(new TermQuery(new Term("$type:string/title", "wick")), 4));

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    Query expected =
        builder.add(shouldClause(new DisjunctionMaxQuery(shouldQueries, 0.2f))).build();

    LuceneSearchTranslation.get().assertTranslatedTo(definition, expected);
  }

  @Test
  public void testTermDefinition() throws Exception {
    TermOperator definition = OperatorBuilder.term().path("title").query("godfather").build();

    Query expected = new TermQuery(new Term("$type:string/title", "godfather"));
    LuceneSearchTranslation.get().assertTranslatedTo(definition, expected);
  }

  @Test
  public void testTermQueryIsNormalized() throws Exception {
    var definition = OperatorBuilder.term().path("title").query("God FaThers").build();

    Query expected = new TermQuery(new Term("$type:string/title", "god fathers"));
    LuceneSearchTranslation.analyzer("lucene.english").assertTranslatedTo(definition, expected);
  }

  @Test
  public void testTermWildcardQueryIsNormalized() throws Exception {
    var definition =
        OperatorBuilder.term().wildcard(true).path("title").query("God FaThers").build();

    Query expected = new WildcardQuery(new Term("$type:string/title", "god fathers"));
    LuceneSearchTranslation.analyzer("lucene.english").assertTranslatedTo(definition, expected);
  }

  @Test
  public void testTermRegexQueryIsNormalized() throws Exception {
    var definition = OperatorBuilder.term().regex(true).path("title").query("God FaThers").build();

    Query expected = new RegexpQuery(new Term("$type:string/title", "god fathers"));
    LuceneSearchTranslation.analyzer("lucene.english").assertTranslatedTo(definition, expected);
  }

  @Test
  public void testTermPrefixQueryIsNormalized() throws Exception {
    var definition = OperatorBuilder.term().prefix(true).path("title").query("God FaThers").build();

    Query expected = new PrefixQuery(new Term("$type:string/title", "god fathers"));
    LuceneSearchTranslation.analyzer("lucene.english").assertTranslatedTo(definition, expected);
  }

  @Test
  public void testTermFuzzyQueryIsNormalized() throws Exception {
    var definition =
        OperatorBuilder.term()
            .fuzzy(
                TermOperatorBuilder.fuzzyBuilder()
                    .maxEdits(1)
                    .maxExpansions(1)
                    .prefixLength(1)
                    .build())
            .path("title")
            .query("God FaThers")
            .build();

    Query expected = new FuzzyQuery(new Term("$type:string/title", "god fathers"), 1, 1, 1, true);
    LuceneSearchTranslation.analyzer("lucene.english").assertTranslatedTo(definition, expected);
  }

  @Test
  public void testTextSynonymQuery() throws Exception {
    var operator =
        OperatorBuilder.text()
            .path("fieldWithAnalyzer")
            .query("fast")
            .synonyms("en")
            .matchCriteria(TextOperator.MatchCriteria.ANY)
            .build();

    List<SynonymMappingDefinition> synonyms =
        SynonymMappingDefinitionBuilder.builder()
            .name("en")
            .analyzer("fieldAnalyzer")
            .synonymSourceDefinition("my_synonyms")
            .buildAsList();

    SearchIndexDefinition indexDefinition = getIndexDefinition(synonyms);
    AnalyzerRegistry registry = getAnalyzerRegistry();
    SynonymRegistry synonymRegistry =
        LuceneSynonymRegistry.create(registry, indexDefinition.getSynonymMap(), Optional.empty());

    List<String> equivalentSynonyms = List.of("fast", "quick");

    SynonymMapping.Builder builder =
        synonymRegistry.mappingBuilder(indexDefinition.getSynonymMap().get("en"));
    builder.addDocument(SynonymDocumentBuilder.equivalent(equivalentSynonyms));
    synonymRegistry.update("en", builder.build());

    Query expected =
        BooleanComposer.should(
            new SynonymQuery.Builder("$type:string/fieldWithAnalyzer")
                .addTerm(new Term("$type:string/fieldWithAnalyzer", "fast"))
                // this duplication is done by Lucene QueryBuilder and is expected. See
                // SynonymQueryUtil for more information on how this query is built.
                .addTerm(new Term("$type:string/fieldWithAnalyzer", "fast"))
                .addTerm(new Term("$type:string/fieldWithAnalyzer", "quick"))
                .build(),
            new BoostQuery(new TermQuery(new Term("$type:string/fieldWithAnalyzer", "fast")), 2));

    LuceneSearchTranslation.synonyms(registry, indexDefinition, synonymRegistry)
        .assertTranslatedTo(operator, expected);
  }

  @Test
  public void testPhraseSlop0TextSynonymQuery() throws Exception {
    var operator =
        OperatorBuilder.text().path("fieldWithAnalyzer").query("fast").synonyms("en").build();

    List<SynonymMappingDefinition> synonyms =
        SynonymMappingDefinitionBuilder.builder()
            .name("en")
            .analyzer("fieldAnalyzer")
            .synonymSourceDefinition("my_synonyms")
            .buildAsList();

    SearchIndexDefinition indexDefinition = getIndexDefinition(synonyms);
    AnalyzerRegistry registry = getAnalyzerRegistry();
    SynonymRegistry synonymRegistry =
        LuceneSynonymRegistry.create(registry, indexDefinition.getSynonymMap(), Optional.empty());

    List<String> equivalentSynonyms = List.of("fast", "quick");

    SynonymMapping.Builder builder =
        synonymRegistry.mappingBuilder(indexDefinition.getSynonymMap().get("en"));
    builder.addDocument(SynonymDocumentBuilder.equivalent(equivalentSynonyms));
    synonymRegistry.update("en", builder.build());

    SearchQueryFactoryContext queryFactoryContext =
        new SearchQueryFactoryContext(
            registry,
            LuceneAnalyzer.queryAnalyzer(indexDefinition, registry),
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            synonymRegistry,
            new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
            FeatureFlags.getDefault());

    StringPathQuery spq =
        new StringPathQuery("fast", StringPathBuilder.fieldPath("fieldWithAnalyzer"));
    Analyzer analyzer =
        queryFactoryContext.getSynonymAnalyzer("en", spq.getPath(), Optional.empty());
    TokenStream queryGraph = analyzer.tokenStream("$type:string/fieldWithAnalyzer", spq.getQuery());
    TokenStreamToTermAutomatonQuery termAutomatonQuery = new TokenStreamToTermAutomatonQuery();
    Query expected =
        SafeTermAutomatonQueryWrapper.create(
            termAutomatonQuery.toQuery("$type:string/fieldWithAnalyzer", queryGraph));
    queryGraph.close();

    var actual =
        LuceneSearchTranslation.synonyms(registry, indexDefinition, synonymRegistry)
            .translate(operator);
    // NB: we are not directly comparing the Query objects here because TermAutomatonQuery.equals()
    // requires that the instance pointers be the same in Lucene 9.11. Logically these queries may
    // still be the same even if the instances are not identical.
    Assert.assertEquals(expected.getClass(), actual.getClass());
    Assert.assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testPhraseSynonymsQuery() throws Exception {
    var operator =
        OperatorBuilder.phrase().path("fieldWithAnalyzer").query("fast car").synonyms("en").build();

    List<SynonymMappingDefinition> synonyms =
        SynonymMappingDefinitionBuilder.builder()
            .name("en")
            .analyzer("fieldAnalyzer")
            .synonymSourceDefinition("my_synonyms")
            .buildAsList();

    SearchIndexDefinition indexDefinition = getIndexDefinition(synonyms);
    AnalyzerRegistry registry = getAnalyzerRegistry();
    SynonymRegistry synonymRegistry =
        LuceneSynonymRegistry.create(registry, indexDefinition.getSynonymMap(), Optional.empty());

    List<String> equivalentSynonyms = List.of("fast", "quick");

    SynonymMapping.Builder builder =
        synonymRegistry.mappingBuilder(indexDefinition.getSynonymMap().get("en"));
    builder.addDocument(SynonymDocumentBuilder.equivalent(equivalentSynonyms));
    synonymRegistry.update("en", builder.build());

    MultiPhraseQuery.Builder multiPhraseQueryBuilder = new MultiPhraseQuery.Builder();
    multiPhraseQueryBuilder.add(
        new Term[] {
          new Term("$type:string/fieldWithAnalyzer", "fast"),
          new Term("$type:string/fieldWithAnalyzer", "quick"),
          // this duplication is done by Lucene QueryBuilder and is expected. See SynonymQueryUtil
          // for more information on how this query is built.
          new Term("$type:string/fieldWithAnalyzer", "fast"),
        });
    multiPhraseQueryBuilder.add(new Term("$type:string/fieldWithAnalyzer", "car"));

    var expected =
        BooleanComposer.should(
            multiPhraseQueryBuilder.build(),
            new BoostQuery(new PhraseQuery("$type:string/fieldWithAnalyzer", "fast", "car"), 2));

    LuceneSearchTranslation.synonyms(registry, indexDefinition, synonymRegistry)
        .assertTranslatedTo(operator, expected);
  }

  @Test
  public void testTextSynonymQueryGeneratesExplicitSynonyms() throws Exception {
    List<SynonymMappingDefinition> synonyms =
        SynonymMappingDefinitionBuilder.builder()
            .name("en")
            .analyzer("fieldAnalyzer")
            .synonymSourceDefinition("my_synonyms")
            .buildAsList();

    SearchIndexDefinition indexDefinition = getIndexDefinition(synonyms);
    AnalyzerRegistry registry = getAnalyzerRegistry();
    SynonymRegistry synonymRegistry =
        LuceneSynonymRegistry.create(registry, indexDefinition.getSynonymMap(), Optional.empty());

    List<String> inputs = List.of("fast", "quick", "rapid");
    List<String> outputs = List.of("something", "else");

    SynonymMapping.Builder builder =
        synonymRegistry.mappingBuilder(indexDefinition.getSynonymMap().get("en"));
    builder.addDocument(SynonymDocumentBuilder.explicit(inputs, outputs));
    synonymRegistry.update("en", builder.build());

    StringPathQuery spq =
        new StringPathQuery("fast", StringPathBuilder.fieldPath("fieldWithAnalyzer"));

    SearchQueryFactoryContext queryFactoryContext =
        new SearchQueryFactoryContext(
            registry,
            LuceneAnalyzer.queryAnalyzer(indexDefinition, registry),
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            synonymRegistry,
            new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
            FeatureFlags.getDefault());
    Analyzer analyzer =
        queryFactoryContext.getSynonymAnalyzer("en", spq.getPath(), Optional.empty());

    TokenStream queryGraph = analyzer.tokenStream("$type:string/fieldWithAnalyzer", spq.getQuery());

    CharTermAttribute charTermAttribute = queryGraph.addAttribute(CharTermAttribute.class);
    queryGraph.reset();

    List<String> iteratedTokens = new ArrayList<>();
    while (queryGraph.incrementToken()) {
      String token = charTermAttribute.toString();
      Assert.assertTrue(
          String.format(
              "tokenStream should generate synonyms but generated token \"%s\" instead", token),
          inputs.contains(token) || outputs.contains(token));
      iteratedTokens.add(token);
    }
    queryGraph.close();

    Assert.assertEquals(iteratedTokens, List.of("something", "else", "fast"));
  }

  @Test
  public void testTextSynonymQueryGeneratesEquivalentSynonyms() throws Exception {
    List<SynonymMappingDefinition> synonyms =
        SynonymMappingDefinitionBuilder.builder()
            .name("en")
            .analyzer("fieldAnalyzer")
            .synonymSourceDefinition("my_synonyms")
            .buildAsList();

    SearchIndexDefinition indexDefinition = getIndexDefinition(synonyms);
    AnalyzerRegistry registry = getAnalyzerRegistry();
    SynonymRegistry synonymRegistry =
        LuceneSynonymRegistry.create(registry, indexDefinition.getSynonymMap(), Optional.empty());

    List<String> equivalentSynonyms = List.of("fast", "quick", "rapid");

    SynonymMapping.Builder builder =
        synonymRegistry.mappingBuilder(indexDefinition.getSynonymMap().get("en"));
    builder.addDocument(SynonymDocumentBuilder.equivalent(equivalentSynonyms));
    synonymRegistry.update("en", builder.build());

    StringPathQuery spq =
        new StringPathQuery("fast", StringPathBuilder.fieldPath("fieldWithAnalyzer"));

    SearchQueryFactoryContext queryFactoryContext =
        new SearchQueryFactoryContext(
            registry,
            LuceneAnalyzer.queryAnalyzer(indexDefinition, registry),
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            synonymRegistry,
            new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
            FeatureFlags.getDefault());
    Analyzer analyzer =
        queryFactoryContext.getSynonymAnalyzer("en", spq.getPath(), Optional.empty());

    TokenStream queryGraph = analyzer.tokenStream("$type:string/fieldWithAnalyzer", spq.getQuery());
    CharTermAttribute charTermAttribute = queryGraph.addAttribute(CharTermAttribute.class);
    queryGraph.reset();

    List<String> iteratedTokens = new ArrayList<>();
    while (queryGraph.incrementToken()) {
      String token = charTermAttribute.toString();
      Assert.assertTrue(
          String.format("tokenStream should generate synonyms but generated %s instead", token),
          equivalentSynonyms.contains(token));
      iteratedTokens.add(token);
    }
    queryGraph.close();

    Assert.assertEquals(iteratedTokens, List.of("fast", "quick", "rapid", "fast"));
  }

  @Test
  public void testTextSynonymQueryEnglishBaseAnalyzer() throws Exception {
    FieldDefinition fieldWithEnglishAnalyzer =
        FieldDefinitionBuilder.builder()
            .string(
                StringFieldDefinitionBuilder.builder()
                    .analyzerName(StockAnalyzerNames.LUCENE_ENGLISH.getName())
                    .build())
            .build();

    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field("fieldWithEnglishAnalyzer", fieldWithEnglishAnalyzer)
            .build();

    List<SynonymMappingDefinition> synonyms =
        SynonymMappingDefinitionBuilder.builder()
            .name("en")
            .analyzer(StockAnalyzerNames.LUCENE_ENGLISH.getName())
            .synonymSourceDefinition("my_synonyms")
            .buildAsList();

    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(mappings)
            .synonyms(synonyms)
            .build();
    AnalyzerRegistry registry = getAnalyzerRegistry();

    List<String> equivalentSynonyms = List.of("fast", "quick");

    SynonymRegistry synonymRegistry =
        LuceneSynonymRegistry.create(registry, indexDefinition.getSynonymMap(), Optional.empty());
    SynonymMapping.Builder builder =
        synonymRegistry.mappingBuilder(indexDefinition.getSynonymMap().get("en"));
    builder.addDocument(SynonymDocumentBuilder.equivalent(equivalentSynonyms));
    synonymRegistry.update("en", builder.build());

    SearchQueryFactoryContext queryFactoryContext =
        new SearchQueryFactoryContext(
            registry,
            LuceneAnalyzer.queryAnalyzer(indexDefinition, registry),
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            synonymRegistry,
            new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
            FeatureFlags.getDefault());

    StringPathQuery spq =
        new StringPathQuery("fasts", StringPathBuilder.fieldPath("fieldWithEnglishAnalyzer"));
    Analyzer analyzer =
        queryFactoryContext.getSynonymAnalyzer("en", spq.getPath(), Optional.empty());

    TokenStream queryGraph =
        analyzer.tokenStream("$type:string/fieldWithEnglishAnalyzer", spq.getQuery());
    CharTermAttribute charTermAttribute = queryGraph.addAttribute(CharTermAttribute.class);

    queryGraph.reset();
    while (queryGraph.incrementToken()) {
      String token = charTermAttribute.toString();
      Assert.assertTrue(
          String.format("tokenStream should generate synonyms but generated %s instead", token),
          equivalentSynonyms.contains(token));
    }
    queryGraph.close();
  }

  @Test
  public void testSynonymMapDuplicateEntries() throws Exception {
    List<SynonymMappingDefinition> synonyms =
        List.of(
            SynonymMappingDefinitionBuilder.builder()
                .name("normal")
                .analyzer("fieldAnalyzer")
                .synonymSourceDefinition("my_synonyms")
                .build(),
            SynonymMappingDefinitionBuilder.builder()
                .name("withDuplicates")
                .analyzer("fieldAnalyzer")
                .synonymSourceDefinition("my_synonyms")
                .build());

    SearchIndexDefinition indexDefinition = getIndexDefinition(synonyms);
    AnalyzerRegistry registry = getAnalyzerRegistry();
    SynonymRegistry synonymRegistry =
        LuceneSynonymRegistry.create(registry, indexDefinition.getSynonymMap(), Optional.empty());

    List<String> equivalentSynonyms = List.of("fast", "quick");

    SynonymMapping.Builder builder =
        synonymRegistry.mappingBuilder(indexDefinition.getSynonymMap().get("normal"));
    builder.addDocument(SynonymDocumentBuilder.equivalent(equivalentSynonyms));
    synonymRegistry.update("normal", builder.build());

    SynonymMapping.Builder duplicateBuilder =
        synonymRegistry.mappingBuilder(indexDefinition.getSynonymMap().get("withDuplicates"));
    duplicateBuilder.addDocument(SynonymDocumentBuilder.equivalent(equivalentSynonyms));
    duplicateBuilder.addDocument(SynonymDocumentBuilder.equivalent(equivalentSynonyms));
    synonymRegistry.update("withDuplicates", duplicateBuilder.build());

    SearchQueryFactoryContext queryFactoryContext =
        new SearchQueryFactoryContext(
            registry,
            LuceneAnalyzer.queryAnalyzer(indexDefinition, registry),
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            synonymRegistry,
            new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
            FeatureFlags.getDefault());

    StringPathQuery spq =
        new StringPathQuery("fast", StringPathBuilder.fieldPath("fieldWithAnalyzer"));

    Analyzer analyzer =
        queryFactoryContext.getSynonymAnalyzer("normal", spq.getPath(), Optional.empty());
    TokenStream queryGraph = analyzer.tokenStream("$type:string/fieldWithAnalyzer", spq.getQuery());
    TokenStreamToTermAutomatonQuery termAutomatonQuery = new TokenStreamToTermAutomatonQuery();
    Query query = termAutomatonQuery.toQuery("$type:string/fieldWithAnalyzer", queryGraph);

    Analyzer analyzerWithDuplicates =
        queryFactoryContext.getSynonymAnalyzer("withDuplicates", spq.getPath(), Optional.empty());
    TokenStream queryGraphWithDuplicates =
        analyzerWithDuplicates.tokenStream("$type:string/fieldWithAnalyzer", spq.getQuery());
    TokenStreamToTermAutomatonQuery termAutomatonQueryWithoutDuplicates =
        new TokenStreamToTermAutomatonQuery();
    Query queryWithDuplicates =
        termAutomatonQueryWithoutDuplicates.toQuery(
            "$type:string/fieldWithAnalyzer", queryGraphWithDuplicates);

    // NB: lucene queries are not reliably equality comparable, and TermAutomatonQuery in particular
    // does not seem to support equality comparison.
    Assert.assertEquals(
        "synonym map with duplicate entries should yield same query as synonym map without"
            + " duplicates",
        query.toString(),
        queryWithDuplicates.toString());
  }

  @Test
  public void testEmptySynonymMapGeneratesRegularTextQuery() throws Exception {
    List<SynonymMappingDefinition> synonyms =
        SynonymMappingDefinitionBuilder.builder()
            .name("en")
            .analyzer("fieldAnalyzer")
            .synonymSourceDefinition("my_synonyms")
            .buildAsList();

    SearchIndexDefinition indexDefinition = getIndexDefinition(synonyms);
    AnalyzerRegistry registry = getAnalyzerRegistry();
    SynonymRegistry synonymRegistry =
        LuceneSynonymRegistry.create(registry, indexDefinition.getSynonymMap(), Optional.empty());
    synonyms.forEach(synonymRegistry::clear);

    SearchQueryFactoryContext queryFactoryContext =
        new SearchQueryFactoryContext(
            registry,
            LuceneAnalyzer.queryAnalyzer(indexDefinition, registry),
            indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
            synonymRegistry,
            new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
            FeatureFlags.getDefault());

    StringPathQuery spq =
        new StringPathQuery("fast", StringPathBuilder.fieldPath("fieldWithAnalyzer"));
    Analyzer synonymAnalyzer =
        queryFactoryContext.getSynonymAnalyzer("en", spq.getPath(), Optional.empty());
    TokenStream queryGraph =
        synonymAnalyzer.tokenStream("$type:string/fieldWithEnglishAnalyzer", spq.getQuery());
    CharTermAttribute charTermAttribute = queryGraph.addAttribute(CharTermAttribute.class);

    @Var boolean firstToken = true;
    queryGraph.reset();
    while (queryGraph.incrementToken()) {
      String token = charTermAttribute.toString();
      Assert.assertEquals(
          String.format("tokenStream should generate synonyms but generated %s instead", token),
          "fast",
          token);
      Assert.assertTrue(firstToken);
      firstToken = false;
    }
    queryGraph.close();
  }

  static IndexMetricsUpdater.QueryingMetricsUpdater queryMetrics() {
    return new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());
  }

  @Test
  public void testQueryWithAllStopWords() throws Exception {
    SearchOperator definition = OperatorBuilder.search().path("title").query("the a and").build();

    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("default")
            .database("mock_database")
            .lastObservedCollectionName("mock_collection")
            .collectionUuid(UUID.randomUUID())
            .dynamicMapping()
            .analyzerName("lucene.english")
            .build();
    AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
    SynonymRegistry synonymRegistry = SynonymRegistryBuilder.empty();
    LuceneSearchQueryFactoryDistributor factory =
        LuceneSearchQueryFactoryDistributor.create(
            indexDefinition,
            IndexFormatVersion.CURRENT,
            analyzerRegistry,
            synonymRegistry,
            queryMetrics(),
            false,
            FeatureFlags.getDefault());

    Query expected = new BooleanQuery.Builder().build();
    IndexReader reader = DirectoryReader.open(directory);
    Query result = factory.createQuery(definition, reader, DEFAULT_OPTIONS);
    Assert.assertEquals("testQueryWithAllStopWords", expected, result);
  }

  @Test
  public void testExplainQuerySimple() throws InvalidQueryException, IOException {
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping().build();
    LuceneSearchQueryFactoryDistributor factory =
        LuceneSearchQueryFactoryDistributor.create(
            indexDefinition,
            IndexFormatVersion.CURRENT,
            AnalyzerRegistryBuilder.empty(),
            SynonymRegistryBuilder.empty(),
            queryMetrics(),
            false,
            FeatureFlags.getDefault());

    Operator termOperator = OperatorBuilder.term().path("title").query("God FaThers").build();
    WrappedQuery expected =
        new WrappedQuery(new TermQuery(new Term("$type:string/title", "god fathers")));
    try (var reader = DirectoryReader.open(directory)) {
      Query result = factory.createSearchExplainQuery(termOperator, reader, DEFAULT_OPTIONS);
      Assert.assertEquals("TermQuery should be wrapped along with an operator", expected, result);
    }
  }

  @Test
  public void testExplainQueryCompound() throws InvalidQueryException, IOException {
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping().build();
    LuceneSearchQueryFactoryDistributor factory =
        LuceneSearchQueryFactoryDistributor.create(
            indexDefinition,
            IndexFormatVersion.CURRENT,
            AnalyzerRegistryBuilder.empty(),
            SynonymRegistryBuilder.empty(),
            queryMetrics(),
            false,
            FeatureFlags.getDefault());

    // Operator for inner BooleanQuery
    Operator termOperator = OperatorBuilder.term().path("title").query("Harry Potter").build();
    Operator innerBoolOperator =
        OperatorBuilder.compound().should(termOperator).filter(termOperator).build();

    // Inner BooleanQuery
    TermQuery termQuery = new TermQuery(new Term("$type:string/title", "harry potter"));
    WrappedQuery innerQuery =
        new WrappedQuery(
            new BooleanQuery.Builder()
                .add(
                    shouldClause(
                        new WrappedQuery(
                            termQuery,
                            Optional.of(
                                FieldPath.fromParts("compound", "filter", "compound", "should")))))
                .add(
                    filterClause(
                        new WrappedQuery(
                            termQuery,
                            Optional.of(
                                FieldPath.fromParts("compound", "filter", "compound", "filter")))))
                .build(),
            Optional.of(FieldPath.fromParts("compound", "filter")));

    Operator outerBoolOperator = OperatorBuilder.compound().filter(innerBoolOperator).build();

    WrappedQuery expected =
        new WrappedQuery(new BooleanQuery.Builder().add(filterClause(innerQuery)).build());
    try (var reader = DirectoryReader.open(directory)) {
      Query result = factory.createSearchExplainQuery(outerBoolOperator, reader, DEFAULT_OPTIONS);
      Assert.assertEquals(
          "Each lucene query should be wrapped along with it's corresponding operator",
          expected,
          result);

      assertThat(result).isInstanceOf(WrappedQuery.class);
    }
  }

  /**
   * Tests QueryStringDefinition to Lucene Query.
   *
   * @throws Exception Exception
   */
  @Test
  public void testQueryStringDefinition() throws Exception {
    QueryStringOperator definition =
        OperatorBuilder.queryString().defaultPath("a").query("foo AND bar").build();

    SearchIndexDefinition indexDefinition = SearchIndexDefinitionBuilder.VALID_INDEX;
    AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
    LuceneSynonymRegistry synonymRegistry =
        LuceneSynonymRegistry.create(
            analyzerRegistry, indexDefinition.getSynonymMap(), Optional.empty());

    LuceneSearchQueryFactoryDistributor factory =
        LuceneSearchQueryFactoryDistributor.create(
            indexDefinition,
            IndexFormatVersion.CURRENT,
            analyzerRegistry,
            synonymRegistry,
            queryMetrics(),
            false,
            FeatureFlags.getDefault());
    IndexReader reader = DirectoryReader.open(directory);
    Query result = factory.createQuery(definition, reader, DEFAULT_OPTIONS);
    Assert.assertEquals("+$type:string/a:foo +$type:string/a:bar", result.toString());
  }

  @Test
  public void testInWithSingleValueTranslatesToEquals() throws Exception {
    InOperator definition = OperatorBuilder.in().path("title").strings(List.of("foo")).build();
    var indexQuery =
        new ConstantScoreQuery(new TermQuery(new Term("$type:token/title", new BytesRef("foo"))));
    var docValuesQuery =
        SortedSetDocValuesField.newSlowExactQuery("$type:token/title", new BytesRef("foo"));

    var expected = new IndexOrDocValuesQuery(indexQuery, docValuesQuery);

    LuceneSearchTranslation.get().assertTranslatedTo(definition, expected);
  }

  @Test
  public void testInWithSingleObjectIdTranslatesToEquals() throws Exception {
    ObjectId objId = new ObjectId("507f1f77bcf86cd799439011");
    InOperator definition = OperatorBuilder.in().path("objId").objectIds(List.of(objId)).build();
    var expected =
        new ConstantScoreQuery(
            new TermQuery(new Term("$type:objectId/objId", new BytesRef(objId.toByteArray()))));

    LuceneSearchTranslation.get().assertTranslatedTo(definition, expected);
  }

  @Test
  public void testTooComplexToDeterminizeExceptionThrowsInvalidQueryException() throws Exception {
    Random random = new Random(0);
    var operator =
        OperatorBuilder.wildcard()
            .query(
                RandomStringUtils.random(10000, Ascii.SPACE, Ascii.DEL, false, false, null, random))
            .path("title")
            .allowAnalyzedField(true)
            .build();
    Exception e =
        Assert.assertThrows(
            InvalidQueryException.class, () -> LuceneSearchTranslation.get().translate(operator));
    String msg = e.getMessage();
    Assert.assertTrue(msg.contains("Determinizing the automaton would require too much work"));
  }

  @Test
  public void testOmitSearchDocumentResultsWrapsQueryInConstantScore() throws Exception {
    TextOperator definition = OperatorBuilder.text().path("title").query("placeholder").build();

    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("default")
            .database("mock_database")
            .lastObservedCollectionName("mock_collection")
            .collectionUuid(UUID.randomUUID())
            .dynamicMapping()
            .analyzerName("lucene.english")
            .build();

    AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
    SynonymRegistry synonymRegistry = SynonymRegistryBuilder.empty();
    LuceneSearchQueryFactoryDistributor factory =
        LuceneSearchQueryFactoryDistributor.create(
            indexDefinition,
            IndexFormatVersion.CURRENT,
            analyzerRegistry,
            synonymRegistry,
            queryMetrics(),
            false,
            FeatureFlags.getDefault());

    IndexReader reader = DirectoryReader.open(directory);

    Query expected =
        factory.createQuery(
            definition,
            reader,
            QueryOptimizationFlagsBuilder.builder().omitSearchDocumentResults(false).build());
    Query result =
        factory.createQuery(
            definition,
            reader,
            QueryOptimizationFlagsBuilder.builder().omitSearchDocumentResults(true).build());
    // should return same query wrapped in a ConstantScoreQuery
    assertThat(result).isEqualTo(new ConstantScoreQuery(expected));
  }

  @Test
  public void testOmitSearchDocumentResultsCreatesEmptySort() throws Exception {
    SearchIndexDefinition indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("default")
            .database("mock_database")
            .lastObservedCollectionName("mock_collection")
            .collectionUuid(UUID.randomUUID())
            .dynamicMapping()
            .analyzerName("lucene.english")
            .build();

    AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
    SynonymRegistry synonymRegistry = SynonymRegistryBuilder.empty();
    LuceneSearchQueryFactoryDistributor factory =
        LuceneSearchQueryFactoryDistributor.create(
            indexDefinition,
            IndexFormatVersion.CURRENT,
            analyzerRegistry,
            synonymRegistry,
            queryMetrics(),
            false,
            FeatureFlags.getDefault());

    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .sortField(
                SortFieldBuilder.builder()
                    .path("bar")
                    .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                    .build())
            .buildSort();

    assertThat(
            factory.createSort(
                Optional.of(sortSpec),
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("foo"),
                        FieldName.TypeField.NUMBER_INT64_V2,
                        FieldPath.newRoot("bar"),
                        FieldName.TypeField.TOKEN),
                    ImmutableMap.of()),
                Optional.empty(),
                QueryOptimizationFlagsBuilder.builder().omitSearchDocumentResults(true).build(),
                Optional.empty()))
        .isEmpty();
  }
}
