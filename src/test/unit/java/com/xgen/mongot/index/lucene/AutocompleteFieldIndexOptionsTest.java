package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.lucene.InstrumentedConcurrentMergeSchedulerTest.createMergeScheduler;
import static com.xgen.mongot.index.lucene.field.FieldName.TypeField.AUTOCOMPLETE;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_GENERATION_ID;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_ID;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.SearchHighlight;
import com.xgen.mongot.index.SearchHighlightText;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.merge.InstrumentedConcurrentMergeScheduler;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.lucene.writer.SingleLuceneIndexWriter;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.highlights.UnresolvedHighlight;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.definition.AutocompleteFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import com.xgen.testing.mongot.index.query.highlights.UnresolvedHighlightBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.apache.lucene.util.automaton.Operations;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Verifies that the Lucene {@link IndexOptions} used for an autocomplete field are gated on the
 * index feature version, and that highlighting is unaffected by the reduced options.
 *
 * <p>Autocomplete matching never consults positions or offsets, so newer indexes (feature version
 * &gt;= 5) index the field with {@code DOCS_AND_FREQS}, dropping the positions/offsets that older
 * indexes ({@code DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}) carry. Lucene forbids a field changing
 * its {@code IndexOptions} within a single index (see {@link LuceneIndexOptionsConsistencyTest}),
 * so the choice is keyed to the index's feature version.
 */
@RunWith(JUnit4.class)
public class AutocompleteFieldIndexOptionsTest {

  private static final String FIELD = "name";
  private static final String LUCENE_FIELD =
      AUTOCOMPLETE.getLuceneFieldName(FieldPath.newRoot(FIELD), Optional.empty());

  private static final int LEGACY_FEATURE_VERSION = 4;
  private static final int REDUCED_FEATURE_VERSION = 5;

  private static SearchIndexDefinition autocompleteIndexAt(int indexFeatureVersion) {
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .field(
                FIELD,
                FieldDefinitionBuilder.builder()
                    .autocomplete(AutocompleteFieldDefinitionBuilder.builder().build())
                    .build())
            .build();
    return SearchIndex.mockDefinitionBuilder()
        .mappings(mappings)
        .indexFeatureVersion(indexFeatureVersion)
        .build();
  }

  private static SingleLuceneIndexWriter writerFor(Directory directory, int indexFeatureVersion)
      throws Exception {
    return writerFor(
        directory, autocompleteIndexAt(indexFeatureVersion), AnalyzerRegistryBuilder.empty());
  }

  private static SingleLuceneIndexWriter writerFor(
      Directory directory, SearchIndexDefinition definition, AnalyzerRegistry analyzerRegistry)
      throws Exception {
    return SingleLuceneIndexWriter.createForSearchIndex(
        directory,
        createMergeScheduler(
            new InstrumentedConcurrentMergeScheduler(new SimpleMeterRegistry()),
            MOCK_INDEX_GENERATION_ID,
            0,
            1,
            false,
            directory),
        new TieredMergePolicy(),
        16D,
        Optional.empty(),
        Optional.empty(),
        LuceneAnalyzer.indexAnalyzer(definition, analyzerRegistry),
        definition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
        SearchIndex.mockIndexingMetricsUpdater(definition.getType()),
        Optional.empty(),
        FeatureFlags.getDefault(),
        () -> false);
  }

  private static void insertAutocompleteDoc(SingleLuceneIndexWriter writer, int id, String value)
      throws Exception {
    BsonDocument document =
        new BsonDocument()
            .append(MOCK_INDEX_ID.toString(), new BsonDocument("_id", new BsonInt32(id)))
            .append(FIELD, new BsonString(value));
    writer.updateIndex(
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(
                Optional.of(BsonUtils.documentToRaw(document)), MOCK_INDEX_ID),
            BsonUtils.documentToRaw(document)));
  }

  private static IndexOptions readAutocompleteIndexOptions(SingleLuceneIndexWriter writer)
      throws Exception {
    try (DirectoryReader reader = DirectoryReader.open(writer.getLuceneWriter())) {
      Set<IndexOptions> observed = new HashSet<>();
      for (LeafReaderContext leaf : reader.leaves()) {
        var fieldInfo = leaf.reader().getFieldInfos().fieldInfo(LUCENE_FIELD);
        if (fieldInfo != null) {
          observed.add(fieldInfo.getIndexOptions());
        }
      }
      // A single index must never mix index options for the same field.
      assertThat(observed).hasSize(1);
      return observed.iterator().next();
    }
  }

  @Test
  public void addAutocompleteField_legacyFeatureVersion_indexesPositionsAndOffsets()
      throws Exception {
    try (Directory directory = new MMapDirectory(TestUtils.getTempFolder().getRoot().toPath())) {
      SingleLuceneIndexWriter writer = writerFor(directory, LEGACY_FEATURE_VERSION);
      insertAutocompleteDoc(writer, 1, "pizza parlor");
      writer.commit(EncodedUserData.EMPTY);

      assertThat(readAutocompleteIndexOptions(writer))
          .isEqualTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
      writer.close();
    }
  }

  @Test
  public void addAutocompleteField_reducedFeatureVersion_dropsPositionsAndOffsets()
      throws Exception {
    try (Directory directory = new MMapDirectory(TestUtils.getTempFolder().getRoot().toPath())) {
      SingleLuceneIndexWriter writer = writerFor(directory, REDUCED_FEATURE_VERSION);
      insertAutocompleteDoc(writer, 1, "pizza parlor");
      writer.commit(EncodedUserData.EMPTY);

      assertThat(readAutocompleteIndexOptions(writer)).isEqualTo(IndexOptions.DOCS_AND_FREQS);
      writer.close();
    }
  }

  /**
   * Dropping positions/offsets at the reduced feature version must not change what users see:
   * highlight spans for an exact autocomplete query are identical, because the highlighter falls
   * back to re-analysis (offset source {@code analysis}) instead of reading postings offsets
   * ({@code postings}).
   */
  @Test
  public void highlight_exactQueryAtReducedFeatureVersion_matchesLegacySpans() throws Exception {
    // The autocomplete clause of AutocompleteQueryFactory#queryFor: a term query over the field's
    // pre-computed grams ("steakhouse" is a full edge-gram token of "morton's steakhouse").
    Query query = new TermQuery(new Term(LUCENE_FIELD, "steakhouse"));

    HighlightOutcome legacy = highlightAt(LEGACY_FEATURE_VERSION, query);
    HighlightOutcome reduced = highlightAt(REDUCED_FEATURE_VERSION, query);

    assertThat(highlightTexts(reduced.highlights())).isEqualTo(highlightTexts(legacy.highlights()));
    assertThat(legacy.offsetSources())
        .containsEntry("$type:autocomplete/name", UnifiedHighlighter.OffsetSource.POSTINGS);
    assertThat(reduced.offsetSources())
        .containsEntry("$type:autocomplete/name", UnifiedHighlighter.OffsetSource.ANALYSIS);
    assertThat(reduced.autocompleteClauseScore()).isEqualTo(legacy.autocompleteClauseScore());
  }

  /** Fuzzy queries already re-analyze at the legacy version (multi-term query), so spans match. */
  @Test
  public void highlight_fuzzyQueryAtReducedFeatureVersion_matchesLegacySpans() throws Exception {
    Query query = fuzzyAutocompleteQuery("stakhouse");

    HighlightOutcome legacy = highlightAt(LEGACY_FEATURE_VERSION, query);
    HighlightOutcome reduced = highlightAt(REDUCED_FEATURE_VERSION, query);

    assertThat(highlightTexts(reduced.highlights())).isEqualTo(highlightTexts(legacy.highlights()));
    // A fuzzy (multi-term) autocomplete query routes to re-analysis at both feature versions.
    assertThat(legacy.offsetSources())
        .containsEntry("$type:autocomplete/name", UnifiedHighlighter.OffsetSource.ANALYSIS);
    assertThat(reduced.offsetSources())
        .containsEntry("$type:autocomplete/name", UnifiedHighlighter.OffsetSource.ANALYSIS);
    assertThat(reduced.autocompleteClauseScore()).isEqualTo(legacy.autocompleteClauseScore());
  }

  /**
   * Mirrors {@code AutocompleteQueryFactory#fuzzyQueryFor} (package-private): an edit-distance
   * automaton over the autocomplete field's gram terms. maxEdits=1 turns "stakhouse" into a match
   * for the indexed "steakhouse".
   */
  private static Query fuzzyAutocompleteQuery(String queryToken) {
    LevenshteinAutomata automaton = new LevenshteinAutomata(queryToken, true);
    return new AutomatonQuery(
        new Term(LUCENE_FIELD, queryToken),
        Operations.determinize(
            automaton.toAutomaton(1, ""), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT));
  }

  /**
   * Indexes the same two documents at the given feature version, runs the autocomplete clause for
   * scoring, and highlights the matched document through the same machinery production uses ({@link
   * LuceneHighlighterContext} plus the Lucene unified highlighter).
   */
  private static HighlightOutcome highlightAt(int indexFeatureVersion, Query autocompleteClause)
      throws Exception {
    SearchIndexDefinition definition = autocompleteIndexAt(indexFeatureVersion);

    try (Directory directory = new MMapDirectory(TestUtils.getTempFolder().getRoot().toPath())) {
      SingleLuceneIndexWriter writer =
          writerFor(directory, definition, AnalyzerRegistryBuilder.empty());
      insertAutocompleteDoc(writer, 1, "morton's steakhouse");
      insertAutocompleteDoc(writer, 2, "jimmys pizza joint");
      writer.commit(EncodedUserData.EMPTY);
      writer.close();

      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        LuceneIndexSearcher searcher =
            LuceneIndexSearcher.create(
                reader,
                new QueryCacheProvider.DefaultQueryCacheProvider(),
                Optional.empty(),
                Optional.empty(),
                false,
                false,
                Optional.empty());

        TopDocs topDocs = searcher.search(autocompleteClause, 10);
        assertThat(topDocs.scoreDocs).hasLength(1);
        float autocompleteClauseScore = topDocs.scoreDocs[0].score;

        LuceneHighlighterContext context =
            new LuceneHighlighterContext(
                definition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
                LuceneAnalyzer.indexAnalyzer(definition, AnalyzerRegistryBuilder.empty()));
        UnresolvedHighlight highlight =
            UnresolvedHighlightBuilder.builder()
                .path(UnresolvedStringPathBuilder.fieldPath(FIELD))
                .build();

        try (var unused =
            Explain.setup(
                Optional.of(Explain.Verbosity.EXECUTION_STATS),
                Optional.of(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()))) {
          Optional<LuceneUnifiedHighlighter> highlighter =
              context.getHighlighterIfPresent(
                  searcher,
                  Optional.of(highlight),
                  autocompleteClause,
                  OperatorBuilder.autocomplete().path(FIELD).query("steakhouse").build(),
                  Optional.empty(),
                  QueryOptimizationFlags.DEFAULT_OPTIONS);

          List<List<SearchHighlight>> highlights =
              highlighter.orElseThrow().highlightsAsSearchHighlightsArray(topDocs);

          Map<String, UnifiedHighlighter.OffsetSource> offsetSources =
              Explain.collect().orElseThrow().highlightStats().orElseThrow().offsetSources();

          return new HighlightOutcome(highlights, offsetSources, autocompleteClauseScore);
        }
      }
    }
  }

  private static List<List<List<SearchHighlightText>>> highlightTexts(
      List<List<SearchHighlight>> highlights) {
    return highlights.stream()
        .map(doc -> doc.stream().map(SearchHighlight::texts).toList())
        .toList();
  }

  /** One arm of a legacy-vs-reduced comparison: highlight spans, offset sources, clause score. */
  private record HighlightOutcome(
      List<List<SearchHighlight>> highlights,
      Map<String, UnifiedHighlighter.OffsetSource> offsetSources,
      float autocompleteClauseScore) {}
}
