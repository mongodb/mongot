package com.xgen.mongot.index.lucene;

import com.google.common.truth.Truth;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.SearchHighlight;
import com.xgen.mongot.index.SearchHighlightText;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.explainers.HighlightFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.highlights.Highlight;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import com.xgen.testing.mongot.index.query.highlights.HighlightBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      LuceneUnifiedHighlighterTest.TestMongotHighlightTransposition.class,
      LuceneUnifiedHighlighterTest.TestQueryValidation.class,
      LuceneUnifiedHighlighterTest.TestUnitLogic.class,
      LuceneUnifiedHighlighterTest.TestComputeStoredFieldsAndRemap.class,
      LuceneUnifiedHighlighterTest.TestByPathComparator.class
    })
public class LuceneUnifiedHighlighterTest {

  @RunWith(MockitoJUnitRunner.class)
  public static class TestMongotHighlightTransposition {
    private final List<List<SearchHighlight>> docSearchHighlights = new ArrayList<>(2);

    /** Setup the docSearchHighlights array to test against. */
    public TestMongotHighlightTransposition() {
      float testScore = 1.234f;

      LuceneSearchHighlights[] luceneSearchHighlightsArray0 = new LuceneSearchHighlights[2];
      luceneSearchHighlightsArray0[0] =
          new LuceneSearchHighlights(
              List.of(
                  new LuceneSearchHighlight(
                      testScore,
                      List.of(
                          new SearchHighlightText(
                              "testSearchHighlightText0", SearchHighlightText.Type.TEXT)))));

      luceneSearchHighlightsArray0[1] =
          new LuceneSearchHighlights(
              List.of(
                  new LuceneSearchHighlight(
                      testScore,
                      List.of(
                          new SearchHighlightText(
                              "testSearchHighlightText1", SearchHighlightText.Type.TEXT)))));

      LuceneSearchHighlights[] luceneSearchHighlightsArray1 = new LuceneSearchHighlights[2];
      luceneSearchHighlightsArray1[0] =
          new LuceneSearchHighlights(
              List.of(
                  new LuceneSearchHighlight(
                      testScore,
                      List.of(
                          new SearchHighlightText(
                              "testSearchHighlightText2", SearchHighlightText.Type.TEXT)))));

      luceneSearchHighlightsArray1[1] =
          new LuceneSearchHighlights(
              List.of(
                  new LuceneSearchHighlight(
                      testScore,
                      List.of(
                          new SearchHighlightText(
                              "testSearchHighlightText3", SearchHighlightText.Type.TEXT)))));

      Map<String, Object[]> testMap = new HashMap<>();
      testMap.put("$type:string/testPath0", luceneSearchHighlightsArray0);
      testMap.put("$type:string/testPath1", luceneSearchHighlightsArray1);

      for (int i = 0; i < 2; i++) {
        this.docSearchHighlights.add(new ArrayList<>());
      }

      LuceneUnifiedHighlighter.mergeSearchHighlightsArray(testMap, this.docSearchHighlights);
    }

    @Test
    public void testSortTextsIntoDocs() {

      // Check for correct text
      Assert.assertEquals(
          "testSearchHighlightText0",
          "testSearchHighlightText0",
          this.docSearchHighlights.get(0).get(0).texts().get(0).value());

      Assert.assertEquals(
          "testSearchHighlightText2",
          "testSearchHighlightText2",
          this.docSearchHighlights.get(0).get(1).texts().get(0).value());

      Assert.assertEquals(
          "testSearchHighlightText1",
          "testSearchHighlightText1",
          this.docSearchHighlights.get(1).get(0).texts().get(0).value());

      Assert.assertEquals(
          "testSearchHighlightText3",
          "testSearchHighlightText3",
          this.docSearchHighlights.get(1).get(1).texts().get(0).value());
    }

    @Test
    public void testSortPathsIntoDocs() {
      Assert.assertEquals(
          "testPath00",
          this.docSearchHighlights.get(0).get(0).path().asField().getValue(),
          FieldPath.parse("testPath0"));
      Assert.assertEquals(
          "testPath10",
          this.docSearchHighlights.get(0).get(1).path().asField().getValue(),
          FieldPath.parse("testPath1"));
      Assert.assertEquals(
          "testPath10",
          this.docSearchHighlights.get(1).get(0).path().asField().getValue(),
          FieldPath.parse("testPath0"));
      Assert.assertEquals(
          "testPath11",
          this.docSearchHighlights.get(1).get(1).path().asField().getValue(),
          FieldPath.parse("testPath1"));
    }
  }

  public static class TestQueryValidation {
    private static Directory directory;
    private static IndexReader reader;
    private static IndexWriter writer;

    /** set up an index. */
    @Before
    public void setUp() throws IOException {
      TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
      directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
      writer = new IndexWriter(directory, new IndexWriterConfig());
      writer.commit();
      reader = DirectoryReader.open(directory);
    }

    /** teardown index. */
    @After
    public void tearDown() throws IOException {
      reader.close();
      writer.close();
      directory.close();
    }

    public LuceneUnifiedHighlighter highlighterFor(Highlight highlight, Query query) {
      Optional<HighlightFeatureExplainer> explainer =
          Explain.isEnabled()
              ? Explain.getExplainQueryState()
                  .map(
                      state ->
                          state
                              .getQueryInfo()
                              .getFeatureExplainer(
                                  HighlightFeatureExplainer.class,
                                  () -> new HighlightFeatureExplainer(highlight)))
              : Optional.empty();

      return LuceneUnifiedHighlighter.create(
          new IndexSearcher(reader), new StandardAnalyzer(), highlight, query, explainer);
    }

    @Test
    public void testExplain() throws IOException {
      try (var unused =
          Explain.setup(
              Optional.of(Explain.Verbosity.EXECUTION_STATS),
              Optional.of(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()))) {
        var highlight =
            HighlightBuilder.builder()
                .maxNumPassages(1)
                .maxCharsToExamine(1)
                .path("title")
                .storedPath("title")
                .build();
        var unifiedHighlighter = highlighterFor(highlight, new MatchAllDocsQuery());
        unifiedHighlighter.highlightsAsSearchHighlightsArray(
            new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]));

        var result = Explain.collect();
        Truth.assertThat(result.get().highlightStats()).isPresent();
        var highlightStats = result.get().highlightStats().get();

        Truth.assertThat(
                highlightStats
                    .stats()
                    .get()
                    .invocationCounts()
                    .get()
                    .get(ExplainTimings.Type.EXECUTE_HIGHLIGHT.getName()))
            .isEqualTo(1);
      }
    }

    @Test
    public void testValidatesUtf8() {
      var highlight =
          HighlightBuilder.builder()
              .maxNumPassages(1)
              .maxCharsToExamine(1)
              .path("title")
              .storedPath("title")
              .build();
      var brokenUnicode =
          new BytesRef(new byte[] {(byte) 0xE7, (byte) 0x84}); // 無 but missing the last byte.
      var query = new TermQuery(new Term("$type:string/title", brokenUnicode));

      var unifiedHighlighter = highlighterFor(highlight, query);

      var exception =
          Assert.assertThrows(
              InvalidQueryException.class, unifiedHighlighter::assertHighlightedTermsValid);
      Assert.assertEquals("highlight: query value must be utf-8: title", exception.getMessage());
    }

    @Test
    public void testValidatesUtf8Complex() throws InvalidQueryException, IOException {
      var highlight =
          HighlightBuilder.builder()
              .maxNumPassages(1)
              .path("title")
              .storedPath("title")
              .multi("description", "ja_JP")
              .storedMulti("description", "ja_JP")
              .build();
      var brokenUnicode =
          new BytesRef(new byte[] {(byte) 0xE7, (byte) 0x84}); // 無 but missing the last byte.
      var query =
          new BooleanQuery.Builder()
              .add(
                  new TermQuery(new Term("$type:string/title", "breakin")),
                  BooleanClause.Occur.MUST)
              .add(
                  new TermQuery(new Term("$type:string/title", "boogaloo")),
                  BooleanClause.Occur.SHOULD)
              .add(
                  new TermQuery(new Term("$multi/description.ja_JP", brokenUnicode)),
                  BooleanClause.Occur.MUST)
              .build();

      var unifiedHighlighter = highlighterFor(highlight, query);

      var exception =
          Assert.assertThrows(
              InvalidQueryException.class, unifiedHighlighter::assertHighlightedTermsValid);
      Assert.assertEquals(
          "highlight: query value must be utf-8: description (multi: ja_JP)",
          exception.getMessage());
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class TestUnitLogic {

    @Test
    public void testFromLuceneStringOrAutocompleteFieldName() {
      @Var StringPath expectedStringPath = StringPathBuilder.fieldPath("a.b.c");

      Assert.assertEquals(
          "a.b.c",
          expectedStringPath,
          LuceneUnifiedHighlighter.fromLuceneStringOrAutocompleteFieldName("$type:string/a.b.c"));

      Assert.assertEquals(
          "a.b.c",
          expectedStringPath,
          LuceneUnifiedHighlighter.fromLuceneStringOrAutocompleteFieldName(
              "$type:autocomplete/a.b.c"));

      expectedStringPath = StringPathBuilder.withMulti("a.b.c", "multi");

      Assert.assertEquals(
          "$multi/a.b.c.multi",
          expectedStringPath,
          LuceneUnifiedHighlighter.fromLuceneStringOrAutocompleteFieldName("$multi/a.b.c.multi"));

      Assert.assertThrows(
          IllegalArgumentException.class,
          () ->
              LuceneUnifiedHighlighter.fromLuceneStringOrAutocompleteFieldName(
                  "$type:boolean/a.b.c"));
    }
  }

  public static class TestComputeStoredFieldsAndRemap {

    @Test
    public void computeStoredFieldsAndRemap_deduplicatesStoredFields() {
      String[] fields = {"$multi/title.french", "$multi/title.spanish", "$type:string/title"};
      Map<String, String> map =
          Map.of(
              "$multi/title.french", "$type:string/title",
              "$multi/title.spanish", "$type:string/title",
              "$type:string/title", "$type:string/title");
      int[] remapOut = new int[fields.length];

      String[] storedFields =
          LuceneUnifiedHighlighter.PublicUnifiedHighlighter.computeStoredFieldsAndRemap(
              fields, map, remapOut);

      Assert.assertArrayEquals(new String[] {"$type:string/title"}, storedFields);

      Assert.assertArrayEquals(new int[] {0, 0, 0}, remapOut);
    }

    @Test
    public void computeStoredFieldsAndRemap_sortsStoredFields() {
      String[] fields = {
        "$multi/title.french", "$multi/author.french", "$multi/plot.french", "$type:string/title"
      };
      Map<String, String> map =
          Map.of(
              "$multi/title.french", "$type:string/title",
              "$multi/author.french", "$type:string/author",
              "$multi/plot.french", "$type:string/plot",
              "$type:string/title", "$type:string/title");
      int[] remapOut = new int[fields.length];

      String[] storedFields =
          LuceneUnifiedHighlighter.PublicUnifiedHighlighter.computeStoredFieldsAndRemap(
              fields, map, remapOut);

      Assert.assertArrayEquals(
          new String[] {"$type:string/author", "$type:string/plot", "$type:string/title"},
          storedFields);

      Assert.assertArrayEquals(new int[] {2, 0, 1, 2}, remapOut);
    }

    @Test
    public void computeStoredFieldsAndRemap_storedFieldsOnly() {
      String[] fields = {"$type:string/title", "$type:string/author"};
      Map<String, String> map =
          Map.of(
              "$type:string/title", "$type:string/title",
              "$type:string/author", "$type:string/author");
      int[] remapOut = new int[fields.length];

      String[] storedFields =
          LuceneUnifiedHighlighter.PublicUnifiedHighlighter.computeStoredFieldsAndRemap(
              fields, map, remapOut);

      Assert.assertArrayEquals(
          new String[] {"$type:string/author", "$type:string/title"}, storedFields);

      Assert.assertArrayEquals(new int[] {1, 0}, remapOut);
    }
  }

  public static class TestByPathComparator {

    private static SearchHighlight highlight(StringPath path) {
      return new SearchHighlight(
          1.0f, path, List.of(new SearchHighlightText("text", SearchHighlightText.Type.TEXT)));
    }

    @Test
    public void byPath_fieldPaths_sortsAlphabeticallyByPath() {
      List<SearchHighlight> highlights =
          new ArrayList<>(
              List.of(
                  highlight(StringPathBuilder.fieldPath("c")),
                  highlight(StringPathBuilder.fieldPath("a")),
                  highlight(StringPathBuilder.fieldPath("b"))));

      highlights.sort(LuceneUnifiedHighlighter.BY_PATH);

      Truth.assertThat(
              highlights.stream().map(h -> h.path().toString()).collect(Collectors.toList()))
          .containsExactly("a", "b", "c")
          .inOrder();
    }

    @Test
    public void byPath_nestedFieldPaths_sortsByLexicographicPath() {
      List<SearchHighlight> highlights =
          new ArrayList<>(
              List.of(
                  highlight(StringPathBuilder.fieldPath("a.z")),
                  highlight(StringPathBuilder.fieldPath("a.b")),
                  highlight(StringPathBuilder.fieldPath("a"))));

      highlights.sort(LuceneUnifiedHighlighter.BY_PATH);

      Truth.assertThat(
              highlights.stream().map(h -> h.path().toString()).collect(Collectors.toList()))
          .containsExactly("a", "a.b", "a.z")
          .inOrder();
    }

    @Test
    public void byPath_multiAndFieldPaths_orderMultiAfterBaseField() {
      // StringMultiFieldPath.toString() is "<path> (multi: <name>)", so it sorts after the bare
      // base field "a" but is still grouped near other "a"-rooted paths.
      List<SearchHighlight> highlights =
          new ArrayList<>(
              List.of(
                  highlight(StringPathBuilder.withMulti("a", "french")),
                  highlight(StringPathBuilder.fieldPath("a"))));

      highlights.sort(LuceneUnifiedHighlighter.BY_PATH);

      Truth.assertThat(
              highlights.stream().map(h -> h.path().toString()).collect(Collectors.toList()))
          .containsExactly("a", "a (multi: french)")
          .inOrder();
    }

    @Test
    public void byPath_alreadySorted_isStableNoop() {
      List<SearchHighlight> highlights =
          new ArrayList<>(
              List.of(
                  highlight(StringPathBuilder.fieldPath("a")),
                  highlight(StringPathBuilder.fieldPath("b")),
                  highlight(StringPathBuilder.fieldPath("c"))));

      highlights.sort(LuceneUnifiedHighlighter.BY_PATH);

      Truth.assertThat(
              highlights.stream().map(h -> h.path().toString()).collect(Collectors.toList()))
          .containsExactly("a", "b", "c")
          .inOrder();
    }
  }
}
