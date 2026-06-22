package com.xgen.mongot.index.analyzer.wrapper;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.index.definition.SearchIndexDefinition.DEFAULT_FALLBACK_ANALYZER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.analyzer.AnalyzerMeta;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import com.xgen.mongot.index.lucene.field.FieldName.MultiField;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.path.string.StringFieldPath;
import com.xgen.mongot.index.path.string.StringMultiFieldPath;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.testing.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.junit.Before;
import org.junit.Test;

public class QueryAnalyzerWrapperTest {

  private static final StringMultiFieldPath MULTI_PATH =
      StringPathBuilder.withMulti("field", "multi");

  private static final StringFieldPath STRING_PATH = StringPathBuilder.fieldPath("nonMulti");

  private AnalyzerRegistry analyzerRegistry;

  private QueryAnalyzerWrapper wrapper;

  @Before
  public void setup() throws InvalidAnalyzerDefinitionException {
    this.analyzerRegistry = getAnalyzerRegistry();
    String multiFieldName = MultiField.getLuceneFieldName(MULTI_PATH, Optional.empty());
    String fieldName =
        TypeField.STRING.getLuceneFieldName(STRING_PATH.getValue(), Optional.empty());
    ImmutableMap<String, AnalyzerMeta> mapping =
        ImmutableMap.of(
            multiFieldName, this.analyzerRegistry.getAnalyzerMeta("multiAnalyzer"),
            fieldName, this.analyzerRegistry.getAnalyzerMeta("fieldAnalyzer"));
    this.wrapper =
        new QueryAnalyzerWrapper(
            SearchIndexDefinitionBuilder.VALID_INDEX,
            this.analyzerRegistry.getAnalyzerMeta(DEFAULT_FALLBACK_ANALYZER),
            mapping,
            Optional.empty());
  }

  private static AnalyzerRegistry getAnalyzerRegistry() throws InvalidAnalyzerDefinitionException {
    List<OverriddenBaseAnalyzerDefinition> analyzers = new ArrayList<>();
    for (String name : Arrays.asList("multiAnalyzer", "fieldAnalyzer")) {
      analyzers.add(
          OverriddenBaseAnalyzerDefinitionBuilder.builder()
              .baseAnalyzerName(StockAnalyzerNames.LUCENE_STANDARD.getName())
              .name(name)
              .build());
    }
    return AnalyzerRegistry.factory().create(analyzers, true);
  }

  @Test
  public void testInvalidMultiThrowsException() {
    StringPath invalidMulti = StringPathBuilder.withMulti("field", "invalidMulti");

    InvalidQueryException ex =
        assertThrows(
            InvalidQueryException.class,
            () -> this.wrapper.getAnalyzerMeta(invalidMulti, Optional.empty()));

    assertThat(ex)
        .hasMessageThat()
        .isEqualTo("field (multi: invalidMulti) not found in index: default");
  }

  @Test
  public void testValidMultiReturnsMeta() throws InvalidQueryException {
    AnalyzerMeta meta = this.wrapper.getAnalyzerMeta(MULTI_PATH, Optional.empty());

    assertEquals("multiAnalyzer", meta.getName());
  }

  @Test
  public void testUnknownFieldReturnsFallback() throws InvalidQueryException {
    StringPath unknownField = StringPathBuilder.fieldPath("unknown_field");

    AnalyzerMeta meta = this.wrapper.getAnalyzerMeta(unknownField, Optional.empty());

    assertEquals(DEFAULT_FALLBACK_ANALYZER, meta.getName());
  }

  @Test
  public void testValidMultiReturnsAnalyzer() {
    String luceneFieldName = MultiField.getLuceneFieldName(MULTI_PATH, Optional.empty());
    Analyzer expected = this.analyzerRegistry.getAnalyzer("multiAnalyzer");

    Analyzer result = this.wrapper.getWrappedAnalyzer(luceneFieldName);

    assertEquals(expected, result);
  }

  @Test
  public void testUnknownFieldReturnsFallbackAnalyzer() {
    StringFieldPath unknownField = StringPathBuilder.fieldPath("unknown_field");
    String luceneFieldName =
        TypeField.STRING.getLuceneFieldName(unknownField.getValue(), Optional.empty());
    Analyzer expected = this.analyzerRegistry.getAnalyzer(DEFAULT_FALLBACK_ANALYZER);

    Analyzer result = this.wrapper.getWrappedAnalyzer(luceneFieldName);

    assertEquals(expected, result);
  }
}
