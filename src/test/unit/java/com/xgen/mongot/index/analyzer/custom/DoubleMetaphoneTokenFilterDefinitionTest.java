package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class DoubleMetaphoneTokenFilterDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.DoubleMetaphoneTokenFilter.builder().build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testIncludeOriginal() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.DoubleMetaphoneTokenFilter.builder()
            .originalTokens(OriginalTokens.INCLUDE)
            .build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, List.of("smith"), List.of("smith", "SM0", "XMT"));
  }

  @Test
  public void testOmitOriginal() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.DoubleMetaphoneTokenFilter.builder()
            .originalTokens(OriginalTokens.OMIT)
            .build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, List.of("smith"), List.of("SM0", "XMT"));
  }

  @Test
  public void apply_defaultMaxCodeLength_producesFullCode() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.DoubleMetaphoneTokenFilter.builder()
            .originalTokens(OriginalTokens.OMIT)
            .build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, List.of("weekend"), List.of("AKNT", "FKNT"));
  }

  @Test
  public void apply_maxCodeLength_truncatesCode() throws Exception {
    TokenFilterDefinition tokenFilterDefinition =
        TokenFilterDefinitionBuilder.DoubleMetaphoneTokenFilter.builder()
            .originalTokens(OriginalTokens.OMIT)
            .maxCodeLength(2)
            .build();

    TokenFilterTestUtil.testTokenFilterProducesTokens(
        tokenFilterDefinition, List.of("weekend"), List.of("AK", "FK"));
  }
}
