package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.lucene.analyzer.TokenFilterTestUtil;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.junit.Assert;
import org.junit.Test;

public class LimitTokenFilterDefinitionTest {

  @Test
  public void testEmptyInput() throws Exception {
    LimitTokenFilterDefinition filter = new LimitTokenFilterDefinition(3);
    TokenFilterTestUtil.testTokenFilterProducesTokens(
        filter, Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testTruncatesToMaxTokens() throws Exception {
    LimitTokenFilterDefinition filter = new LimitTokenFilterDefinition(3);

    List<String> inputTokens = Arrays.asList("one", "two", "three", "four", "five");
    List<String> expectedTokens = Arrays.asList("one", "two", "three");

    TokenFilterTestUtil.testTokenFilterProducesTokens(filter, inputTokens, expectedTokens);
  }

  @Test
  public void testFunctionalTokenStream() throws Exception {
    LimitTokenFilterDefinition filter = new LimitTokenFilterDefinition(3);
    String input = "one two three four five six";

    StandardTokenizer tokenizer = new StandardTokenizer();
    tokenizer.setReader(new StringReader(input));

    TokenStream ts = new LimitTokenCountFilter(tokenizer, filter.getMaxTokenCount(), true);
    CharTermAttribute termAttr = ts.addAttribute(CharTermAttribute.class);

    ts.reset();
    List<String> tokens = new java.util.ArrayList<>();
    while (ts.incrementToken()) {
      tokens.add(termAttr.toString());
    }
    ts.end();
    ts.close();

    Assert.assertEquals(Arrays.asList("one", "two", "three"), tokens);
  }

  @Test
  public void testOffsetBehaviorConsumeAllTokensTrue() throws Exception {
    String input = "one two three four";
    int limit = 2;

    StandardTokenizer tokenizer = new StandardTokenizer();
    tokenizer.setReader(new StringReader(input));

    TokenStream ts = new LimitTokenCountFilter(tokenizer, limit, true);
    ts.reset();
    while (ts.incrementToken()) {
      /* consume tokens */
    }
    ts.end();

    OffsetAttribute offsetAttr = ts.getAttribute(OffsetAttribute.class);
    Assert.assertEquals(
        "With consumeAllTokens=true, endOffset should be full string length",
        input.length(),
        offsetAttr.endOffset());
    ts.close();
  }

  @Test
  public void testOffsetBehaviorConsumeAllTokensFalse() throws Exception {
    String input = "one two three four";
    int limit = 2;

    StandardTokenizer tokenizer = new StandardTokenizer();
    tokenizer.setReader(new StringReader(input));

    TokenStream ts = new LimitTokenCountFilter(tokenizer, limit, false);
    ts.reset();
    while (ts.incrementToken()) {
      /* consume tokens */
    }
    ts.end();

    OffsetAttribute offsetAttr = ts.getAttribute(OffsetAttribute.class);
    Assert.assertEquals(
        "End offset should match the end of the second token", 7, offsetAttr.endOffset());
    Assert.assertTrue(
        "With consumeAllTokens=false, endOffset should be less than full string length",
        offsetAttr.endOffset() < input.length());
    ts.close();
  }
}
