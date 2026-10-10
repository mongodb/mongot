package com.xgen.mongot.index.lucene.analyzer;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.regex.RegexBudget;
import com.xgen.mongot.util.regex.RegexLimitExceededException;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.pattern.PatternReplaceFilter;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.junit.Test;

public class BoundedPatternCompatibilityTest {
  private static final Pattern DELIMITER = Pattern.compile("[,\\s]+");
  private static final Pattern OPTIONAL_CAPTURE = Pattern.compile("(a)?b");
  private static final Pattern ZERO_WIDTH = Pattern.compile("(?=b)");
  private static final Pattern NAMED_CAPTURE = Pattern.compile("(?<letter>a)(b)");
  private static final Pattern ABSENT = Pattern.compile("z");
  private static final Pattern PATHOLOGICAL = Pattern.compile("(a+)+$");
  private static final Pattern RECURSIVE = Pattern.compile("(a|aa)+$");
  private static final Pattern LINEAR_CAPTURE = Pattern.compile("(a+)");

  @Test
  public void incrementToken_largeLinearInput_succeedsWithDefaultBudget() throws Exception {
    String text = "a".repeat(1_000_000);

    assertThat(tokenize(new BoundedPatternTokenizer(LINEAR_CAPTURE, 1), text))
        .containsExactly(new Token(text, 0, text.length()));
  }

  @Test
  public void incrementToken_deepRecursion_reportsControlledFailure() {
    assertThrows(
        RegexLimitExceededException.class,
        () -> tokenize(new BoundedPatternTokenizer(RECURSIVE, 1), "a".repeat(30_000) + "X"));
  }

  @Test
  public void incrementToken_deepReplacementRecursion_reportsControlledFailure() {
    KeywordTokenizer source = new KeywordTokenizer();
    source.setReader(new StringReader("a".repeat(30_000) + "X"));

    assertThrows(
        RegexLimitExceededException.class,
        () -> consume(new BoundedPatternReplaceFilter(source, RECURSIVE, "", true)));
  }

  @Test
  public void incrementToken_split_preservesLuceneTermsAndOffsets() throws Exception {
    Pattern pattern = DELIMITER;
    String text = ", alpha😀, beta  gamma,";

    assertThat(tokenize(new BoundedPatternTokenizer(pattern, -1), text))
        .isEqualTo(tokenize(new PatternTokenizer(pattern, -1), text));
  }

  @Test
  public void incrementToken_optionalCapture_preservesLuceneEmptyGroupHandling() throws Exception {
    Pattern pattern = OPTIONAL_CAPTURE;
    String text = "b ab b";

    assertThat(tokenize(new BoundedPatternTokenizer(pattern, 1), text))
        .isEqualTo(tokenize(new PatternTokenizer(pattern, 1), text));
  }

  @Test
  public void incrementToken_zeroWidthSplit_preservesLuceneBehavior() throws Exception {
    Pattern pattern = ZERO_WIDTH;

    assertThat(tokenize(new BoundedPatternTokenizer(pattern, -1), "abb"))
        .isEqualTo(tokenize(new PatternTokenizer(pattern, -1), "abb"));
  }

  @Test
  public void incrementToken_namedReplacement_preservesJavaReplacementSemantics() throws Exception {
    Pattern pattern = NAMED_CAPTURE;
    KeywordTokenizer original = new KeywordTokenizer();
    KeywordTokenizer bounded = new KeywordTokenizer();
    original.setReader(new StringReader("ab ab"));
    bounded.setReader(new StringReader("ab ab"));

    assertThat(consume(new BoundedPatternReplaceFilter(bounded, pattern, "${letter}-$2\\$", true)))
        .isEqualTo(consume(new PatternReplaceFilter(original, pattern, "${letter}-$2\\$", true)));
  }

  @Test
  public void incrementToken_multipleTokens_doesNotReplenishOperationBudget() throws Exception {
    WhitespaceTokenizer source = new WhitespaceTokenizer();
    source.setReader(new StringReader("a a a a a a a a a a a a"));

    try (var ignored = RegexBudget.openScope(5)) {
      assertThrows(
          RegexLimitExceededException.class,
          () -> consume(new BoundedPatternReplaceFilter(source, ABSENT, "", true)));
    }
  }

  @Test
  public void normalize_pathologicalReplacement_aborts() throws Exception {
    try (var analyzer =
            CustomAnalyzer.builder(KeywordTokenizer::new)
                .tokenFilter(
                    input -> new BoundedPatternReplaceFilter(input, PATHOLOGICAL, "", true))
                .build();
        var ignored = RegexBudget.openScope(10_000)) {
      assertThrows(
          RegexLimitExceededException.class,
          () -> analyzer.normalize("field", "a".repeat(1000) + "X"));
    }
  }

  private static List<Token> tokenize(Tokenizer tokenizer, String text) throws IOException {
    tokenizer.setReader(new StringReader(text));
    return consume(tokenizer);
  }

  private static List<Token> consume(TokenStream stream) throws IOException {
    try (stream) {
      CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
      OffsetAttribute offset = stream.addAttribute(OffsetAttribute.class);
      List<Token> tokens = new ArrayList<>();
      stream.reset();
      while (stream.incrementToken()) {
        tokens.add(new Token(term.toString(), offset.startOffset(), offset.endOffset()));
      }
      stream.end();
      return tokens;
    }
  }

  private record Token(String text, int start, int end) {}
}
