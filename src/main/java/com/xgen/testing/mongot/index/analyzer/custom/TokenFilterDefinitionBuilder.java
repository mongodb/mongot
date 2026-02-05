package com.xgen.testing.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.custom.AsciiFoldingTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.DaitchMokotoffSoundexTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.EdgeGramTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.EnglishPossessiveTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.FlattenGraphTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.IcuFoldingTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.IcuNormalizerTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.KStemmingTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.LengthTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.LimitTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.LowercaseTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.NGramTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.OriginalTokens;
import com.xgen.mongot.index.analyzer.custom.PorterStemmingTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.RegexTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.ReverseTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.ShingleTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.SnowballStemmingTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.SpanishPluralStemmingTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.StempelTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.StopwordTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.TrimTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.WordDelimiterGraphTokenFilterDefinition;
import com.xgen.mongot.util.Check;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

public class TokenFilterDefinitionBuilder {
  public static class LowercaseTokenFilter {
    public static LowercaseTokenFilter builder() {
      return new LowercaseTokenFilter();
    }

    public LowercaseTokenFilterDefinition build() {
      return new LowercaseTokenFilterDefinition();
    }
  }

  public static class LengthTokenFilter {
    private int min = 0;
    private int max = 255;

    public static LengthTokenFilter builder() {
      return new LengthTokenFilter();
    }

    public LengthTokenFilter min(int min) {
      this.min = min;
      return this;
    }

    public LengthTokenFilter max(int max) {
      this.max = max;
      return this;
    }

    public LengthTokenFilterDefinition build() {
      return new LengthTokenFilterDefinition(this.min, this.max);
    }
  }

  public static class RegexTokenFilter {
    private String pattern;
    private String replacement;
    private Optional<RegexTokenFilterDefinition.Matches> matches;

    public static RegexTokenFilter builder() {
      return new RegexTokenFilter();
    }

    public RegexTokenFilter pattern(String pattern) {
      this.pattern = pattern;
      return this;
    }

    public RegexTokenFilter replacement(String replacement) {
      this.replacement = replacement;
      return this;
    }

    public RegexTokenFilter matches(RegexTokenFilterDefinition.Matches matches) {
      this.matches = Optional.of(matches);
      return this;
    }

    /** Builds a RegexTokenFilterDefinition, throwing an error if matches is not set. */
    public RegexTokenFilterDefinition build() {
      return new RegexTokenFilterDefinition(
          Pattern.compile(this.pattern),
          this.replacement,
          Check.isPresent(this.matches, "matches"));
    }
  }

  public static class IcuNormalizerTokenFilter {
    private IcuNormalizerTokenFilterDefinition.NormalizationForm normalizationForm =
        IcuNormalizerTokenFilterDefinition.NormalizationForm.NFC;

    public static IcuNormalizerTokenFilter builder() {
      return new IcuNormalizerTokenFilter();
    }

    public IcuNormalizerTokenFilter normalizationForm(
        IcuNormalizerTokenFilterDefinition.NormalizationForm normalizationForm) {
      this.normalizationForm = normalizationForm;
      return this;
    }

    public IcuNormalizerTokenFilterDefinition build() {
      return new IcuNormalizerTokenFilterDefinition(this.normalizationForm);
    }
  }

  public static class IcuFoldingTokenFilter {
    public static IcuFoldingTokenFilter builder() {
      return new IcuFoldingTokenFilter();
    }

    public IcuFoldingTokenFilterDefinition build() {
      return new IcuFoldingTokenFilterDefinition();
    }
  }

  public static class KStemmingTokenFilter {
    public static KStemmingTokenFilter builder() {
      return new KStemmingTokenFilter();
    }

    public KStemmingTokenFilterDefinition build() {
      return new KStemmingTokenFilterDefinition();
    }
  }

  public static class NGramTokenFilter {
    private int minGram;
    private int maxGram;
    private NGramTokenFilterDefinition.TermNotInBounds termNotInBounds =
        NGramTokenFilterDefinition.TermNotInBounds.OMIT;

    public static NGramTokenFilter builder() {
      return new NGramTokenFilter();
    }

    public NGramTokenFilter minGram(int minGram) {
      this.minGram = minGram;
      return this;
    }

    public NGramTokenFilter maxGram(int maxGram) {
      this.maxGram = maxGram;
      return this;
    }

    public NGramTokenFilter termNotInBounds(
        NGramTokenFilterDefinition.TermNotInBounds termNotInBounds) {
      this.termNotInBounds = termNotInBounds;
      return this;
    }

    public NGramTokenFilterDefinition build() {
      return new NGramTokenFilterDefinition(this.minGram, this.maxGram, this.termNotInBounds);
    }
  }

  public static class PorterStemmingTokenFilter {
    public static PorterStemmingTokenFilter builder() {
      return new PorterStemmingTokenFilter();
    }

    public PorterStemmingTokenFilterDefinition build() {
      return new PorterStemmingTokenFilterDefinition();
    }
  }

  public static class DaitchMokotoffSoundexTokenFilter {
    private OriginalTokens outputOption = OriginalTokens.INCLUDE;

    public static DaitchMokotoffSoundexTokenFilter builder() {
      return new DaitchMokotoffSoundexTokenFilter();
    }

    public DaitchMokotoffSoundexTokenFilter originalTokens(OriginalTokens outputOption) {
      this.outputOption = outputOption;
      return this;
    }

    public DaitchMokotoffSoundexTokenFilterDefinition build() {
      return new DaitchMokotoffSoundexTokenFilterDefinition(this.outputOption);
    }
  }

  public static class EdgeGramTokenFilter {
    private int minGram;
    private int maxGram;
    private EdgeGramTokenFilterDefinition.TermNotInBounds termNotInBounds =
        EdgeGramTokenFilterDefinition.TermNotInBounds.OMIT;

    public static EdgeGramTokenFilter builder() {
      return new EdgeGramTokenFilter();
    }

    public EdgeGramTokenFilter minGram(int minGram) {
      this.minGram = minGram;
      return this;
    }

    public EdgeGramTokenFilter maxGram(int maxGram) {
      this.maxGram = maxGram;
      return this;
    }

    public EdgeGramTokenFilter termNotInBounds(
        EdgeGramTokenFilterDefinition.TermNotInBounds termNotInBounds) {
      this.termNotInBounds = termNotInBounds;
      return this;
    }

    public EdgeGramTokenFilterDefinition build() {
      return new EdgeGramTokenFilterDefinition(this.minGram, this.maxGram, this.termNotInBounds);
    }
  }

  public static class EnglishPossessiveTokenFilter {
    public static EnglishPossessiveTokenFilter builder() {
      return new EnglishPossessiveTokenFilter();
    }

    public EnglishPossessiveTokenFilterDefinition build() {
      return new EnglishPossessiveTokenFilterDefinition();
    }
  }

  public static class FlattenGraphTokenFilter {
    public static FlattenGraphTokenFilter builder() {
      return new FlattenGraphTokenFilter();
    }

    public FlattenGraphTokenFilterDefinition build() {
      return new FlattenGraphTokenFilterDefinition();
    }
  }

  public static class LimitTokenCountTokenFilter {
    private int maxTokenCount;

    public static LimitTokenCountTokenFilter builder() {
      return new LimitTokenCountTokenFilter();
    }

    public LimitTokenCountTokenFilter maxTokenCount(int maxTokenCount) {
      this.maxTokenCount = maxTokenCount;
      return this;
    }

    public LimitTokenFilterDefinition build() {
      return new LimitTokenFilterDefinition(this.maxTokenCount);
    }
  }

  public static class ShingleTokenFilter {
    private int minShingleSize;
    private int maxShingleSize;

    public static ShingleTokenFilter builder() {
      return new ShingleTokenFilter();
    }

    public ShingleTokenFilter minShingleSize(int minShingleSize) {
      this.minShingleSize = minShingleSize;
      return this;
    }

    public ShingleTokenFilter maxShingleSize(int maxShingleSize) {
      this.maxShingleSize = maxShingleSize;
      return this;
    }

    public ShingleTokenFilterDefinition build() {
      if (this.minShingleSize > this.maxShingleSize) {
        throw new
            IllegalArgumentException("minShingleSize must not be greater than maxShingleSize");
      }
      return new ShingleTokenFilterDefinition(this.minShingleSize, this.maxShingleSize);
    }
  }

  public static class SnowballStemmingTokenFilter {
    private Optional<SnowballStemmingTokenFilterDefinition.StemmerName> stemmerName =
        Optional.empty();

    public static SnowballStemmingTokenFilter builder() {
      return new SnowballStemmingTokenFilter();
    }

    public SnowballStemmingTokenFilter stemmerName(
        SnowballStemmingTokenFilterDefinition.StemmerName stemmerName) {
      this.stemmerName = Optional.of(stemmerName);
      return this;
    }

    /** Create a SnowballStemmingTokenFilterDefinition. */
    public SnowballStemmingTokenFilterDefinition build() {
      return new SnowballStemmingTokenFilterDefinition(
          Check.isPresent(this.stemmerName, "stemmerName"));
    }
  }

  public static class SpanishPluralStemmingTokenFilter {
    public static SpanishPluralStemmingTokenFilter builder() {
      return new SpanishPluralStemmingTokenFilter();
    }

    public SpanishPluralStemmingTokenFilterDefinition build() {
      return new SpanishPluralStemmingTokenFilterDefinition();
    }
  }

  public static class StempelTokenFilter {
    public static StempelTokenFilter builder() {
      return new StempelTokenFilter();
    }

    public StempelTokenFilterDefinition build() {
      return new StempelTokenFilterDefinition();
    }
  }

  public static class StopwordTokenFilter {
    private Optional<List<String>> tokens = Optional.empty();
    private boolean ignoreCase = true;

    public static StopwordTokenFilter builder() {
      return new StopwordTokenFilter();
    }

    public StopwordTokenFilter tokens(List<String> tokens) {
      this.tokens = Optional.of(tokens);
      return this;
    }

    public StopwordTokenFilter ignoreCase(boolean ignoreCase) {
      this.ignoreCase = ignoreCase;
      return this;
    }

    /** Builds a StopwordTokenFilterDefinition. */
    public StopwordTokenFilterDefinition build() {
      Check.argNotEmpty(Check.isPresent(this.tokens, "tokens"), "tokens");

      return new StopwordTokenFilterDefinition(this.tokens.get(), this.ignoreCase);
    }
  }

  public static class ReverseTokenFilterBuilder {
    public static ReverseTokenFilterBuilder builder() {
      return new ReverseTokenFilterBuilder();
    }

    public ReverseTokenFilterDefinition build() {
      return new ReverseTokenFilterDefinition();
    }
  }

  public static class TrimTokenFilter {
    public static TrimTokenFilter builder() {
      return new TrimTokenFilter();
    }

    public TrimTokenFilterDefinition build() {
      return new TrimTokenFilterDefinition();
    }
  }

  public static class AsciiFoldingTokenFilter {
    private OriginalTokens outputOption = OriginalTokens.OMIT;

    public static AsciiFoldingTokenFilter builder() {
      return new AsciiFoldingTokenFilter();
    }

    public AsciiFoldingTokenFilter originalTokens(OriginalTokens outputOption) {
      this.outputOption = outputOption;
      return this;
    }

    public AsciiFoldingTokenFilterDefinition build() {
      return new AsciiFoldingTokenFilterDefinition(this.outputOption);
    }
  }

  public static class WordDelimiterGraphTokenFilter {

    public static class ProtectedWords {
      private List<String> words = new ArrayList<>();
      private boolean ignoreCase = true;

      public static ProtectedWords builder() {
        return new ProtectedWords();
      }

      public WordDelimiterGraphTokenFilterDefinition.ProtectedWords build() {
        return new WordDelimiterGraphTokenFilterDefinition.ProtectedWords(
            this.words, this.ignoreCase);
      }

      public ProtectedWords words(List<String> words) {
        if (!this.words.isEmpty()) {
          throw new AssertionError(
              "words being set and overriding existing words; "
                  + "use words() once or use word() to incrementally add words to protectedWords");
        }
        this.words = words;
        return this;
      }

      public ProtectedWords addWord(String word) {
        this.words.add(word);
        return this;
      }

      public ProtectedWords ignoreCase(boolean ignoreCase) {
        this.ignoreCase = ignoreCase;
        return this;
      }
    }

    public static class DelimiterOptions {
      private boolean generateWordParts = true;
      private boolean generateNumberParts = true;
      private boolean concatenateWords = false;
      private boolean concatenateNumbers = false;
      private boolean concatenateAll = false;
      private boolean preserveOriginal = false;
      private boolean splitOnCaseChange = true;
      private boolean splitOnNumerics = true;
      private boolean stemEnglishPossessive = true;
      private boolean ignoreKeywords = false;

      public static DelimiterOptions builder() {
        return new DelimiterOptions();
      }

      public static DelimiterOptions allFalse() {
        return builder()
            .generateWordParts(false)
            .generateNumberParts(false)
            .concatenateWords(false)
            .concatenateNumbers(false)
            .concatenateAll(false)
            .preserveOriginal(false)
            .splitOnCaseChange(false)
            .splitOnNumerics(false)
            .stemEnglishPossessive(false)
            .ignoreKeywords(false);
      }

      public WordDelimiterGraphTokenFilterDefinition.DelimiterOptions build() {
        return new WordDelimiterGraphTokenFilterDefinition.DelimiterOptions(
            this.generateWordParts,
            this.generateNumberParts,
            this.concatenateWords,
            this.concatenateNumbers,
            this.concatenateAll,
            this.preserveOriginal,
            this.splitOnCaseChange,
            this.splitOnNumerics,
            this.stemEnglishPossessive,
            this.ignoreKeywords);
      }

      public DelimiterOptions generateWordParts(boolean generateWordParts) {
        this.generateWordParts = generateWordParts;
        return this;
      }

      public DelimiterOptions generateNumberParts(boolean generateNumberParts) {
        this.generateNumberParts = generateNumberParts;
        return this;
      }

      public DelimiterOptions concatenateWords(boolean concatenateWords) {
        this.concatenateWords = concatenateWords;
        return this;
      }

      public DelimiterOptions concatenateNumbers(boolean concatenateNumbers) {
        this.concatenateNumbers = concatenateNumbers;
        return this;
      }

      public DelimiterOptions concatenateAll(boolean concatenateAll) {
        this.concatenateAll = concatenateAll;
        return this;
      }

      public DelimiterOptions preserveOriginal(boolean preserveOriginal) {
        this.preserveOriginal = preserveOriginal;
        return this;
      }

      public DelimiterOptions splitOnCaseChange(boolean splitOnCaseChange) {
        this.splitOnCaseChange = splitOnCaseChange;
        return this;
      }

      public DelimiterOptions splitOnNumerics(boolean splitOnNumerics) {
        this.splitOnNumerics = splitOnNumerics;
        return this;
      }

      public DelimiterOptions stemEnglishPossessive(boolean stemEnglishPossessive) {
        this.stemEnglishPossessive = stemEnglishPossessive;
        return this;
      }

      public DelimiterOptions ignoreKeywords(boolean ignoreKeywords) {
        this.ignoreKeywords = ignoreKeywords;
        return this;
      }
    }

    private WordDelimiterGraphTokenFilterDefinition.ProtectedWords protectedWords =
        ProtectedWords.builder().build();
    private WordDelimiterGraphTokenFilterDefinition.DelimiterOptions delimiterOptions =
        DelimiterOptions.builder().build();

    public static WordDelimiterGraphTokenFilter builder() {
      return new WordDelimiterGraphTokenFilter();
    }

    public WordDelimiterGraphTokenFilter protectedWords(
        WordDelimiterGraphTokenFilterDefinition.ProtectedWords protectedWords) {
      this.protectedWords = protectedWords;
      return this;
    }

    public WordDelimiterGraphTokenFilter delimiterOptions(
        WordDelimiterGraphTokenFilterDefinition.DelimiterOptions delimiterOptions) {
      this.delimiterOptions = delimiterOptions;
      return this;
    }

    public WordDelimiterGraphTokenFilterDefinition build() {
      return new WordDelimiterGraphTokenFilterDefinition(
          this.protectedWords, this.delimiterOptions);
    }
  }
}
