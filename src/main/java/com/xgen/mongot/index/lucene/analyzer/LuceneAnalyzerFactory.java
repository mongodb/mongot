package com.xgen.mongot.index.lucene.analyzer;

import com.xgen.mongot.index.analyzer.custom.CharFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.EdgeGramTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.EdgeGramTokenizerDefinition;
import com.xgen.mongot.index.analyzer.custom.LengthTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.LimitTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.NGramTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.NGramTokenizerDefinition;
import com.xgen.mongot.index.analyzer.custom.OriginalTokens;
import com.xgen.mongot.index.analyzer.custom.RegexCaptureGroupTokenizerDefinition;
import com.xgen.mongot.index.analyzer.custom.RegexTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.ShingleTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.StopwordTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.TokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.TokenizerDefinition;
import com.xgen.mongot.index.analyzer.custom.WordDelimiterGraphTokenFilterDefinition;
import java.io.Reader;
import java.util.Collections;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.apache.lucene.analysis.charfilter.MappingCharFilter;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.apache.lucene.analysis.core.FlattenGraphFilter;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.email.UAX29URLEmailTokenizer;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.es.SpanishPluralStemFilter;
import org.apache.lucene.analysis.fa.PersianCharFilter;
import org.apache.lucene.analysis.icu.ICUFoldingFilter;
import org.apache.lucene.analysis.icu.ICUNormalizer2CharFilter;
import org.apache.lucene.analysis.icu.ICUNormalizer2Filter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilter;
import org.apache.lucene.analysis.miscellaneous.RemoveDuplicatesTokenFilter;
import org.apache.lucene.analysis.miscellaneous.TrimFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.pattern.PatternReplaceFilter;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import org.apache.lucene.analysis.phonetic.DaitchMokotoffSoundexFilter;
import org.apache.lucene.analysis.pl.PolishAnalyzer;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.stempel.StempelFilter;
import org.apache.lucene.analysis.stempel.StempelStemmer;

public class LuceneAnalyzerFactory {

  /** Create the lucene analyzer specified by a provided CustomAnalyzerDefinition. */
  public static Analyzer build(CustomAnalyzerSpecification definition) {
    return CustomAnalyzer.builder(TokenizerFactory.build(definition.tokenizer()))
        .charFilters(
            definition.charFilters().orElseGet(Collections::emptyList).stream()
                .map(CharFilterFactory::build)
                .collect(Collectors.toList()))
        .tokenFilters(
            definition.tokenFilters().orElseGet(Collections::emptyList).stream()
                .map(TokenFilterFactory::build)
                .collect(Collectors.toList()))
        .build();
  }

  /** CharFilterFactory */
  public static class CharFilterFactory {
    public static AnalysisStep<Reader> build(CharFilterDefinition definition) {
      return switch (definition.getType()) {
        case HTML_STRIP ->
            reader ->
                new HTMLStripCharFilter(
                    reader, definition.asHtmlCharFilterDefinition().ignoredTags);

        case ICU_NORMALIZE -> reader -> new ICUNormalizer2CharFilter(reader);

        case MAPPING -> {
          NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
          for (var mapping : definition.asMappingCharFilterDefinition().mappings.entrySet()) {
            builder.add(mapping.getKey(), mapping.getValue());
          }
          // It is important to assign the result of builder.build() to a variable before using it
          // in the lambda return (see https://jira.mongodb.com/browse/CLOUDP-75731). Be careful if
          // "cleaning this up"!
          var charMap = builder.build();
          yield reader -> new MappingCharFilter(charMap, reader);
        }

        case PERSIAN -> reader -> new PersianCharFilter(reader);
      };
    }
  }

  /** TokenizerFactory */
  public static class TokenizerFactory {
    /** build */
    public static Supplier<Tokenizer> build(TokenizerDefinition definition) {
      return switch (definition.getType()) {
        case EDGE_GRAM -> {
          EdgeGramTokenizerDefinition edgeGram = definition.asEdgeGramTokenizerDefinition();
          yield () -> new EdgeNGramTokenizer(edgeGram.minGram, edgeGram.maxGram);
        }
        case KEYWORD -> KeywordTokenizer::new;
        case N_GRAM -> {
          NGramTokenizerDefinition ngram = definition.asNGramTokenizerDefinition();
          yield () -> new NGramTokenizer(ngram.minGram, ngram.maxGram);
        }
        case REGEX_CAPTURE_GROUP -> {
          RegexCaptureGroupTokenizerDefinition regexCapture =
              definition.asRegexCaptureGroupTokenizerDefinition();
          yield () -> new PatternTokenizer(regexCapture.pattern, regexCapture.group);
        }
        case REGEX_SPLIT ->
            () -> new PatternTokenizer(definition.asRegexSplitTokenizerDefinition().pattern, -1);
        case STANDARD ->
            () -> {
              StandardTokenizer tokenizer = new StandardTokenizer();
              definition
                  .asStandardTokenizerDefinition()
                  .maxTokenLength
                  .ifPresent(tokenizer::setMaxTokenLength);
              return tokenizer;
            };
        case UAX_URL_EMAIL ->
            () -> {
              UAX29URLEmailTokenizer tokenizer = new UAX29URLEmailTokenizer();
              definition
                  .asUaxUrlEmailTokenizerDefinition()
                  .maxTokenLength
                  .ifPresent(tokenizer::setMaxTokenLength);
              return tokenizer;
            };
        case WHITESPACE ->
            () ->
                definition
                    .asWhitespaceTokenizerDefinition()
                    .maxTokenLength
                    .map(WhitespaceTokenizer::new)
                    .orElseGet(WhitespaceTokenizer::new);
      };
    }
  }

  /** TokenFilterFactory */
  public static class TokenFilterFactory {
    /** build */
    public static AnalysisStep<TokenStream> build(TokenFilterDefinition definition) {
      return switch (definition.getType()) {
        case ASCII_FOLDING -> {
          boolean preserveOriginal =
              definition.asAsciiFoldingTokenFilterDefinition().outputOption
                  == OriginalTokens.INCLUDE;
          yield input -> new ASCIIFoldingFilter(input, preserveOriginal);
        }

        case DAITCH_MOKOTOFF_SOUNDEX -> {
          boolean includeOriginalTokens =
              definition.asDaitchMokotoffSoundexFilterDefinition().outputOption
                  == OriginalTokens.INCLUDE;
          yield input -> new DaitchMokotoffSoundexFilter(input, includeOriginalTokens);
        }

        case EDGE_GRAM -> {
          EdgeGramTokenFilterDefinition edgeGramFilter =
              definition.asEdgeGramTokenFilterDefinition();
          yield input ->
              new EdgeNGramTokenFilter(
                  input,
                  edgeGramFilter.minGram,
                  edgeGramFilter.maxGram,
                  edgeGramFilter.termNotInBounds
                      == EdgeGramTokenFilterDefinition.TermNotInBounds.INCLUDE);
        }

        case ENGLISH_POSSESSIVE -> input -> new EnglishPossessiveFilter(input);

        case FLATTEN_GRAPH -> input -> new FlattenGraphFilter(input);

        case ICU_FOLDING -> input -> new ICUFoldingFilter(input);

        case ICU_NORMALIZER ->
            input ->
                new ICUNormalizer2Filter(
                    input, definition.asIcuNormalizerTokenFilterDefinition().getNormalizer());

        case K_STEMMING -> input -> new KStemFilter(input);

        case KEYWORD_REPEAT -> input -> new KeywordRepeatFilter(input);

        case N_GRAM -> {
          NGramTokenFilterDefinition ngramFilter = definition.asNGramTokenFilterDefinition();
          yield input ->
              new NGramTokenFilter(
                  input,
                  ngramFilter.minGram,
                  ngramFilter.maxGram,
                  ngramFilter.termNotInBounds
                      == NGramTokenFilterDefinition.TermNotInBounds.INCLUDE);
        }

        case PORTER_STEMMING -> input -> new PorterStemFilter(input);

        case LENGTH -> {
          LengthTokenFilterDefinition lengthFilter = definition.asLengthTokenFilterDefinition();
          yield input -> new LengthFilter(input, lengthFilter.min, lengthFilter.max);
        }

        case LOWERCASE -> input -> new LowerCaseFilter(input);

        case LIMIT_TOKEN_COUNT -> {
          LimitTokenFilterDefinition limitFilter =
              definition.asLimitTokenFilterDefinition();
          yield input ->
              new LimitTokenCountFilter(
                  input,
                  limitFilter.getMaxTokenCount(),
                  false);
        }

        case REGEX -> {
          RegexTokenFilterDefinition regexFilter = definition.asRegexTokenFilterDefinition();
          yield input ->
              new PatternReplaceFilter(
                  input,
                  regexFilter.pattern,
                  regexFilter.replacement,
                  regexFilter.matches == RegexTokenFilterDefinition.Matches.ALL);
        }

        case REMOVE_DUPLICATES -> input -> new RemoveDuplicatesTokenFilter(input);

        case SHINGLE -> {
          ShingleTokenFilterDefinition shingleFilter = definition.asShingleTokenFilterDefinition();
          yield input -> {
            ShingleFilter filter =
                new ShingleFilter(
                    input, shingleFilter.minShingleSize, shingleFilter.maxShingleSize);
            filter.setOutputUnigrams(false);
            return filter;
          };
        }

        case SNOWBALL_STEMMING ->
            input ->
                SnowballTokenFilterFactory.build(
                    definition.asSnowballTokenFilterDefinition(), input);

        case SPANISH_PLURAL_STEMMING -> input -> new SpanishPluralStemFilter(input);

        case STEMPEL ->
            input -> new StempelFilter(input, new StempelStemmer(PolishAnalyzer.getDefaultTable()));

        case STOPWORD -> {
          StopwordTokenFilterDefinition stopwordFilter =
              definition.asStopwordTokenFilterDefinition();
          var stopSet =
              StopFilter.makeStopSet(
                  stopwordFilter.tokens.toArray(new String[0]), stopwordFilter.ignoreCase);
          yield input -> new StopFilter(input, stopSet);
        }

        case TRIM -> input -> new TrimFilter(input);

        case REVERSE -> input -> new ReverseStringFilter(input);

        case WORD_DELIMITER_GRAPH -> {
          WordDelimiterGraphTokenFilterDefinition wordDelimiterGraphTokenFilter =
              definition.asWordDelimiterGraphTokenFilterDefinition();
          int configurationFlags =
              wordDelimiterGraphTokenFilter.delimiterOptions.getConfigurationFlags();
          CharArraySet protectedWords =
              new CharArraySet(
                  wordDelimiterGraphTokenFilter.protectedWords.words,
                  wordDelimiterGraphTokenFilter.protectedWords.ignoreCase);
          yield input -> new WordDelimiterGraphFilter(input, configurationFlags, protectedWords);
        }
      };
    }
  }
}
