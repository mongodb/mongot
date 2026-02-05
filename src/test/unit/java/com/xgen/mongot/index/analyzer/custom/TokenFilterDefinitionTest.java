package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public class TokenFilterDefinitionTest {

  @RunWith(Parameterized.class)
  public static class TestDeserialization {
    private static final String SUITE_NAME = "token-filter-deserialization";
    private static final BsonDeserializationTestSuite<TokenFilterDefinition> TEST_SUITE =
        BsonDeserializationTestSuite.fromDocument(
            "src/test/unit/resources/index/analyzer/custom",
            SUITE_NAME,
            TokenFilterDefinition::fromBson);

    @Parameterized.Parameter
    public BsonDeserializationTestSuite.TestSpecWrapper<TokenFilterDefinition> testSpec;

    /** Test data. */
    @Parameterized.Parameters(name = "deserialize: {0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<TokenFilterDefinition>>
        data() {
      return TEST_SUITE.withExamples(
          keywordRepeat(),
          removeDuplicates(),
          lowercase(),
          length(),
          lengthMinMaxEqual(),
          lengthDefaultMinAndMax(),
          limitTokenCount(),
          regexAll(),
          regexFirst(),
          regexEmptyPattern(),
          regexEmptyReplacement(),
          icuFolding(),
          icuNormalizer(),
          icuNormalizerNullForm(),
          icuNormalizerNfc(),
          icuNormalizerNfd(),
          icuNormalizerNfkc(),
          icuNormalizerNfkd(),
          kStemming(),
          ngram(),
          porterStemmingFilter(),
          ngramMinMaxEqual(),
          ngramInclude(),
          edgeGram(),
          edgeGramMinMaxEqual(),
          edgeGramInclude(),
          englishPossessive(),
          flattenGraph(),
          shingle(),
          shingleMinMaxEqual(),
          stempel(),
          stopwordIgnoreCase(),
          stopwordSimple(),
          arabicSnowballFilter(),
          armenianSnowballFilter(),
          basqueSnowballFilter(),
          catalanSnowballFilter(),
          danishSnowballFilter(),
          dutchSnowballFilter(),
          englishSnowballFilter(),
          estonianSnowballFilter(),
          finnishSnowballFilter(),
          frenchSnowballFilter(),
          germanSnowballFilter(),
          german2SnowballFilter(),
          hungarianSnowballFilter(),
          irishSnowballFilter(),
          italianSnowballFilter(),
          lithuanianSnowballFilter(),
          norwegianSnowballFilter(),
          porterSnowballFilter(),
          portugueseSnowballFilter(),
          romanianSnowballFilter(),
          russianSnowballFilter(),
          spanishSnowballFilter(),
          swedishSnowballFilter(),
          turkishSnowballFilter(),
          spanishPluralStemmingFilter(),
          daitchMokotoffSoundexFilter(),
          trimFilter(),
          asciiFoldingFilter(),
          wordDelimiterGraph(),
          wordDelimiterGraphDefaults());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> keywordRepeat() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "keywordRepeat", new KeywordRepeatTokenFilterDefinition());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        removeDuplicates() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "removeDuplicates", new RemoveDuplicatesTokenFilterDefinition());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> lowercase() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "lowercase", TokenFilterDefinitionBuilder.LowercaseTokenFilter.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> length() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "length",
          TokenFilterDefinitionBuilder.LengthTokenFilter.builder().min(2).max(12).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        lengthMinMaxEqual() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "length min max equal",
          TokenFilterDefinitionBuilder.LengthTokenFilter.builder().min(2).max(2).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        lengthDefaultMinAndMax() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "length default min and max",
          TokenFilterDefinitionBuilder.LengthTokenFilter.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> limitTokenCount() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "limitTokenCount",
          TokenFilterDefinitionBuilder.LimitTokenCountTokenFilter.builder()
              .maxTokenCount(10)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> regexAll() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "regex all",
          TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
              .pattern("a")
              .replacement("b")
              .matches(RegexTokenFilterDefinition.Matches.ALL)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> regexFirst() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "regex first",
          TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
              .pattern("a")
              .replacement("b")
              .matches(RegexTokenFilterDefinition.Matches.FIRST)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        regexEmptyPattern() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "regex empty pattern",
          TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
              .pattern("")
              .replacement("b")
              .matches(RegexTokenFilterDefinition.Matches.ALL)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        regexEmptyReplacement() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "regex empty replacement",
          TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
              .pattern("a")
              .replacement("")
              .matches(RegexTokenFilterDefinition.Matches.ALL)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> icuFolding() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "icu folding", TokenFilterDefinitionBuilder.IcuFoldingTokenFilter.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> icuNormalizer() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "icu normalizer",
          TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder()
              .normalizationForm(IcuNormalizerTokenFilterDefinition.NormalizationForm.NFC)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        icuNormalizerNullForm() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "icu normalizer null form",
          TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder()
              .normalizationForm(IcuNormalizerTokenFilterDefinition.NormalizationForm.NFC)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        icuNormalizerNfc() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "icu normalizer nfc",
          TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder()
              .normalizationForm(IcuNormalizerTokenFilterDefinition.NormalizationForm.NFC)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        icuNormalizerNfd() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "icu normalizer nfd",
          TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder()
              .normalizationForm(IcuNormalizerTokenFilterDefinition.NormalizationForm.NFD)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        icuNormalizerNfkc() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "icu normalizer nfkc",
          TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder()
              .normalizationForm(IcuNormalizerTokenFilterDefinition.NormalizationForm.NFKC)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        icuNormalizerNfkd() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "icu normalizer nfkd",
          TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder()
              .normalizationForm(IcuNormalizerTokenFilterDefinition.NormalizationForm.NFKD)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> kStemming() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "kStemming", TokenFilterDefinitionBuilder.KStemmingTokenFilter.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> ngram() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "nGram",
          TokenFilterDefinitionBuilder.NGramTokenFilter.builder().minGram(1).maxGram(3).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        ngramMinMaxEqual() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "nGram min max equal",
          TokenFilterDefinitionBuilder.NGramTokenFilter.builder().minGram(1).maxGram(1).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> ngramInclude() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "nGram include",
          TokenFilterDefinitionBuilder.NGramTokenFilter.builder()
              .minGram(1)
              .maxGram(3)
              .termNotInBounds(NGramTokenFilterDefinition.TermNotInBounds.INCLUDE)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        porterStemmingFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "porterStemmingFilter",
          TokenFilterDefinitionBuilder.PorterStemmingTokenFilter.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> edgeGram() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "edgeGram",
          TokenFilterDefinitionBuilder.EdgeGramTokenFilter.builder().minGram(1).maxGram(3).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        edgeGramMinMaxEqual() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "edgeGram min max equal",
          TokenFilterDefinitionBuilder.EdgeGramTokenFilter.builder().minGram(1).maxGram(1).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> edgeGramInclude() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "edgeGram include",
          TokenFilterDefinitionBuilder.EdgeGramTokenFilter.builder()
              .minGram(1)
              .maxGram(3)
              .termNotInBounds(EdgeGramTokenFilterDefinition.TermNotInBounds.INCLUDE)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        englishPossessive() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "englishPossessive",
          TokenFilterDefinitionBuilder.EnglishPossessiveTokenFilter.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> flattenGraph() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "flattenGraph", TokenFilterDefinitionBuilder.FlattenGraphTokenFilter.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> shingle() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "shingle",
          TokenFilterDefinitionBuilder.ShingleTokenFilter.builder()
              .minShingleSize(2)
              .maxShingleSize(3)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        stopwordIgnoreCase() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "stopwordIgnoreCase",
          TokenFilterDefinitionBuilder.StopwordTokenFilter.builder()
              .tokens(List.of("is", "the"))
              .ignoreCase(false)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> stopwordSimple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "stopwordSimple",
          TokenFilterDefinitionBuilder.StopwordTokenFilter.builder()
              .tokens(List.of("is", "the"))
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        arabicSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "arabicSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.ARABIC)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        armenianSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "armenianSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.ARMENIAN)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        basqueSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "basqueSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.BASQUE)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        catalanSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "catalanSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.CATALAN)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        danishSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "danishSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.DANISH)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        dutchSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "dutchSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.DUTCH)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        englishSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "englishSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.ENGLISH)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        estonianSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "estonianSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.ESTONIAN)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        finnishSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "finnishSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.FINNISH)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        frenchSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "frenchSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.FRENCH)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        germanSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "germanSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.GERMAN)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        german2SnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "german2Snowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.GERMAN2)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        hungarianSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "hungarianSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.HUNGARIAN)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        irishSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "irishSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.IRISH)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        italianSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "italianSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.ITALIAN)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        lithuanianSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "lithuanianSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.LITHUANIAN)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        shingleMinMaxEqual() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "shingle min max equal",
          TokenFilterDefinitionBuilder.ShingleTokenFilter.builder()
              .minShingleSize(2)
              .maxShingleSize(2)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> stempel() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "stempel", TokenFilterDefinitionBuilder.StempelTokenFilter.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        norwegianSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "norwegianSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.NORWEGIAN)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        porterSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "porterSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.PORTER)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        portugueseSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "portugueseSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.PORTUGUESE)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        romanianSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "romanianSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.ROMANIAN)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        russianSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "russianSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.RUSSIAN)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        spanishSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "spanishSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.SPANISH)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        swedishSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "swedishSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.SWEDISH)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        turkishSnowballFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "turkishSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.TURKISH)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        spanishPluralStemmingFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "spanishPluralStemmingFilter",
          TokenFilterDefinitionBuilder.SpanishPluralStemmingTokenFilter.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        daitchMokotoffSoundexFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "daitchMokotoffSoundex",
          TokenFilterDefinitionBuilder.DaitchMokotoffSoundexTokenFilter.builder()
              .originalTokens(OriginalTokens.OMIT)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition> trimFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "trimFilter", TokenFilterDefinitionBuilder.TrimTokenFilter.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        asciiFoldingFilter() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "asciiFolding",
          TokenFilterDefinitionBuilder.AsciiFoldingTokenFilter.builder()
              .originalTokens(OriginalTokens.OMIT)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        wordDelimiterGraph() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "wordDelimiterGraph",
          TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.builder()
              .delimiterOptions(
                  TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.DelimiterOptions
                      .builder()
                      .generateNumberParts(true)
                      .build())
              .protectedWords(
                  TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.ProtectedWords
                      .builder()
                      .words(List.of("is", "the"))
                      .ignoreCase(false)
                      .build())
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFilterDefinition>
        wordDelimiterGraphDefaults() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "wordDelimiterGraph with default values",
          TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.builder().build());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {
    private static final String SUITE_NAME = "token-filter-serialization";
    private static final BsonSerializationTestSuite<TokenFilterDefinition> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable(
            "src/test/unit/resources/index/analyzer/custom", SUITE_NAME);

    @Parameterized.Parameter
    public BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> testSpec;

    /** Test data. */
    @Parameterized.Parameters(name = "serialize: {0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<TokenFilterDefinition>> data() {
      return Arrays.asList(
          asciiFolding(),
          daitchMokotoffSoundex(),
          lowercase(),
          length(),
          lengthSerializesDefaults(),
          lengthOnlyMin(),
          lengthOnlyMax(),
          limitTokenCount(),
          regexAll(),
          regexFirst(),
          icuFolding(),
          icuNormalizerNfc(),
          icuNormalizerNfd(),
          icuNormalizerNfkc(),
          icuNormalizerNfkd(),
          kStemming(),
          ngram(),
          ngramInclude(),
          porterStemming(),
          edgeGram(),
          edgeGramInclude(),
          englishPossessive(),
          flattenGraph(),
          shingle(),
          spanishPluralStemming(),
          stempel(),
          stopword(),
          arabicSnowball(),
          armenianSnowball(),
          basqueSnowball(),
          catalanSnowball(),
          danishSnowball(),
          dutchSnowball(),
          englishSnowball(),
          estonianSnowball(),
          finnishSnowball(),
          frenchSnowball(),
          germanSnowball(),
          german2Snowball(),
          hungarianSnowball(),
          irishSnowball(),
          italianSnowball(),
          lithuanianSnowball(),
          norwegianSnowball(),
          porterSnowball(),
          portugueseSnowball(),
          romanianSnowball(),
          russianSnowball(),
          spanishSnowball(),
          swedishSnowball(),
          turkishSnowball(),
          trim(),
          wordDelimiterGraph(),
          wordDelimiterGraphDefaults());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> lowercase() {
      return BsonSerializationTestSuite.TestSpec.create(
          "lowercase", TokenFilterDefinitionBuilder.LowercaseTokenFilter.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> length() {
      return BsonSerializationTestSuite.TestSpec.create(
          "length",
          TokenFilterDefinitionBuilder.LengthTokenFilter.builder().min(3).max(10).build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition>
        lengthSerializesDefaults() {
      return BsonSerializationTestSuite.TestSpec.create(
          "length serializes defaults",
          TokenFilterDefinitionBuilder.LengthTokenFilter.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> lengthOnlyMin() {
      return BsonSerializationTestSuite.TestSpec.create(
          "length only min",
          TokenFilterDefinitionBuilder.LengthTokenFilter.builder().min(3).build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> lengthOnlyMax() {
      return BsonSerializationTestSuite.TestSpec.create(
          "length only max",
          TokenFilterDefinitionBuilder.LengthTokenFilter.builder().max(10).build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> limitTokenCount() {
      return BsonSerializationTestSuite.TestSpec.create(
          "limitTokenCount",
          TokenFilterDefinitionBuilder.LimitTokenCountTokenFilter.builder()
              .maxTokenCount(10)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> regexAll() {
      return BsonSerializationTestSuite.TestSpec.create(
          "regex all",
          TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
              .pattern("a")
              .replacement("b")
              .matches(RegexTokenFilterDefinition.Matches.ALL)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> regexFirst() {
      return BsonSerializationTestSuite.TestSpec.create(
          "regex first",
          TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
              .pattern("a")
              .replacement("b")
              .matches(RegexTokenFilterDefinition.Matches.FIRST)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> icuFolding() {
      return BsonSerializationTestSuite.TestSpec.create(
          "icu folding", TokenFilterDefinitionBuilder.IcuFoldingTokenFilter.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> icuNormalizerNfc() {
      return BsonSerializationTestSuite.TestSpec.create(
          "icu normalizer nfc",
          TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder()
              .normalizationForm(IcuNormalizerTokenFilterDefinition.NormalizationForm.NFC)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> icuNormalizerNfd() {
      return BsonSerializationTestSuite.TestSpec.create(
          "icu normalizer nfd",
          TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder()
              .normalizationForm(IcuNormalizerTokenFilterDefinition.NormalizationForm.NFD)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> icuNormalizerNfkc() {
      return BsonSerializationTestSuite.TestSpec.create(
          "icu normalizer nfkc",
          TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder()
              .normalizationForm(IcuNormalizerTokenFilterDefinition.NormalizationForm.NFKC)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> icuNormalizerNfkd() {
      return BsonSerializationTestSuite.TestSpec.create(
          "icu normalizer nfkd",
          TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder()
              .normalizationForm(IcuNormalizerTokenFilterDefinition.NormalizationForm.NFKD)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> kStemming() {
      return BsonSerializationTestSuite.TestSpec.create(
          "kStemming", TokenFilterDefinitionBuilder.KStemmingTokenFilter.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> ngram() {
      return BsonSerializationTestSuite.TestSpec.create(
          "ngram",
          TokenFilterDefinitionBuilder.NGramTokenFilter.builder().minGram(1).maxGram(3).build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> ngramInclude() {
      return BsonSerializationTestSuite.TestSpec.create(
          "ngram include",
          TokenFilterDefinitionBuilder.NGramTokenFilter.builder()
              .minGram(1)
              .maxGram(3)
              .termNotInBounds(NGramTokenFilterDefinition.TermNotInBounds.INCLUDE)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> porterStemming() {
      return BsonSerializationTestSuite.TestSpec.create(
          "porterStemming",
          TokenFilterDefinitionBuilder.PorterStemmingTokenFilter.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> edgeGram() {
      return BsonSerializationTestSuite.TestSpec.create(
          "edgeGram",
          TokenFilterDefinitionBuilder.EdgeGramTokenFilter.builder().minGram(1).maxGram(3).build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> edgeGramInclude() {
      return BsonSerializationTestSuite.TestSpec.create(
          "edgeGram include",
          TokenFilterDefinitionBuilder.EdgeGramTokenFilter.builder()
              .minGram(1)
              .maxGram(3)
              .termNotInBounds(EdgeGramTokenFilterDefinition.TermNotInBounds.INCLUDE)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> englishPossessive() {
      return BsonSerializationTestSuite.TestSpec.create(
          "englishPossessive",
          TokenFilterDefinitionBuilder.EnglishPossessiveTokenFilter.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> flattenGraph() {
      return BsonSerializationTestSuite.TestSpec.create(
          "flattenGraph", TokenFilterDefinitionBuilder.FlattenGraphTokenFilter.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> shingle() {
      return BsonSerializationTestSuite.TestSpec.create(
          "shingle",
          TokenFilterDefinitionBuilder.ShingleTokenFilter.builder()
              .minShingleSize(2)
              .maxShingleSize(3)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition>
        spanishPluralStemming() {
      return BsonSerializationTestSuite.TestSpec.create(
          "spanishPluralStemming",
          TokenFilterDefinitionBuilder.SpanishPluralStemmingTokenFilter.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> stempel() {
      return BsonSerializationTestSuite.TestSpec.create(
          "stempel", TokenFilterDefinitionBuilder.StempelTokenFilter.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> stopword() {
      return BsonSerializationTestSuite.TestSpec.create(
          "stopword",
          TokenFilterDefinitionBuilder.StopwordTokenFilter.builder()
              .tokens(List.of("is", "the"))
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> arabicSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "arabicSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.ARABIC)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> armenianSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "armenianSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.ARMENIAN)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> basqueSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "basqueSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.BASQUE)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> catalanSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "catalanSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.CATALAN)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> danishSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "danishSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.DANISH)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> dutchSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "dutchSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.DUTCH)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> englishSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "englishSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.ENGLISH)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> estonianSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "estonianSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.ESTONIAN)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> finnishSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "finnishSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.FINNISH)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> frenchSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "frenchSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.FRENCH)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> germanSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "germanSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.GERMAN)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> german2Snowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "german2Snowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.GERMAN2)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> hungarianSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "hungarianSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.HUNGARIAN)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> irishSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "irishSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.IRISH)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> italianSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "italianSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.ITALIAN)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> lithuanianSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "lithuanianSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.LITHUANIAN)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> norwegianSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "norwegianSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.NORWEGIAN)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> porterSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "porterSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.PORTER)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> portugueseSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "portugueseSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.PORTUGUESE)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> romanianSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "romanianSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.ROMANIAN)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> russianSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "russianSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.RUSSIAN)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> spanishSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "spanishSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.SPANISH)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> swedishSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "swedishSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.SWEDISH)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> turkishSnowball() {
      return BsonSerializationTestSuite.TestSpec.create(
          "turkishSnowball",
          TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
              .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.TURKISH)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> trim() {
      return BsonSerializationTestSuite.TestSpec.create(
          "trim", TokenFilterDefinitionBuilder.TrimTokenFilter.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> wordDelimiterGraph() {
      return BsonSerializationTestSuite.TestSpec.create(
          "wordDelimiterGraph",
          TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.builder()
              .delimiterOptions(
                  TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.DelimiterOptions
                      .builder()
                      .generateWordParts(true)
                      .build())
              .protectedWords(
                  TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.ProtectedWords
                      .builder()
                      .words(List.of("is", "the"))
                      .ignoreCase(true)
                      .build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition>
        wordDelimiterGraphDefaults() {
      return BsonSerializationTestSuite.TestSpec.create(
          "wordDelimiterGraph with default values",
          TokenFilterDefinitionBuilder.WordDelimiterGraphTokenFilter.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition>
        daitchMokotoffSoundex() {
      return BsonSerializationTestSuite.TestSpec.create(
          "daitchMokotoffSoundex",
          TokenFilterDefinitionBuilder.DaitchMokotoffSoundexTokenFilter.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFilterDefinition> asciiFolding() {
      return BsonSerializationTestSuite.TestSpec.create(
          "asciiFolding", TokenFilterDefinitionBuilder.AsciiFoldingTokenFilter.builder().build());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }
  }

  public static class TestDefinition {
    @Test
    public void testEquals() {
      TestUtils.assertEqualityGroups(
          () -> TokenFilterDefinitionBuilder.LengthTokenFilter.builder().build(),
          () -> TokenFilterDefinitionBuilder.LengthTokenFilter.builder().min(5).build(),
          () -> TokenFilterDefinitionBuilder.LengthTokenFilter.builder().max(5).build(),
          () -> TokenFilterDefinitionBuilder.LowercaseTokenFilter.builder().build(),
          () ->
              TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
                  .pattern("a")
                  .replacement("b")
                  .matches(RegexTokenFilterDefinition.Matches.ALL)
                  .build(),
          () -> TokenFilterDefinitionBuilder.FlattenGraphTokenFilter.builder().build(),
          () -> TokenFilterDefinitionBuilder.IcuFoldingTokenFilter.builder().build(),
          () -> TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder().build(),
          () ->
              TokenFilterDefinitionBuilder.IcuNormalizerTokenFilter.builder()
                  .normalizationForm(IcuNormalizerTokenFilterDefinition.NormalizationForm.NFD)
                  .build(),
          () ->
              TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
                  .pattern("c")
                  .replacement("b")
                  .matches(RegexTokenFilterDefinition.Matches.ALL)
                  .build(),
          () ->
              TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
                  .pattern("a")
                  .replacement("c")
                  .matches(RegexTokenFilterDefinition.Matches.ALL)
                  .build(),
          () ->
              TokenFilterDefinitionBuilder.RegexTokenFilter.builder()
                  .pattern("a")
                  .replacement("b")
                  .matches(RegexTokenFilterDefinition.Matches.FIRST)
                  .build(),
          () ->
              TokenFilterDefinitionBuilder.NGramTokenFilter.builder().minGram(1).maxGram(3).build(),
          () ->
              TokenFilterDefinitionBuilder.NGramTokenFilter.builder().minGram(2).maxGram(3).build(),
          () ->
              TokenFilterDefinitionBuilder.NGramTokenFilter.builder().minGram(1).maxGram(2).build(),
          () ->
              TokenFilterDefinitionBuilder.NGramTokenFilter.builder()
                  .minGram(1)
                  .maxGram(3)
                  .termNotInBounds(NGramTokenFilterDefinition.TermNotInBounds.INCLUDE)
                  .build(),
          () ->
              TokenFilterDefinitionBuilder.EdgeGramTokenFilter.builder()
                  .minGram(1)
                  .maxGram(3)
                  .build(),
          () ->
              TokenFilterDefinitionBuilder.EdgeGramTokenFilter.builder()
                  .minGram(2)
                  .maxGram(3)
                  .build(),
          () ->
              TokenFilterDefinitionBuilder.EdgeGramTokenFilter.builder()
                  .minGram(1)
                  .maxGram(2)
                  .build(),
          () ->
              TokenFilterDefinitionBuilder.EdgeGramTokenFilter.builder()
                  .minGram(1)
                  .maxGram(3)
                  .termNotInBounds(EdgeGramTokenFilterDefinition.TermNotInBounds.INCLUDE)
                  .build(),
          () ->
              TokenFilterDefinitionBuilder.ShingleTokenFilter.builder()
                  .minShingleSize(2)
                  .maxShingleSize(4)
                  .build(),
          () ->
              TokenFilterDefinitionBuilder.ShingleTokenFilter.builder()
                  .minShingleSize(3)
                  .maxShingleSize(4)
                  .build(),
          () ->
              TokenFilterDefinitionBuilder.ShingleTokenFilter.builder()
                  .minShingleSize(2)
                  .maxShingleSize(3)
                  .build(),
          () ->
              TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
                  .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.ENGLISH)
                  .build());
    }

    @Test
    public void testSnowballNotEqual() throws Exception {
      Function<
              TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter,
              CheckedSupplier<SnowballStemmingTokenFilterDefinition, Exception>>
          supplierFunction = (def) -> def::build;

      TestUtils.assertEqualityGroups(
          Arrays.stream(SnowballStemmingTokenFilterDefinition.StemmerName.values())
              .map(
                  stemmer ->
                      TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
                          .stemmerName(stemmer))
              .map(supplierFunction)
              .collect(Collectors.toList()));
    }
  }
}
