package com.xgen.mongot.index.analyzer;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.bn.BengaliAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.ckb.SoraniAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lt.LithuanianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pl.PolishAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;

class LuceneAnalyzerProviders {

  private static final ProviderCreator<ArabicAnalyzer> ARABIC_ANALYZER_CREATOR =
      ProviderCreator.of(ArabicAnalyzer::new, ArabicAnalyzer::new, ArabicAnalyzer::new);
  private static final ProviderCreator<ArmenianAnalyzer> ARMENIAN_ANALYZER_CREATOR =
      ProviderCreator.of(ArmenianAnalyzer::new, ArmenianAnalyzer::new, ArmenianAnalyzer::new);
  private static final ProviderCreator<BasqueAnalyzer> BASQUE_ANALYZER_CREATOR =
      ProviderCreator.of(BasqueAnalyzer::new, BasqueAnalyzer::new, BasqueAnalyzer::new);
  private static final ProviderCreator<BengaliAnalyzer> BENGALI_ANALYZER_CREATOR =
      ProviderCreator.of(BengaliAnalyzer::new, BengaliAnalyzer::new, BengaliAnalyzer::new);
  private static final ProviderCreator<BrazilianAnalyzer> BRAZILIAN_ANALYZER_CREATOR =
      ProviderCreator.of(BrazilianAnalyzer::new, BrazilianAnalyzer::new, BrazilianAnalyzer::new);
  private static final ProviderCreator<BulgarianAnalyzer> BULGARIAN_ANALYZER_CREATOR =
      ProviderCreator.of(BulgarianAnalyzer::new, BulgarianAnalyzer::new, BulgarianAnalyzer::new);
  private static final ProviderCreator<CatalanAnalyzer> CATALAN_ANALYZER_CREATOR =
      ProviderCreator.of(CatalanAnalyzer::new, CatalanAnalyzer::new, CatalanAnalyzer::new);
  private static final ProviderCreator<CzechAnalyzer> CZECH_ANALYZER_CREATOR =
      ProviderCreator.of(CzechAnalyzer::new, CzechAnalyzer::new, CzechAnalyzer::new);
  private static final ProviderCreator<DanishAnalyzer> DANISH_ANALYZER_CREATOR =
      ProviderCreator.of(DanishAnalyzer::new, DanishAnalyzer::new, DanishAnalyzer::new);
  private static final ProviderCreator<DutchAnalyzer> DUTCH_ANALYZER_CREATOR =
      ProviderCreator.of(DutchAnalyzer::new, DutchAnalyzer::new, DutchAnalyzer::new);
  private static final ProviderCreator<EnglishAnalyzer> ENGLISH_ANALYZER_CREATOR =
      ProviderCreator.of(EnglishAnalyzer::new, EnglishAnalyzer::new, EnglishAnalyzer::new);
  private static final ProviderCreator<FinnishAnalyzer> FINNISH_ANALYZER_CREATOR =
      ProviderCreator.of(FinnishAnalyzer::new, FinnishAnalyzer::new, FinnishAnalyzer::new);
  private static final ProviderCreator<FrenchAnalyzer> FRENCH_ANALYZER_CREATOR =
      ProviderCreator.of(FrenchAnalyzer::new, FrenchAnalyzer::new, FrenchAnalyzer::new);
  private static final ProviderCreator<GalicianAnalyzer> GALICIAN_ANALYZER_CREATOR =
      ProviderCreator.of(GalicianAnalyzer::new, GalicianAnalyzer::new, GalicianAnalyzer::new);
  private static final ProviderCreator<GermanAnalyzer> GERMAN_ANALYZER_CREATOR =
      ProviderCreator.of(GermanAnalyzer::new, GermanAnalyzer::new, GermanAnalyzer::new);
  private static final ProviderCreator<HindiAnalyzer> HINDI_ANALYZER_CREATOR =
      ProviderCreator.of(HindiAnalyzer::new, HindiAnalyzer::new, HindiAnalyzer::new);
  private static final ProviderCreator<HungarianAnalyzer> HUNGARIAN_ANALYZER_CREATOR =
      ProviderCreator.of(HungarianAnalyzer::new, HungarianAnalyzer::new, HungarianAnalyzer::new);
  private static final ProviderCreator<IndonesianAnalyzer> INDONESIAN_ANALYZER_CREATOR =
      ProviderCreator.of(IndonesianAnalyzer::new, IndonesianAnalyzer::new, IndonesianAnalyzer::new);
  private static final ProviderCreator<IrishAnalyzer> IRISH_ANALYZER_CREATOR =
      ProviderCreator.of(IrishAnalyzer::new, IrishAnalyzer::new, IrishAnalyzer::new);
  private static final ProviderCreator<ItalianAnalyzer> ITALIAN_ANALYZER_CREATOR =
      ProviderCreator.of(ItalianAnalyzer::new, ItalianAnalyzer::new, ItalianAnalyzer::new);
  private static final ProviderCreator<LatvianAnalyzer> LATVIAN_ANALYZER_CREATOR =
      ProviderCreator.of(LatvianAnalyzer::new, LatvianAnalyzer::new, LatvianAnalyzer::new);
  private static final ProviderCreator<LithuanianAnalyzer> LITHUANIAN_ANALYZER_CREATOR =
      ProviderCreator.of(LithuanianAnalyzer::new, LithuanianAnalyzer::new, LithuanianAnalyzer::new);
  private static final ProviderCreator<NorwegianAnalyzer> NORWEGIAN_ANALYZER_CREATOR =
      ProviderCreator.of(NorwegianAnalyzer::new, NorwegianAnalyzer::new, NorwegianAnalyzer::new);
  private static final ProviderCreator<PolishAnalyzer> POLISH_ANALYZER_CREATOR =
      ProviderCreator.of(PolishAnalyzer::new, PolishAnalyzer::new, PolishAnalyzer::new);
  private static final ProviderCreator<PortugueseAnalyzer> PORTUGUESE_ANALYZER_CREATOR =
      ProviderCreator.of(PortugueseAnalyzer::new, PortugueseAnalyzer::new, PortugueseAnalyzer::new);
  private static final ProviderCreator<RomanianAnalyzer> ROMANIAN_ANALYZER_CREATOR =
      ProviderCreator.of(RomanianAnalyzer::new, RomanianAnalyzer::new, RomanianAnalyzer::new);
  private static final ProviderCreator<RussianAnalyzer> RUSSIAN_ANALYZER_CREATOR =
      ProviderCreator.of(RussianAnalyzer::new, RussianAnalyzer::new, RussianAnalyzer::new);
  private static final ProviderCreator<SoraniAnalyzer> SORANI_ANALYZER_CREATOR =
      ProviderCreator.of(SoraniAnalyzer::new, SoraniAnalyzer::new, SoraniAnalyzer::new);
  private static final ProviderCreator<SpanishAnalyzer> SPANISH_ANALYZER_CREATOR =
      ProviderCreator.of(SpanishAnalyzer::new, SpanishAnalyzer::new, SpanishAnalyzer::new);
  private static final ProviderCreator<SwedishAnalyzer> SWEDISH_ANALYZER_CREATOR =
      ProviderCreator.of(SwedishAnalyzer::new, SwedishAnalyzer::new, SwedishAnalyzer::new);
  private static final ProviderCreator<TurkishAnalyzer> TURKISH_ANALYZER_CREATOR =
      ProviderCreator.of(TurkishAnalyzer::new, TurkishAnalyzer::new, TurkishAnalyzer::new);

  private static final ImmutableMap<String, AnalyzerProvider.OverriddenBase>
      STOP_WORD_BASED_PROVIDERS =
          ImmutableMap.copyOf(
              Map.ofEntries(
                  entryFor(StockAnalyzerNames.LUCENE_ARABIC.getName(), ARABIC_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_ARMENIAN.getName(), ARMENIAN_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_BASQUE.getName(), BASQUE_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_BENGALI.getName(), BENGALI_ANALYZER_CREATOR),
                  entryFor(
                      StockAnalyzerNames.LUCENE_BRAZILIAN.getName(), BRAZILIAN_ANALYZER_CREATOR),
                  entryFor(
                      StockAnalyzerNames.LUCENE_BULGARIAN.getName(), BULGARIAN_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_CATALAN.getName(), CATALAN_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_CZECH.getName(), CZECH_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_DANISH.getName(), DANISH_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_DUTCH.getName(), DUTCH_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_ENGLISH.getName(), ENGLISH_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_FINNISH.getName(), FINNISH_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_FRENCH.getName(), FRENCH_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_GALICIAN.getName(), GALICIAN_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_GERMAN.getName(), GERMAN_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_HINDI.getName(), HINDI_ANALYZER_CREATOR),
                  entryFor(
                      StockAnalyzerNames.LUCENE_HUNGARIAN.getName(), HUNGARIAN_ANALYZER_CREATOR),
                  entryFor(
                      StockAnalyzerNames.LUCENE_INDONESIAN.getName(), INDONESIAN_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_IRISH.getName(), IRISH_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_ITALIAN.getName(), ITALIAN_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_LATVIAN.getName(), LATVIAN_ANALYZER_CREATOR),
                  entryFor(
                      StockAnalyzerNames.LUCENE_LITHUANIAN.getName(), LITHUANIAN_ANALYZER_CREATOR),
                  entryFor(
                      StockAnalyzerNames.LUCENE_NORWEGIAN.getName(), NORWEGIAN_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_POLISH.getName(), POLISH_ANALYZER_CREATOR),
                  entryFor(
                      StockAnalyzerNames.LUCENE_PORTUGUESE.getName(), PORTUGUESE_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_ROMANIAN.getName(), ROMANIAN_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_RUSSIAN.getName(), RUSSIAN_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_SORANI.getName(), SORANI_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_SPANISH.getName(), SPANISH_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_SWEDISH.getName(), SWEDISH_ANALYZER_CREATOR),
                  entryFor(StockAnalyzerNames.LUCENE_TURKISH.getName(), TURKISH_ANALYZER_CREATOR)));

  private static final ImmutableMap<String, AnalyzerProvider.OverriddenBase>
      BASE_ANALYZER_PROVIDERS =
          ImmutableMap.<String, AnalyzerProvider.OverriddenBase>builder()
              .putAll(LuceneAnalyzerProviders.STOP_WORD_BASED_PROVIDERS)
              .put(StockAnalyzerNames.LUCENE_STANDARD.getName(), new StandardAnalyzerProvider())
              .put(StockAnalyzerNames.LUCENE_STOP.getName(), new StopAnalyzerProvider())
              .put(StockAnalyzerNames.LUCENE_SIMPLE.getName(), new SimpleAnalyzerProvider())
              .put(StockAnalyzerNames.LUCENE_WHITESPACE.getName(), new WhitespaceAnalyzerProvider())
              .put(StockAnalyzerNames.LUCENE_KEYWORD.getName(), new KeywordAnalyzerProvider())
              .put(StockAnalyzerNames.LUCENE_CJK.getName(), new CjkAnalyzerProvider())
              .put(StockAnalyzerNames.LUCENE_CHINESE.getName(), new CjkAnalyzerProvider())
              .put(StockAnalyzerNames.LUCENE_JAPANESE.getName(), new CjkAnalyzerProvider())
              .put(StockAnalyzerNames.LUCENE_KOREAN.getName(), new CjkAnalyzerProvider())
              .put(StockAnalyzerNames.LUCENE_SMARTCN.getName(), new SmartCnAnalyzerProvider())
              .put(StockAnalyzerNames.LUCENE_KUROMOJI.getName(), new KuromojiAnalyzerProvider())
              .put(StockAnalyzerNames.LUCENE_NORI.getName(), new NoriAnalyzerProvider())
              .put(StockAnalyzerNames.LUCENE_GREEK.getName(), new GreekAnalyzerProvider())
              .put(StockAnalyzerNames.LUCENE_PERSIAN.getName(), new PersianAnalyzerProvider())
              .put(StockAnalyzerNames.LUCENE_THAI.getName(), new ThaiAnalyzerProvider())
              .put(StockAnalyzerNames.LUCENE_MORFOLOGIK.getName(), new MorfologikAnalyzerProvider())
              .put(
                  StockAnalyzerNames.LUCENE_UKRAINIAN.getName(),
                  new UkrainianMorfologikAnalyzerProvider())
              .build();

  static boolean hasStockAnalyzerNamed(String name) {
    return BASE_ANALYZER_PROVIDERS.containsKey(name);
  }

  static Optional<AnalyzerProvider.OverriddenBase> baseAnalyzerProviderFor(
      String baseAnalyzerName) {
    return Optional.ofNullable(BASE_ANALYZER_PROVIDERS.get(baseAnalyzerName));
  }

  /** Create all stock analyzers. */
  static List<AnalyzerDefinition> allStockAnalyzers() {
    return BASE_ANALYZER_PROVIDERS.keySet().stream()
        /* Don't try to instantiate non stand alone analyzers. */
        .filter(Predicate.not(StockAnalyzerNames::isBaseOnlyStockAnalyzer))
        .map(OverriddenBaseAnalyzerDefinition::stockAnalyzerWithName)
        .collect(Collectors.toList());
  }

  private static <T extends Analyzer> Map.Entry<String, AnalyzerProvider.OverriddenBase> entryFor(
      String name, ProviderCreator<T> creator) {
    return Map.entry(
        name,
        StopWordBasedAnalyzerProviderFactory.create(
            name,
            creator.getStopWordAndStemConstructor(),
            creator.getStopWordConstructor(),
            creator.getDefaultConstructor()));
  }

  private static class ProviderCreator<T extends Analyzer> {
    private final BiFunction<CharArraySet, CharArraySet, T> stopWordAndStemConstructor;
    private final Function<CharArraySet, T> stopWordConstructor;
    private final Supplier<T> defaultConstructor;

    static <T extends Analyzer> ProviderCreator<T> of(
        BiFunction<CharArraySet, CharArraySet, T> stopWordAndStemConstructor,
        Function<CharArraySet, T> stopWordConstructor,
        Supplier<T> defaultConstructor) {
      return new ProviderCreator<>(
          stopWordAndStemConstructor, stopWordConstructor, defaultConstructor);
    }

    ProviderCreator(
        BiFunction<CharArraySet, CharArraySet, T> stopWordAndStemConstructor,
        Function<CharArraySet, T> stopWordConstructor,
        Supplier<T> defaultConstructor) {
      this.stopWordAndStemConstructor = stopWordAndStemConstructor;
      this.stopWordConstructor = stopWordConstructor;
      this.defaultConstructor = defaultConstructor;
    }

    BiFunction<CharArraySet, CharArraySet, T> getStopWordAndStemConstructor() {
      return this.stopWordAndStemConstructor;
    }

    Function<CharArraySet, T> getStopWordConstructor() {
      return this.stopWordConstructor;
    }

    Supplier<T> getDefaultConstructor() {
      return this.defaultConstructor;
    }
  }
}
