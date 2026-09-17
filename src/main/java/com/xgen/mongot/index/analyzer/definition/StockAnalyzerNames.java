package com.xgen.mongot.index.analyzer.definition;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum StockAnalyzerNames {
  LUCENE_STANDARD("lucene.standard"),
  LUCENE_STOP("lucene.stop"),
  LUCENE_SIMPLE("lucene.simple"),
  LUCENE_WHITESPACE("lucene.whitespace"),
  LUCENE_KEYWORD("lucene.keyword"),
  LUCENE_ARABIC("lucene.arabic"),
  LUCENE_ARMENIAN("lucene.armenian"),
  LUCENE_BASQUE("lucene.basque"),
  LUCENE_BENGALI("lucene.bengali"),
  LUCENE_BRAZILIAN("lucene.brazilian"),
  LUCENE_BULGARIAN("lucene.bulgarian"),
  LUCENE_CATALAN("lucene.catalan"),
  LUCENE_CJK("lucene.cjk"),
  LUCENE_CHINESE("lucene.chinese"),
  LUCENE_CZECH("lucene.czech"),
  LUCENE_DANISH("lucene.danish"),
  LUCENE_DUTCH("lucene.dutch"),
  LUCENE_ENGLISH("lucene.english"),
  LUCENE_FINNISH("lucene.finnish"),
  LUCENE_FRENCH("lucene.french"),
  LUCENE_GALICIAN("lucene.galician"),
  LUCENE_GERMAN("lucene.german"),
  LUCENE_GREEK("lucene.greek"),
  LUCENE_HINDI("lucene.hindi"),
  LUCENE_HUNGARIAN("lucene.hungarian"),
  LUCENE_INDONESIAN("lucene.indonesian"),
  LUCENE_IRISH("lucene.irish"),
  LUCENE_ITALIAN("lucene.italian"),
  LUCENE_JAPANESE("lucene.japanese"),
  LUCENE_KOREAN("lucene.korean"),
  LUCENE_KUROMOJI("lucene.kuromoji"),
  LUCENE_LATVIAN("lucene.latvian"),
  LUCENE_LITHUANIAN("lucene.lithuanian"),
  LUCENE_MORFOLOGIK("lucene.morfologik"),
  LUCENE_NORI("lucene.nori"),
  LUCENE_NORWEGIAN("lucene.norwegian"),
  LUCENE_PERSIAN("lucene.persian"),
  LUCENE_POLISH("lucene.polish"),
  LUCENE_PORTUGUESE("lucene.portuguese"),
  LUCENE_ROMANIAN("lucene.romanian"),
  LUCENE_RUSSIAN("lucene.russian"),
  LUCENE_SMARTCN("lucene.smartcn"),
  LUCENE_SORANI("lucene.sorani"),
  LUCENE_SPANISH("lucene.spanish"),
  LUCENE_SWEDISH("lucene.swedish"),
  LUCENE_THAI("lucene.thai"),
  LUCENE_TURKISH("lucene.turkish"),
  LUCENE_UKRAINIAN("lucene.ukrainian");

  private final String analyzerName;

  StockAnalyzerNames(String analyzerName) {
    this.analyzerName = analyzerName;
  }

  public String getName() {
    return this.analyzerName;
  }

  private static final Set<String> STOCK_ANALYZER_NAMES =
      Arrays.stream(StockAnalyzerNames.values())
          .map(StockAnalyzerNames::getName)
          .collect(Collectors.toUnmodifiableSet());

  /**
   * Stock analyzers that only exist as the base of an overridden analyzer definition. They are
   * never instantiated into the analyzer registry, so an index definition that references one
   * directly cannot be resolved at index construction time.
   */
  private static final Set<String> BASE_ONLY_ANALYZER_NAMES = Set.of(LUCENE_STOP.getName());

  public static boolean isStockAnalyzer(String analyzerName) {
    return STOCK_ANALYZER_NAMES.contains(analyzerName);
  }

  public static boolean isBaseOnlyStockAnalyzer(String analyzerName) {
    return BASE_ONLY_ANALYZER_NAMES.contains(analyzerName);
  }
}
