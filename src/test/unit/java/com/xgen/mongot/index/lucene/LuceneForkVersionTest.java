package com.xgen.mongot.index.lucene;

import com.google.common.truth.Truth;
import java.net.URL;
import org.apache.lucene.analysis.cn.smart.HMMChineseTokenizerFactory;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.icu.ICUNormalizer2CharFilter;
import org.apache.lucene.analysis.ja.JapaneseAnalyzer;
import org.apache.lucene.analysis.ko.KoreanAnalyzer;
import org.apache.lucene.analysis.morfologik.MorfologikFilter;
import org.apache.lucene.analysis.phonetic.BeiderMorseFilter;
import org.apache.lucene.analysis.stempel.StempelPolishStemFilterFactory;
import org.apache.lucene.backward_codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.bloom.DefaultBloomFilterFactory;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.misc.SweetSpotSimilarity;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.queryparser.charstream.FastCharStream;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.Version;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verify that lucene-core, lucene-codecs, and lucene-backward-codecs are loaded from our fork,
 * while other Lucene modules come from upstream.
 *
 * <p>The fork is resolved via a separate maven_install ("lucene_fork") and wired into the main
 * dependency graph via override_targets in deps.bzl. If this test fails, check that configuration.
 *
 * <p>This is one of three layered guards enforcing mongot's codec rollback-safety contract, each
 * locking a different axis:
 *
 * <ul>
 *   <li>{@code LuceneForkVersionTest} — which jar each Lucene module loads from.
 *   <li>{@code CodecSubformatChangeDetectorTest} — per-field dispatch and the class of every
 *       sub-format slot.
 *   <li>{@code OnDiskFormatVersionsTest} — the on-disk version each format writes.
 * </ul>
 *
 * <p>See the codec sub-format rollout policy:
 * docs/internal/development/codec-subformat-rollout-policy.md.
 */
public class LuceneForkVersionTest {

  private static final String UPSTREAM_VERSION = "10.1.0";
  private static final String FORK_VERSION = "10.1.0-5";

  @Test
  public void luceneCore_fromFork() {
    String jarPath = jarLocationOf(Version.class);
    Truth.assertThat(jarPath).contains("lucene-core-" + FORK_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-core-" + UPSTREAM_VERSION + ".jar");
  }

  @Test
  public void luceneBackwardCodecs_fromFork() {
    String jarPath = jarLocationOf(Lucene95Codec.class);
    Truth.assertThat(jarPath).contains("lucene-backward-codecs-" + FORK_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-backward-codecs-" + UPSTREAM_VERSION + ".jar");
  }

  @Test
  public void luceneCodecs_fromFork() {
    String jarPath = jarLocationOf(DefaultBloomFilterFactory.class);
    Truth.assertThat(jarPath).contains("lucene-codecs-" + FORK_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-codecs-" + UPSTREAM_VERSION + ".jar");
  }

  @Test
  public void luceneAnalysisCommon_fromUpstream() {
    String jarPath = jarLocationOf(EnglishAnalyzer.class);
    Truth.assertThat(jarPath).contains("lucene-analysis-common-" + UPSTREAM_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-analysis-common-" + FORK_VERSION + ".jar");
  }

  @Test
  public void luceneAnalysisIcu_fromUpstream() {
    String jarPath = jarLocationOf(ICUNormalizer2CharFilter.class);
    Truth.assertThat(jarPath).contains("lucene-analysis-icu-" + UPSTREAM_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-analysis-icu-" + FORK_VERSION + ".jar");
  }

  @Test
  public void luceneAnalysisKuromoji_fromUpstream() {
    String jarPath = jarLocationOf(JapaneseAnalyzer.class);
    Truth.assertThat(jarPath).contains("lucene-analysis-kuromoji-" + UPSTREAM_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-analysis-kuromoji-" + FORK_VERSION + ".jar");
  }

  @Test
  public void luceneAnalysisMorfologik_fromUpstream() {
    String jarPath = jarLocationOf(MorfologikFilter.class);
    Truth.assertThat(jarPath).contains("lucene-analysis-morfologik-" + UPSTREAM_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-analysis-morfologik-" + FORK_VERSION + ".jar");
  }

  @Test
  public void luceneAnalysisNori_fromUpstream() {
    String jarPath = jarLocationOf(KoreanAnalyzer.class);
    Truth.assertThat(jarPath).contains("lucene-analysis-nori-" + UPSTREAM_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-analysis-nori-" + FORK_VERSION + ".jar");
  }

  @Test
  public void luceneAnalysisPhonetic_fromUpstream() {
    String jarPath = jarLocationOf(BeiderMorseFilter.class);
    Truth.assertThat(jarPath).contains("lucene-analysis-phonetic-" + UPSTREAM_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-analysis-phonetic-" + FORK_VERSION + ".jar");
  }

  @Test
  public void luceneAnalysisSmartcn_fromUpstream() {
    String jarPath = jarLocationOf(HMMChineseTokenizerFactory.class);
    Truth.assertThat(jarPath).contains("lucene-analysis-smartcn-" + UPSTREAM_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-analysis-smartcn-" + FORK_VERSION + ".jar");
  }

  @Test
  public void luceneAnalysisStempel_fromUpstream() {
    String jarPath = jarLocationOf(StempelPolishStemFilterFactory.class);
    Truth.assertThat(jarPath).contains("lucene-analysis-stempel-" + UPSTREAM_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-analysis-stempel-" + FORK_VERSION + ".jar");
  }

  @Test
  public void luceneExpressions_fromUpstream() {
    String jarPath = jarLocationOf(Expression.class);
    Truth.assertThat(jarPath).contains("lucene-expressions-" + UPSTREAM_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-expressions-" + FORK_VERSION + ".jar");
  }

  @Test
  public void luceneFacet_fromUpstream() {
    String jarPath = jarLocationOf(FacetsConfig.class);
    Truth.assertThat(jarPath).contains("lucene-facet-" + UPSTREAM_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-facet-" + FORK_VERSION + ".jar");
  }

  @Test
  public void luceneHighlighter_fromUpstream() {
    String jarPath = jarLocationOf(Highlighter.class);
    Truth.assertThat(jarPath).contains("lucene-highlighter-" + UPSTREAM_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-highlighter-" + FORK_VERSION + ".jar");
  }

  @Test
  public void luceneJoin_fromUpstream() {
    String jarPath = jarLocationOf(BitSetProducer.class);
    Truth.assertThat(jarPath).contains("lucene-join-" + UPSTREAM_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-join-" + FORK_VERSION + ".jar");
  }

  @Test
  public void luceneMisc_fromUpstream() {
    String jarPath = jarLocationOf(SweetSpotSimilarity.class);
    Truth.assertThat(jarPath).contains("lucene-misc-" + UPSTREAM_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-misc-" + FORK_VERSION + ".jar");
  }

  @Test
  public void luceneQueries_fromUpstream() {
    String jarPath = jarLocationOf(CommonTermsQuery.class);
    Truth.assertThat(jarPath).contains("lucene-queries-" + UPSTREAM_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-queries-" + FORK_VERSION + ".jar");
  }

  @Test
  public void luceneQueryparser_fromUpstream() {
    String jarPath = jarLocationOf(FastCharStream.class);
    Truth.assertThat(jarPath).contains("lucene-queryparser-" + UPSTREAM_VERSION + ".jar");
    Truth.assertThat(jarPath).doesNotContain("lucene-queryparser-" + FORK_VERSION + ".jar");
  }

  private static String jarLocationOf(Class<?> clazz) {
    URL location = clazz.getProtectionDomain().getCodeSource().getLocation();
    Assert.assertNotNull("Could not determine code source for " + clazz.getName(), location);
    return location.toString();
  }
}
