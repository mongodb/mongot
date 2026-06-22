package com.xgen.mongot.index.lucene.explain.explainers;

import com.google.common.truth.Truth;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.highlights.Highlight;
import java.util.Map;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.junit.Assert;
import org.junit.Test;

public class HighlightFeatureExplainerTest {
  @Test
  public void testTimings() throws InterruptedException {
    var explainer = new HighlightFeatureExplainer(Highlight.create(Map.of("foo", "foo"), 1, 2));
    var createCloseable = explainer.getTimings().split(ExplainTimings.Type.EXECUTE_HIGHLIGHT);
    var setupCloseable = explainer.getTimings().split(ExplainTimings.Type.SETUP_HIGHLIGHT);
    Thread.sleep(10);
    createCloseable.close();
    setupCloseable.close();
    var builder = new SearchExplainInformationBuilder();
    explainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);

    var invocations = builder.highlightStats.get().stats().get().invocationCounts().get();
    Truth.assertThat(invocations).hasSize(2);
    Truth.assertThat(invocations)
        .containsExactly(
            ExplainTimings.Type.EXECUTE_HIGHLIGHT.getName(), 1L,
            ExplainTimings.Type.SETUP_HIGHLIGHT.getName(), 1L);
  }

  @Test
  public void addOffsetSource() {
    var explainer =
        new HighlightFeatureExplainer(Highlight.create(Map.of("foo", "foo", "bar", "bar"), 1, 2));
    explainer.addOffsetSource("foo", UnifiedHighlighter.OffsetSource.ANALYSIS);
    explainer.addOffsetSource("bar", UnifiedHighlighter.OffsetSource.POSTINGS);
    var builder = new SearchExplainInformationBuilder();
    explainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);

    Assert.assertEquals(
        Map.of(
            "foo",
            UnifiedHighlighter.OffsetSource.ANALYSIS,
            "bar",
            UnifiedHighlighter.OffsetSource.POSTINGS),
        builder.highlightStats.get().offsetSources());
  }
}
