package com.xgen.mongot.index.query.aggregation;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.lucene.explain.explainers.AggregationFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainQueryState;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.TermsArgs;
import com.xgen.mongot.util.FieldPath;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

/** Per-node stats: shape, per-strategy bucket counting, and the cross-slice merge. */
public class AggregationNodeStatsTest extends AggregationTestSupport {

  private static TopAggregationSpec termsByCityWithCount() {
    return new TopAggregationSpec(
        List.of(
            new TermsGroupAggregationSpec(
                "by_city",
                FieldPath.parse(CITY_FIELD),
                List.of(new CountAggregationSpec("cnt")),
                new TermsArgs(true, TypeField.TOKEN, Integer.MAX_VALUE))));
  }

  private static List<TestDoc> docsAcross(int cities) {
    List<TestDoc> docs = new ArrayList<>();
    for (int i = 0; i < cities; i++) {
      docs.add(new TestDoc("city-" + i, "x", i));
    }
    return docs;
  }

  /** Minimal explain context so {@code Explain.getQueryInfo()} resolves a real QueryInfo. */
  @SuppressWarnings("MustBeClosedChecker")
  private static Scope explainContext() {
    return Context.current()
        .with(
            new ExplainQueryState(
                new Explain.QueryInfo(Explain.Verbosity.EXECUTION_STATS), /* numPartitions= */ 0))
        .makeCurrent();
  }

  /** Locates a node by name anywhere in the tree, so tests do not hard-code child ordering. */
  private static AggregationNodeStats find(AggregationNodeStats root, String name) {
    if (root.name().equals(name)) {
      return root;
    }
    for (AggregationNodeStats child : root.children()) {
      AggregationNodeStats hit = find(child, name);
      if (hit != null) {
        return hit;
      }
    }
    return null;
  }

  @Test
  public void nodeStats_mirrorsTheAccumulatorTree() throws Exception {
    buildIndex(docsAcross(5), Integer.MAX_VALUE);

    var top = collectAll(termsByCityWithCount(), newExplainContext());
    top.buildTopLevelResult();

    AggregationNodeStats root = top.nodeStats().orElseThrow();
    assertThat(root.children()).hasSize(1);
    AggregationNodeStats byCity = root.children().get(0);
    assertThat(byCity.name()).isEqualTo("by_city");
    assertThat(byCity.children()).hasSize(1);
    assertThat(byCity.children().get(0).name()).isEqualTo("cnt");
  }

  @Test
  public void groupingNode_declaresTypePathAndRequestedBuckets() throws Exception {
    buildIndex(docsAcross(5), Integer.MAX_VALUE);

    var top = collectAll(termsByCityWithCount(), newExplainContext());
    top.buildTopLevelResult();

    AggregationNodeStats byCity = find(top.nodeStats().orElseThrow(), "by_city");
    assertThat(byCity.type()).hasValue(AggregationNodeStats.NodeType.TERMS);
    assertThat(byCity.path()).hasValue(CITY_FIELD);
    assertThat(byCity.requestedBuckets()).hasValue(Integer.MAX_VALUE);
  }

  @Test
  public void metricLeaf_declaresMetricType() throws Exception {
    buildIndex(docsAcross(5), Integer.MAX_VALUE);

    var top = collectAll(termsByCityWithCount(), newExplainContext());
    top.buildTopLevelResult();

    assertThat(find(top.nodeStats().orElseThrow(), "cnt").type())
        .hasValue(AggregationNodeStats.NodeType.METRIC);
  }

  /**
   * The reason observed buckets are counted in buildResults rather than from the bucket-ord space:
   * a dense root terms node indexes storage by global term ordinal, so its array length tracks the
   * field's cardinality, not the buckets it produced.
   */
  @Test
  public void observedBuckets_countsBucketsNotOrdinalSpace() throws Exception {
    buildIndex(docsAcross(7), Integer.MAX_VALUE);

    var top = collectAll(termsByCityWithCount(), newExplainContext());
    top.buildTopLevelResult();

    assertThat(find(top.nodeStats().orElseThrow(), "by_city").observedBuckets()).isEqualTo(7);
  }

  @Test
  public void observedBuckets_notRecordedUntilResultsAreBuilt() throws Exception {
    buildIndex(docsAcross(7), Integer.MAX_VALUE);

    var top = collectAll(termsByCityWithCount(), newExplainContext());

    assertThat(find(top.nodeStats().orElseThrow(), "by_city").observedBuckets()).isEqualTo(0);
  }

  @Test
  public void totalObservedBuckets_sumsTheSubtree() {
    var leaf = new AggregationNodeStats("cnt", List.of());
    var mid = new AggregationNodeStats("by_city", List.of(leaf));
    var root = new AggregationNodeStats("root", List.of(mid));
    mid.recordObservedBuckets(7);
    leaf.recordObservedBuckets(3);

    assertThat(root.totalObservedBuckets()).isEqualTo(10);
  }

  /** Slice trees are identical in shape, so merging is a structural sum. */
  @Test
  public void merge_sumsCountersNodeByNode() {
    var a =
        new AggregationNodeStats("root", List.of(new AggregationNodeStats("by_city", List.of())));
    var b =
        new AggregationNodeStats("root", List.of(new AggregationNodeStats("by_city", List.of())));
    a.children().get(0).recordObservedBuckets(4);
    b.children().get(0).recordObservedBuckets(6);

    a.merge(b);

    assertThat(a.children().get(0).observedBuckets()).isEqualTo(10);
  }

  @Test
  public void merge_fillsMetadataFromTheOtherTreeWhenAbsent() {
    var a = new AggregationNodeStats("by_city", List.of());
    var b = new AggregationNodeStats("by_city", List.of());
    b.declareGrouping(AggregationNodeStats.NodeType.TERMS, "city", 10);

    a.merge(b);

    assertThat(a.type()).hasValue(AggregationNodeStats.NodeType.TERMS);
    assertThat(a.requestedBuckets()).hasValue(10);
  }

  /** A shape mismatch means the trees are not from one query; fail rather than mis-attribute. */
  @Test
  public void merge_differingShape_throws() {
    var a = new AggregationNodeStats("root", List.of(new AggregationNodeStats("x", List.of())));
    var b = new AggregationNodeStats("root", List.of());

    assertThrows(IllegalArgumentException.class, () -> a.merge(b));
  }

  /** clone() keeps the merge target off the slice trees, which would otherwise double-count. */
  @Test
  public void clone_isIndependentOfTheOriginal() {
    var child = new AggregationNodeStats("cnt", List.of());
    child.recordCollectInvocation();
    var original = new AggregationNodeStats("by_city", List.of(child));
    original.recordObservedBuckets(5);
    original.declareGrouping(AggregationNodeStats.NodeType.TERMS, "city", 10);

    var copy = original.clone();
    copy.recordObservedBuckets(5);
    copy.children().get(0).recordCollectInvocation();

    assertThat(original.observedBuckets()).isEqualTo(5);
    assertThat(copy.observedBuckets()).isEqualTo(10);
    assertThat(copy.requestedBuckets()).hasValue(10);
    assertThat(copy).isNotSameInstanceAs(original);
    assertThat(copy.children().get(0)).isNotSameInstanceAs(original.children().get(0));
    assertThat(original.children().get(0).collectInvocations()).isEqualTo(1);
    assertThat(copy.children().get(0).collectInvocations()).isEqualTo(2);
  }

  /** End to end: stats tree -> explainer -> the nested {@code aggregation.tree} explain section. */
  @Test
  public void recordNodeStats_rendersTheNestedExplainTree() {
    var leaf = new AggregationNodeStats("cnt", List.of());
    leaf.declareMetric(Optional.of("amount"));
    var byCity = new AggregationNodeStats("by_city", List.of(leaf));
    byCity.declareGrouping(AggregationNodeStats.NodeType.TERMS, "city", 10);
    byCity.recordObservedBuckets(4200);
    var root = new AggregationNodeStats("root", List.of(byCity));

    try (var unused = explainContext()) {
      AggregationFeatureExplainer.recordNodeStats(root);
      AggregationFeatureExplainer.recordCursorId(7431299184L);
      var builder = new SearchExplainInformationBuilder();
      Explain.getQueryInfo()
          .orElseThrow()
          .getFeatureExplainer(AggregationFeatureExplainer.class, AggregationFeatureExplainer::new)
          .emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);

      var tree = builder.build().aggregation().orElseThrow().tree().orElseThrow();
      assertThat(tree.name()).isEqualTo("root");
      var child = tree.children().orElseThrow().get(0);
      assertThat(child.name()).isEqualTo("by_city");
      assertThat(child.type()).hasValue(AggregationNodeStats.NodeType.TERMS);
      assertThat(child.path()).hasValue("city");
      assertThat(child.requestedBuckets()).hasValue(10);
      assertThat(child.observedBuckets()).isEqualTo(4200);
      assertThat(child.children().orElseThrow().get(0).name()).isEqualTo("cnt");

      // The flat query-level header sits alongside the tree, derived from the same stats.
      var query = builder.build().aggregation().orElseThrow().query().orElseThrow();
      assertThat(query.treeDepth()).isEqualTo(3);
      assertThat(query.nodeCountsByType())
          .containsExactly(
              AggregationNodeStats.NodeType.TERMS, 1L, AggregationNodeStats.NodeType.METRIC, 1L);
      assertThat(query.observedBuckets()).isEqualTo(4200);
      assertThat(query.cursorId()).hasValue(7431299184L);
    }
  }

  @Test
  public void emitExplanation_queryStatsWithoutCursorId_throws() {
    var root = new AggregationNodeStats("root", List.of());

    try (var unused = explainContext()) {
      AggregationFeatureExplainer.recordNodeStats(root);
      var builder = new SearchExplainInformationBuilder();
      var explainer =
          Explain.getQueryInfo()
              .orElseThrow()
              .getFeatureExplainer(
                  AggregationFeatureExplainer.class, AggregationFeatureExplainer::new);

      assertThrows(
          IllegalStateException.class,
          () -> explainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder));
    }
  }

  @Test
  public void queryHeader_carriesCursorIdOnceRecorded() {
    var root = new AggregationNodeStats("root", List.of());

    try (var unused = explainContext()) {
      AggregationFeatureExplainer.recordNodeStats(root);
      AggregationFeatureExplainer.recordCursorId(7431299184L);

      var builder = new SearchExplainInformationBuilder();
      Explain.getQueryInfo()
          .orElseThrow()
          .getFeatureExplainer(AggregationFeatureExplainer.class, AggregationFeatureExplainer::new)
          .emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);

      assertThat(builder.build().aggregation().orElseThrow().query().orElseThrow().cursorId())
          .hasValue(7431299184L);
    }
  }

  /** collectInvocations counts leaf-collector calls on every node, bucket and metric alike. */
  @Test
  public void collectInvocations_countedOnBucketAndMetricNodes() throws Exception {
    buildIndex(docsAcross(6), Integer.MAX_VALUE);

    var top = collectAll(termsByCityWithCount(), newExplainContext());
    top.buildTopLevelResult();

    assertThat(find(top.nodeStats().orElseThrow(), "by_city").collectInvocations()).isEqualTo(6);
    assertThat(find(top.nodeStats().orElseThrow(), "cnt").collectInvocations()).isEqualTo(6);
  }

  @Test
  public void collectInvocations_notRecordedWhenExplainIsOff() throws Exception {
    buildIndex(docsAcross(6), Integer.MAX_VALUE);

    var top = collectAll(termsByCityWithCount(), newContext());
    top.buildTopLevelResult();

    assertThat(top.nodeStats()).isEmpty();
  }

  /** A metric leaf has no children, so the field is omitted rather than emitted empty. */
  @Test
  public void recordNodeStats_metricLeaf_omitsChildren() {
    var leaf = new AggregationNodeStats("cnt", List.of());
    leaf.declareMetric(Optional.empty());

    try (var unused = explainContext()) {
      AggregationFeatureExplainer.recordNodeStats(leaf);
      AggregationFeatureExplainer.recordCursorId(7431299184L);
      var builder = new SearchExplainInformationBuilder();
      Explain.getQueryInfo()
          .orElseThrow()
          .getFeatureExplainer(AggregationFeatureExplainer.class, AggregationFeatureExplainer::new)
          .emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);

      assertThat(builder.build().aggregation().orElseThrow().tree().orElseThrow().children())
          .isEmpty();
    }
  }
}
