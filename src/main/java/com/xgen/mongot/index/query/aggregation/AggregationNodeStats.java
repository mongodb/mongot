package com.xgen.mongot.index.query.aggregation;

import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Per-node runtime counters for one aggregation, mirroring the accumulator tree.
 *
 * <p>The point is to let a reader walk down the tree and find the level that got expensive, rather
 * than seeing one number for the whole query. A node's counters cover only that node -- they are
 * deliberately not rolled up into the parent, because the interesting comparison is between
 * siblings and between levels, and a rolled-up parent hides which child dominated.
 *
 * <p>One tree is built per accumulator tree, and Lucene builds one accumulator tree per search
 * slice. Slice trees are merged structurally in {@code AccumulatorCollectorManager.reduce} before
 * anything reads them; see {@link #merge}.
 *
 * <p>Each slice tree is confined to the thread collecting that slice, and merge runs after those
 * threads join. Counters are {@code volatile} so those writes stay visible if Lucene later changes
 * how collectors are scheduled. Increment is still not atomic: concurrent updates to the same node
 * would lose counts.
 */
public final class AggregationNodeStats implements Cloneable {

  /** What kind of node this is; absent until the accumulator declares it. */
  public enum NodeType {
    TERMS,
    BOUNDARY,
    METRIC
  }

  private final String name;
  // Not final: clone() reassigns after super.clone() so the copy owns its own child list.
  private List<AggregationNodeStats> children;

  private volatile Optional<NodeType> type = Optional.empty();
  private volatile Optional<String> path = Optional.empty();

  /** {@code numBuckets} for terms, or the boundary count; absent on metric leaves. */
  private volatile Optional<Integer> requestedBuckets = Optional.empty();

  /**
   * Buckets this node instantiated, summed over search slices.
   *
   * <p>Deliberately an <em>allocation</em> count, not a distinct-bucket count: a term appearing in
   * three slices is three instantiated buckets. On a multi-slice search this therefore exceeds the
   * number of distinct buckets the query returns.
   */
  private volatile long observedBuckets;

  /**
   * Times this node's leaf collector was invoked, summed over documents, segments and slices.
   *
   * <p>Localises cost without a clock: per-node wall time would mean two clock reads per node per
   * matching document. A counter increment on the same path costs a thread-confined add, and the
   * ratio between a parent's and a child's counts is what shows where the fan-out happened.
   */
  private volatile long collectInvocations;

  public AggregationNodeStats(String name, List<AggregationNodeStats> children) {
    this.name = name;
    this.children = List.copyOf(children);
  }

  /**
   * Declares this node as a bucket-producing grouping. Called once, at accumulator construction.
   */
  void declareGrouping(NodeType type, String path, int requestedBuckets) {
    this.type = Optional.of(type);
    this.path = Optional.of(path);
    this.requestedBuckets = Optional.of(requestedBuckets);
  }

  /** Declares this node as a metric leaf. Called once, at accumulator construction. */
  void declareMetric(Optional<String> path) {
    this.type = Optional.of(NodeType.METRIC);
    this.path = path;
  }

  void recordObservedBuckets(long count) {
    this.observedBuckets += count;
  }

  /** One leaf-collector invocation on this node. On the per-document path; keep it this cheap. */
  void recordCollectInvocation() {
    this.collectInvocations++;
  }

  public String name() {
    return this.name;
  }

  public Optional<NodeType> type() {
    return this.type;
  }

  public Optional<String> path() {
    return this.path;
  }

  public Optional<Integer> requestedBuckets() {
    return this.requestedBuckets;
  }

  public long observedBuckets() {
    return this.observedBuckets;
  }

  public long collectInvocations() {
    return this.collectInvocations;
  }

  public List<AggregationNodeStats> children() {
    return this.children;
  }

  /** Levels in this subtree, counting this node. The query header's {@code treeDepth}. */
  public int depth() {
    @Var int deepestChild = 0;
    for (AggregationNodeStats child : this.children) {
      deepestChild = Math.max(deepestChild, child.depth());
    }
    return 1 + deepestChild;
  }

  /**
   * Counts nodes of each declared type in this subtree; undeclared nodes (the root) are skipped.
   */
  public Map<NodeType, Long> nodeCountsByType() {
    Map<NodeType, Long> counts = new EnumMap<>(NodeType.class);
    collectNodeCounts(counts);
    return counts;
  }

  private void collectNodeCounts(Map<NodeType, Long> counts) {
    this.type.ifPresent(t -> counts.merge(t, 1L, Long::sum));
    for (AggregationNodeStats child : this.children) {
      child.collectNodeCounts(counts);
    }
  }

  /** This node's buckets plus every descendant's. */
  public long totalObservedBuckets() {
    @Var long total = this.observedBuckets;
    for (AggregationNodeStats child : this.children) {
      total += child.totalObservedBuckets();
    }
    return total;
  }

  /**
   * Structurally merges {@code other} into this tree, summing counters node by node.
   *
   * <p>Every slice builds its tree from the same spec, so the shapes are identical by construction;
   * a mismatch means the trees did not come from one query and is a programming error rather than
   * something to tolerate silently.
   */
  public void merge(AggregationNodeStats other) {
    if (!this.name.equals(other.name) || this.children.size() != other.children.size()) {
      throw new IllegalArgumentException(
          String.format(
              "cannot merge aggregation node stats of differing shape: '%s' (%d children) vs '%s'"
                  + " (%d children)",
              this.name, this.children.size(), other.name, other.children.size()));
    }
    this.observedBuckets += other.observedBuckets;
    this.collectInvocations += other.collectInvocations;
    if (this.type.isEmpty()) {
      this.type = other.type;
      this.path = other.path;
      this.requestedBuckets = other.requestedBuckets;
    }
    for (int i = 0; i < this.children.size(); i++) {
      this.children.get(i).merge(other.children.get(i));
    }
  }

  /**
   * A deep copy, so a merge target is never one of the slice trees it is merging.
   *
   * <p>{@code super.clone()} copies the counters; children are cloned into a new list so the copy
   * does not share the original tree.
   */
  @Override
  public AggregationNodeStats clone() {
    try {
      AggregationNodeStats copy = (AggregationNodeStats) super.clone();
      List<AggregationNodeStats> copiedChildren = new ArrayList<>(this.children.size());
      for (AggregationNodeStats child : this.children) {
        copiedChildren.add(child.clone());
      }
      copy.children = List.copyOf(copiedChildren);
      return copy;
    } catch (CloneNotSupportedException e) {
      throw new AssertionError(e);
    }
  }
}
