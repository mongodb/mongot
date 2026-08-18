package com.xgen.mongot.index.query.aggregation;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.BoundaryArgs;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

/**
 * Group spec for numeric / date boundary (range) bucketing.
 *
 * <p>Carries two parallel views of the boundaries: {@code boundaryEdges} (encoded into the stored
 * doc-values long space, used for collection) and {@code rangeKeys} (the original lower-bound BSON
 * value per range, used as the bucket {@code _id} in results). The caller supplies both, encoding
 * the edges into the stored long space ahead of construction.
 */
public final class BoundaryGroupAggregationSpec extends GroupAggregationSpec {
  // Boundaries encoded into the stored doc-values long space, strictly ascending.
  // Length N => N-1 half-open ranges [edges[i], edges[i+1]).
  private final long[] boundaryEdges;
  // Original lower-bound value per range (size == numRanges), used as the bucket _id in results.
  private final List<BsonValue> rangeKeys;
  // Present => emit a trailing "default" bucket; the value is its label (a BsonString).
  private final Optional<BsonValue> defaultBucketKey;
  // The top-most range's upper edge is inclusive only in the INT64-overflow case (a double boundary
  // > Long.MAX_VALUE), so huge / +Inf values land in the top bucket.
  private final boolean lastEdgeInclusive;
  private final TypeField typeField;

  /** Constructs a boundary group spec with pre-encoded edges and metadata from {@code args}. */
  public BoundaryGroupAggregationSpec(
      String name,
      FieldPath path,
      List<AggregationSpec> children,
      long[] boundaryEdges,
      List<BsonValue> rangeKeys,
      boolean lastEdgeInclusive,
      BoundaryArgs args) {
    this(
        name,
        path,
        children,
        boundaryEdges,
        rangeKeys,
        lastEdgeInclusive,
        args.defaultKey(),
        args.typeField()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "type field is required when field metadata is present")));
  }

  private BoundaryGroupAggregationSpec(
      String name,
      FieldPath path,
      List<AggregationSpec> children,
      long[] boundaryEdges,
      List<BsonValue> rangeKeys,
      boolean lastEdgeInclusive,
      Optional<BsonValue> defaultBucketKey,
      TypeField typeField) {
    super(path, name, children);
    checkArg(
        boundaryEdges.length >= 2,
        "boundaries must have >= 2 entries, got: %s",
        boundaryEdges.length);
    for (int i = 1; i < boundaryEdges.length; i++) {
      checkArg(
          boundaryEdges[i] > boundaryEdges[i - 1],
          "boundaries must be strictly ascending in encoded space at index %s",
          i);
    }
    checkArg(
        rangeKeys.size() == boundaryEdges.length - 1,
        "rangeKeys size (%s) must equal number of ranges (%s)",
        rangeKeys.size(),
        boundaryEdges.length - 1);
    this.boundaryEdges = boundaryEdges.clone();
    this.rangeKeys = List.copyOf(rangeKeys);
    this.defaultBucketKey = defaultBucketKey;
    this.lastEdgeInclusive = lastEdgeInclusive;
    this.typeField = typeField;
  }

  /**
   * Builds a boundary group spec for shard merge planning when index field metadata is unavailable.
   *
   * <p>Encoded doc-value edges are unused by {@link GroupAggregationSpec#buildShardMergePipeline};
   * placeholder edges keep the constructor's ascending-edge invariant while range keys carry the
   * request's boundary values.
   */
  public static BoundaryGroupAggregationSpec createBoundarySpecWithoutResolving(
      String name, FieldPath path, List<AggregationSpec> children, BoundaryArgs args) {
    List<? extends BsonValue> boundaries = args.boundaries();
    int n = boundaries.size();
    long[] edges = new long[n];
    for (int i = 0; i < n; i++) {
      edges[i] = i;
    }
    List<BsonValue> rangeKeys = new ArrayList<>(n - 1);
    for (int i = 0; i < n - 1; i++) {
      rangeKeys.add(boundaries.get(i));
    }
    return new BoundaryGroupAggregationSpec(
        name,
        path,
        children,
        edges,
        rangeKeys,
        false,
        args.defaultKey(),
        // Merge planning omits typeField; encoded edges are unused and a dummy keeps the
        // accumulator field populated for the constructor invariant.
        args.typeField().orElse(TypeField.NUMBER_INT64));
  }

  // Package-private accessors consumed once by BoundaryAccumulator's constructor.
  long[] boundaryEdges() {
    return this.boundaryEdges.clone();
  }

  List<BsonValue> rangeKeys() {
    return this.rangeKeys;
  }

  Optional<BsonValue> defaultBucketKey() {
    return this.defaultBucketKey;
  }

  boolean lastEdgeInclusive() {
    return this.lastEdgeInclusive;
  }

  @Override
  protected Accumulator createAccumulator(
      AggregationContext context, List<Accumulator> childAccumulators) {
    return new BoundaryAccumulator(this, context, childAccumulators, this.typeField);
  }
}
