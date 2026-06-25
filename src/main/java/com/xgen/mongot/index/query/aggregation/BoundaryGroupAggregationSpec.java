package com.xgen.mongot.index.query.aggregation;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.BoundaryArgs;
import com.xgen.mongot.util.FieldPath;
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
    this.defaultBucketKey = args.defaultKey();
    this.lastEdgeInclusive = lastEdgeInclusive;
    this.typeField = args.typeField();
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
