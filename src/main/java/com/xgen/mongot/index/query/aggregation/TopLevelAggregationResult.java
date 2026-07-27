package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * Result produced by the top-level aggregation. Wraps the child results (one per top-level
 * aggregation).
 */
public record TopLevelAggregationResult(List<AggregationResult> childResults)
    implements AggregationResult {

  @Override
  public String getName() {
    throw new UnsupportedOperationException("getName() is not supported for top-level result");
  }

  @Override
  public AggregationResultBuilder toBuilder() {
    return new Builder(this);
  }

  /** Merge state for the top level: one child builder per named top-level aggregation. */
  private static final class Builder implements AggregationResultBuilder {
    // Seeded in the seed's child order so the built result preserves it.
    private final LinkedHashMap<String, AggregationResultBuilder> childBuilders =
        new LinkedHashMap<>();

    // Identity element of the merge for `shape`'s child names: every child builder starts empty
    // until add() folds results in, so the fold has no first-element special case.
    Builder(TopLevelAggregationResult shape) {
      for (AggregationResult child : shape.childResults) {
        if (this.childBuilders.put(child.getName(), child.toBuilder()) != null) {
          throw new IllegalStateException("Duplicate child result name: " + child.getName());
        }
      }
    }

    @Override
    public void add(AggregationResult other) {
      TopLevelAggregationResult that = Check.instanceOf(other, TopLevelAggregationResult.class);
      List<String> otherNames = that.childResults.stream().map(AggregationResult::getName).toList();
      if (!this.childBuilders.keySet().equals(Set.copyOf(otherNames))
          || otherNames.size() != this.childBuilders.size()) {
        throw new IllegalStateException(
            "Cannot merge top-level results with mismatched child result names: "
                + this.childBuilders.keySet()
                + " vs "
                + otherNames);
      }
      that.childResults.forEach(child -> this.childBuilders.get(child.getName()).add(child));
    }

    @Override
    public TopLevelAggregationResult build() {
      return new TopLevelAggregationResult(
          this.childBuilders.values().stream().map(AggregationResultBuilder::build).toList());
    }
  }

  @Override
  public AggregationResult applyLimit() {
    return new TopLevelAggregationResult(
        this.childResults.stream().map(AggregationResult::applyLimit).toList());
  }

  @Override
  public BsonDocument toStandaloneBson() {
    var result = new BsonDocument();
    for (AggregationResult child : this.childResults) {
      result.append(child.getName(), child.toStandaloneBson());
    }
    return result;
  }

  @Override
  public BsonArray toIntermediateBson() {
    BsonArray docs = new BsonArray();
    for (AggregationResult child : this.childResults) {
      BsonArray childDocs = child.toIntermediateBson();
      for (BsonValue value : childDocs) {
        BsonDocument source = value.asDocument();
        if (source.containsKey(IntermediateAggregationMetaField.GROUP_NAME)) {
          throw new IllegalStateException(
              "Child intermediate document must not set routing field "
                  + IntermediateAggregationMetaField.GROUP_NAME);
        }
        BsonDocument doc = new BsonDocument();
        doc.putAll(source);
        doc.append(IntermediateAggregationMetaField.GROUP_NAME, new BsonString(child.getName()));
        docs.add(doc);
      }
    }
    return docs;
  }
}
