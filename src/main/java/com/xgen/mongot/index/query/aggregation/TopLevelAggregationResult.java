package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
  public AggregationResult mergePartitionResult(AggregationResult other) {
    TopLevelAggregationResult that = Check.instanceOf(other, TopLevelAggregationResult.class);
    Map<String, AggregationResult> otherByName = new LinkedHashMap<>();
    for (AggregationResult r : that.childResults) {
      if (otherByName.put(r.getName(), r) != null) {
        throw new IllegalStateException("Duplicate child result name: " + r.getName());
      }
    }
    if (this.childResults.size() != otherByName.size()) {
      throw new IllegalStateException(
          "Cannot merge top-level results with mismatched child result names: "
              + this.childResults.stream().map(AggregationResult::getName).toList()
              + " vs "
              + otherByName.keySet());
    }
    List<AggregationResult> merged = new ArrayList<>();
    for (AggregationResult child : this.childResults) {
      if (!otherByName.containsKey(child.getName())) {
        throw new IllegalStateException(
            "Cannot merge top-level results with mismatched child result names: "
                + this.childResults.stream().map(AggregationResult::getName).toList()
                + " vs "
                + otherByName.keySet());
      }
      merged.add(child.mergePartitionResult(otherByName.get(child.getName())));
    }
    return new TopLevelAggregationResult(merged);
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
