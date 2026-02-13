package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public abstract class ExplainableTestSpec extends TestSpec {

  protected final BsonDocument explain;
  protected final List<BsonDocument> documents;

  ExplainableTestSpec(
      String name,
      Optional<String> description,
      Optional<String> basedOnTestSpec,
      Optional<IndexSpec> index,
      Optional<BsonDocument> query,
      Optional<List<BsonDocument>> aggregation,
      Result result,
      BsonDocument explain,
      List<BsonDocument> documents,
      Optional<Map<String, ShardZoneConfig>> shardZoneConfigs,
      Optional<MongoDbVersionInfo> minMongoDbVersionInfo,
      Optional<MongoDbVersionInfo> minShardedMongoDbVersionInfo,
      Optional<List<DynamicFeatureFlagConfig>> dynamicFeatureFlags,
      boolean skipOnAtlas) {
    super(
        name,
        description,
        basedOnTestSpec,
        index,
        query,
        aggregation,
        result,
        shardZoneConfigs,
        minMongoDbVersionInfo,
        minShardedMongoDbVersionInfo,
        dynamicFeatureFlags,
        skipOnAtlas);
    this.explain = explain;
    this.documents = documents;
  }

  public BsonDocument getExplain() {
    return this.explain;
  }

  public List<BsonDocument> getDocuments() {
    return this.documents;
  }

  public BsonValue getExplainVerbosity() {
    return this.explain.get("verbosity");
  }

  public abstract SearchStage getSearchStage();
}
