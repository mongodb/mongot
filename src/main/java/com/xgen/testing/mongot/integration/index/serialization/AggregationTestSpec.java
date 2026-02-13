package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.BsonDocument;

public class AggregationTestSpec extends TestSpec {
  static class Fields {
    static final Field.Required<Map<String, List<BsonDocument>>> COLLECTIONS =
        Field.builder("collections").documentField().asList().asMap().required();

    static final Field.WithDefault<String> BASE_COLLECTION =
        Field.builder("baseCollection").stringField().optional().withDefault("coll0");
  }

  private final Map<String, List<BsonDocument>> collections;
  // The base collection is the one that the aggregation is run against.
  private final String baseCollection;

  private AggregationTestSpec(
      String name,
      Optional<String> description,
      Optional<String> basedOnTestSpec,
      Optional<IndexSpec> index,
      Map<String, List<BsonDocument>> collections,
      String baseCollection,
      Optional<BsonDocument> query,
      Optional<List<BsonDocument>> aggregation,
      Result result,
      Optional<Map<String, ShardZoneConfig>> shardZoneConfigs,
      Optional<MongoDbVersionInfo> minMongoDbVersion,
      Optional<MongoDbVersionInfo> minShardedMongoDbVersion,
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
        minMongoDbVersion,
        minShardedMongoDbVersion,
        dynamicFeatureFlags,
        skipOnAtlas);
    this.collections = collections;
    this.baseCollection = baseCollection;
  }

  static AggregationTestSpec fromBson(DocumentParser parser) throws BsonParseException {
    return new AggregationTestSpec(
        parser.getField(TestSpec.Fields.NAME).unwrap(),
        parser.getField(TestSpec.Fields.DESCRIPTION).unwrap(),
        parser.getField(TestSpec.Fields.BASED_ON_TEST_SPEC).unwrap(),
        parser.getField(TestSpec.Fields.INDEX).unwrap(),
        parser.getField(Fields.COLLECTIONS).unwrap(),
        parser.getField(Fields.BASE_COLLECTION).unwrap(),
        parser.getField(TestSpec.Fields.QUERY).unwrap(),
        parser.getField(TestSpec.Fields.AGGREGATION).unwrap(),
        parser.getField(TestSpec.Fields.RESULT).unwrap(),
        parser.getField(TestSpec.Fields.SHARD_ZONE_CONFIGS).unwrap(),
        parser.getField(TestSpec.Fields.MIN_MONGODB_VERSION).unwrap(),
        parser.getField(TestSpec.Fields.MIN_SHARDED_MONGODB_VERSION).unwrap(),
        parser.getField(TestSpec.Fields.DYNAMIC_FEATURE_FLAGS).unwrap(),
        parser.getField(TestSpec.Fields.SKIP_ON_ATLAS).unwrap());
  }

  public Map<String, List<BsonDocument>> getCollections() {
    return this.collections;
  }

  public Map<String, List<BsonDocument>> getCollectionsForTestName(String testName) {
    return this.collections.entrySet().stream()
        .collect(
            Collectors.toMap(
                coll -> String.format("%s-%s", testName, coll.getKey()), Map.Entry::getValue));
  }

  public String getBaseCollection() {
    return this.baseCollection;
  }

  @Override
  public Type getType() {
    return Type.AGGREGATION;
  }

  @Override
  BsonDocument testSpecToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.COLLECTIONS, this.collections)
        .field(Fields.BASE_COLLECTION, this.baseCollection)
        .build();
  }
}
