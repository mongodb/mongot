package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;

public abstract class TestSpec implements DocumentEncodable {
  static class Fields {
    static final Field.Required<String> NAME = Field.builder("name").stringField().required();

    static final Field.Optional<String> DESCRIPTION =
        Field.builder("description").stringField().optional().noDefault();

    // If present, this test spec is based on another test spec. This test spec is constructed by
    // overlaying the base test spec JSON with this test spec JSON.
    static final Field.Optional<String> BASED_ON_TEST_SPEC =
        Field.builder("basedOnTestSpec").stringField().optional().noDefault();

    static final Field.Optional<IndexSpec> INDEX =
        Field.builder("index")
            .classField(IndexSpec::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<BsonDocument> QUERY =
        Field.builder("query").documentField().optional().noDefault();

    static final Field.Optional<BsonDocument> VECTOR_SEARCH =
        Field.builder("vectorSearch").documentField().optional().noDefault();

    static final Field.Optional<List<BsonDocument>> AGGREGATION =
        Field.builder("aggregation").documentField().asList().optional().noDefault();

    static final Field.Required<Result> RESULT =
        Field.builder("result").classField(Result::fromBson).disallowUnknownFields().required();

    static final Field.Optional<Map<String, ShardZoneConfig>> SHARD_ZONE_CONFIGS =
        Field.builder("shardZoneConfigs")
            .mapOf(
                Value.builder()
                    .classValue(ShardZoneConfig::fromBson)
                    .disallowUnknownFields()
                    .required())
            .optional()
            .noDefault();

    static final Field.Optional<MongoDbVersionInfo> MIN_MONGODB_VERSION =
        Field.builder("minMongoDbVersion")
            .classField(MongoDbVersionInfo::fromBson)
            .optional()
            .noDefault();

    static final Field.Optional<MongoDbVersionInfo> MIN_SHARDED_MONGODB_VERSION =
        Field.builder("minShardedMongoDbVersion")
            .classField(MongoDbVersionInfo::fromBson)
            .optional()
            .noDefault();

    static final Field.Optional<List<DynamicFeatureFlagConfig>> DYNAMIC_FEATURE_FLAGS =
        Field.builder("dynamicFeatureFlags")
            .classField(DynamicFeatureFlagConfig::fromBson)
            .allowUnknownFields()
            .asList()
            .optional()
            .noDefault();

    /**
     * If true, this test will be skipped when running on Atlas. Useful for tests that require
     * specific DFF configurations that cannot be guaranteed on Atlas, since the engineer must
     * manually configure DFFs in Atlas before running the tests.
     */
    static final Field.WithDefault<Boolean> SKIP_ON_ATLAS =
        Field.builder("skipOnAtlas").booleanField().optional().withDefault(false);

    // If adding fields to this class, considering also adding them to the fingerprint definition in
    // //scripts/tools/find_duplicate_tests.py
  }

  public enum Type {
    AGGREGATION,
    QUERY,
    VECTOR
  }

  private final String name;
  private final Optional<String> description;
  private final Optional<String> basedOnTestSpec;
  private final Optional<IndexSpec> index;
  private final Optional<BsonDocument> query;
  private final Optional<List<BsonDocument>> aggregation;
  private final Result result;
  private final Optional<Map<String, ShardZoneConfig>> shardZoneConfigs;
  private final Optional<MongoDbVersionInfo> minMongoDbVersionInfo;
  private final Optional<MongoDbVersionInfo> minShardedMongoDbVersionInfo;
  private final Optional<List<DynamicFeatureFlagConfig>> dynamicFeatureFlags;
  private final boolean skipOnAtlas;

  TestSpec(
      String name,
      Optional<String> description,
      Optional<String> basedOnTestSpec,
      Optional<IndexSpec> index,
      Optional<BsonDocument> query,
      Optional<List<BsonDocument>> aggregation,
      Result result,
      Optional<Map<String, ShardZoneConfig>> shardZoneConfigs,
      Optional<MongoDbVersionInfo> minMongoDbVersionInfo,
      Optional<MongoDbVersionInfo> minShardedMongoDbVersionInfo,
      Optional<List<DynamicFeatureFlagConfig>> dynamicFeatureFlags,
      boolean skipOnAtlas) {
    this.name = name;
    this.description = description;
    this.basedOnTestSpec = basedOnTestSpec;
    this.index = index;
    this.query = query;
    this.aggregation = aggregation;
    this.result = result;
    this.shardZoneConfigs = shardZoneConfigs;
    this.minMongoDbVersionInfo = minMongoDbVersionInfo;
    this.minShardedMongoDbVersionInfo = minShardedMongoDbVersionInfo;
    this.dynamicFeatureFlags = normalizeDynamicFeatureFlagScopes(dynamicFeatureFlags);
    this.skipOnAtlas = skipOnAtlas;

    // Validate that DFF phases are only ENABLED or DISABLED for test specs.
    // CONTROLLED and UNSPECIFIED are not supported because tests should explicitly
    // test enabled or disabled behavior, not rollout percentage logic.
    validateDynamicFeatureFlagPhases(name, this.dynamicFeatureFlags);
  }

  private static Optional<List<DynamicFeatureFlagConfig>> normalizeDynamicFeatureFlagScopes(
      Optional<List<DynamicFeatureFlagConfig>> dynamicFeatureFlags) {
    return dynamicFeatureFlags.map(
        dffs ->
            dffs.stream()
                .map(
                    dff -> {
                      // Default to ORG scope if not specified, so cluster-invariant evaluation
                      // works
                      if (dff.scope() == DynamicFeatureFlagConfig.Scope.UNSPECIFIED) {
                        return new DynamicFeatureFlagConfig(
                            dff.featureFlagName(),
                            dff.phase(),
                            dff.allowedList(),
                            dff.blockedList(),
                            dff.rolloutPercentage(),
                            DynamicFeatureFlagConfig.Scope.ORG);
                      }
                      return dff;
                    })
                .toList());
  }

  private static void validateDynamicFeatureFlagPhases(
      String testName, Optional<List<DynamicFeatureFlagConfig>> dynamicFeatureFlags) {
    dynamicFeatureFlags.ifPresent(
        dffs -> {
          for (DynamicFeatureFlagConfig dff : dffs) {
            if (dff.phase() != DynamicFeatureFlagConfig.Phase.ENABLED
                && dff.phase() != DynamicFeatureFlagConfig.Phase.DISABLED) {
              throw new IllegalArgumentException(
                  String.format(
                      "Test '%s' has invalid DFF phase '%s' for flag '%s'. "
                          + "Only ENABLED or DISABLED phases are allowed in test specs. "
                          + "CONTROLLED and UNSPECIFIED are not supported.",
                      testName, dff.phase(), dff.featureFlagName()));
            }
          }
        });
  }

  static TestSpec fromBson(DocumentParser parser) throws BsonParseException {
    // Must have either a search query or an aggregation pipeline.
    var query = parser.getField(Fields.QUERY);
    var aggregation = parser.getField(Fields.AGGREGATION);
    var vector = parser.getField(Fields.VECTOR_SEARCH);

    parser.getGroup().exactlyOneOf(query, aggregation, vector);

    if (query.unwrap().isPresent()) {
      return SearchIndexQueryTestSpec.fromBson(parser);
    } else if (aggregation.unwrap().isPresent()) {
      return AggregationTestSpec.fromBson(parser);
    } else if (vector.unwrap().isEmpty()) {
      return VectorIndexQueryTestSpec.fromBson(parser);
    } else {
      return Check.unreachable();
    }
  }

  @Override
  public BsonDocument toBson() {
    var document =
        BsonDocumentBuilder.builder()
            .field(Fields.NAME, this.name)
            .field(Fields.DESCRIPTION, this.description)
            .field(Fields.BASED_ON_TEST_SPEC, this.basedOnTestSpec)
            .field(Fields.INDEX, this.index)
            .field(Fields.QUERY, this.query)
            .field(Fields.AGGREGATION, this.aggregation)
            .field(Fields.RESULT, this.result)
            .field(Fields.SHARD_ZONE_CONFIGS, this.shardZoneConfigs)
            .field(Fields.MIN_MONGODB_VERSION, this.minMongoDbVersionInfo)
            .field(Fields.MIN_SHARDED_MONGODB_VERSION, this.minMongoDbVersionInfo)
            .field(Fields.DYNAMIC_FEATURE_FLAGS, this.dynamicFeatureFlags)
            .field(Fields.SKIP_ON_ATLAS, this.skipOnAtlas)
            .build();

    document.putAll(this.testSpecToBson());
    return document;
  }

  public String getName() {
    return this.name;
  }

  public IndexSpec getIndex() {
    return this.index.orElse(SearchIndexSpec.EMPTY);
  }

  /** Get the index spec, with synonym collection name keys modified to be prefixed by testName. */
  public Optional<IndexSpec> getIndexForTestName(String testName) {
    // get index with synonym source collection names translated to be specific to this test name
    return this.index.map(
        indexSpec ->
            indexSpec.getType() == IndexSpec.Type.VECTOR_SEARCH
                ? indexSpec.asVector()
                : indexSpec.asSearch().withPrefixedSynonymMappingCollections(testName));
  }

  public VectorIndexSpec getVectorIndex() {
    return this.index.map(IndexSpec::asVector).orElse(VectorIndexSpec.EMPTY);
  }

  public BsonDocument getQuery() {
    return Check.isPresent(this.query, "query").clone();
  }

  public List<BsonDocument> getAggregation() {
    return Check.isPresent(this.aggregation, "aggregation");
  }

  public Result getResult() {
    return this.result;
  }

  public Optional<Map<String, ShardZoneConfig>> getShardZoneConfigs() {
    return this.shardZoneConfigs;
  }

  public Optional<MongoDbVersionInfo> getMinMongoDbVersionInfo() {
    return this.minMongoDbVersionInfo;
  }

  public Optional<MongoDbVersionInfo> getMinShardedMongoDbVersionInfo() {
    return this.minShardedMongoDbVersionInfo;
  }

  public Optional<List<DynamicFeatureFlagConfig>> getDynamicFeatureFlags() {
    return this.dynamicFeatureFlags;
  }

  /**
   * Returns true if this test should be skipped when running on Atlas.
   *
   * <p>Tests that require specific DFF configurations should set this to true if the engineer
   * cannot guarantee the Atlas environment has the required DFFs configured.
   */
  public boolean shouldSkipOnAtlas() {
    return this.skipOnAtlas;
  }

  public AggregationTestSpec asAggregationTestSpec() {
    Check.expectedType(Type.AGGREGATION, getType());
    return (AggregationTestSpec) this;
  }

  public SearchIndexQueryTestSpec asQueryTestSpec() {
    Check.expectedType(Type.QUERY, getType());
    return (SearchIndexQueryTestSpec) this;
  }

  public VectorIndexQueryTestSpec asVectorSearchSpec() {
    Check.expectedType(Type.VECTOR, getType());
    return (VectorIndexQueryTestSpec) this;
  }

  public boolean isQueryTestSpec() {
    return getType() == Type.QUERY;
  }

  public boolean isVectorSearch() {
    return this.query.map(document -> document.containsKey("queryVector")).orElse(false);
  }

  public boolean isLexicalVectorSearch() {
    return this.isQueryTestSpec()
        && this.query.map(document -> document.containsKey("vectorSearch")).orElse(false);
  }

  abstract BsonDocument testSpecToBson();

  public abstract Type getType();
}
