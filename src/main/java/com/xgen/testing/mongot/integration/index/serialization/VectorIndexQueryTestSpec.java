package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonString;

public class VectorIndexQueryTestSpec extends ExplainableTestSpec {

  public record VariantsConfig(
      List<VectorSearchTestVariant.Features> exclude, List<VariantResultOverride> overrides)
      implements DocumentEncodable {

    static class Fields {
      static final Field.WithDefault<List<VectorSearchTestVariant.Features>> EXCLUDE =
          Field.builder("exclude")
              .listOf(
                  Value.builder()
                      .enumValue(VectorSearchTestVariant.Features.class)
                      .asCamelCase()
                      .required())
              .optional()
              .withDefault(List.of());

      static final Field.WithDefault<List<VariantResultOverride>> OVERRIDES =
          Field.builder("overrides")
              .listOf(
                  Value.builder()
                      .classValue(VariantResultOverride::fromBson)
                      .disallowUnknownFields()
                      .required())
              .optional()
              .withDefault(List.of());
    }

    static VariantsConfig fromBson(DocumentParser parser) throws BsonParseException {
      return new VariantsConfig(
          parser.getField(Fields.EXCLUDE).unwrap(), parser.getField(Fields.OVERRIDES).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.EXCLUDE, this.exclude)
          .field(Fields.OVERRIDES, this.overrides)
          .build();
    }
  }

  public record VariantResultOverride(
      Optional<String> reason, VectorSearchTestVariant variant, Result result)
      implements DocumentEncodable {

    static class Fields {
      static final Field.Optional<String> REASON =
          Field.builder("reason").stringField().optional().noDefault();

      static final Field.Required<VectorSearchTestVariant> VARIANT =
          Field.builder("query")
              .classField(VectorSearchTestVariant::fromBson)
              .disallowUnknownFields()
              .required();

      static final Field.Required<Result> RESULT =
          Field.builder("result").classField(Result::fromBson).disallowUnknownFields().required();
    }

    static VariantResultOverride fromBson(DocumentParser parser) throws BsonParseException {
      return new VariantResultOverride(
          parser.getField(Fields.REASON).unwrap(),
          parser.getField(Fields.VARIANT).unwrap(),
          parser.getField(Fields.RESULT).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.REASON, this.reason)
          .field(Fields.VARIANT, this.variant)
          .field(Fields.RESULT, this.result)
          .build();
    }
  }

  static class Fields {
    static final Field.WithDefault<BsonDocument> EXPLAIN =
        Field.builder("explain")
            .documentField()
            .optional()
            .withDefault(new BsonDocument("verbosity", new BsonString("queryPlanner")));

    static final Field.WithDefault<List<BsonDocument>> DOCUMENTS =
        Field.builder("documents")
            .documentField()
            .asList()
            .optional()
            .withDefault(Collections.emptyList());

    static final Field.Required<VectorIndexSpec> INDEX =
        Field.builder("index")
            .classField(VectorIndexSpec::fromBson)
            .disallowUnknownFields()
            .required();

    static final Field.Optional<VariantsConfig> VARIANTS =
        Field.builder("variants")
            .classField(VariantsConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  private final List<VectorSearchTestVariant> variants;
  private final Map<VectorSearchTestVariant, Result> resultOverrides;
  private VectorIndexSpec indexSpec;
  private Result result;
  private BsonDocument explain;

  private VectorIndexQueryTestSpec(
      String name,
      Optional<String> description,
      Optional<String> basedOnTestSpec,
      BsonDocument explain,
      VectorIndexSpec index,
      List<BsonDocument> documents,
      Optional<BsonDocument> query,
      Result result,
      List<VectorSearchTestVariant> variants,
      Map<VectorSearchTestVariant, Result> resultOverrides,
      Optional<Map<String, ShardZoneConfig>> shardZoneConfigs,
      Optional<MongoDbVersionInfo> minMongoDbVersion,
      Optional<MongoDbVersionInfo> minShardedMongoDbVersion,
      Optional<List<DynamicFeatureFlagConfig>> dynamicFeatureFlags,
      boolean skipOnAtlas) {
    super(
        name,
        description,
        basedOnTestSpec,
        Optional.of(index),
        query,
        Optional.empty(),
        result,
        explain,
        documents,
        shardZoneConfigs,
        minMongoDbVersion,
        minShardedMongoDbVersion,
        dynamicFeatureFlags,
        skipOnAtlas);
    this.variants = variants;
    this.resultOverrides = resultOverrides;
    this.indexSpec = index;
    this.result = result;
    this.explain = explain;
  }

  public static VectorIndexQueryTestSpec fromBson(DocumentParser parser) throws BsonParseException {
    Optional<VariantsConfig> variantsConfigOpt = parser.getField(Fields.VARIANTS).unwrap();

    Map<VectorSearchTestVariant, Result> resultOverrides;
    List<VectorSearchTestVariant> variants;
    if (variantsConfigOpt.isEmpty()) {
      variants = List.of();
      resultOverrides = Map.of();

    } else {
      variants =
          VectorSearchTestVariant.resolvePossibleVariants(
              new HashSet<>(variantsConfigOpt.get().exclude()));
      resultOverrides =
          variantsConfigOpt.stream()
              .flatMap(config -> config.overrides().stream())
              .collect(Collectors.toMap((value -> value.variant), (value -> value.result)));
    }

    return new VectorIndexQueryTestSpec(
        parser.getField(TestSpec.Fields.NAME).unwrap(),
        parser.getField(TestSpec.Fields.DESCRIPTION).unwrap(),
        parser.getField(TestSpec.Fields.BASED_ON_TEST_SPEC).unwrap(),
        parser.getField(Fields.EXPLAIN).unwrap(),
        parser.getField(Fields.INDEX).unwrap(),
        parser.getField(Fields.DOCUMENTS).unwrap(),
        parser.getField(TestSpec.Fields.QUERY).unwrap(),
        parser.getField(TestSpec.Fields.RESULT).unwrap(),
        variants,
        resultOverrides,
        parser.getField(TestSpec.Fields.SHARD_ZONE_CONFIGS).unwrap(),
        parser.getField(TestSpec.Fields.MIN_MONGODB_VERSION).unwrap(),
        parser.getField(TestSpec.Fields.MIN_SHARDED_MONGODB_VERSION).unwrap(),
        parser.getField(TestSpec.Fields.DYNAMIC_FEATURE_FLAGS).unwrap(),
        parser.getField(TestSpec.Fields.SKIP_ON_ATLAS).unwrap());
  }

  @Override
  public Type getType() {
    return Type.VECTOR;
  }

  @Override
  public VectorIndexSpec getIndex() {
    return this.indexSpec;
  }

  public void setIndex(VectorIndexSpec indexSpec) {
    this.indexSpec = indexSpec;
  }

  @Override
  public Result getResult() {
    return this.result;
  }

  public void setResult(Result result) {
    this.result = result;
  }

  @Override
  public BsonDocument getExplain() {
    return this.explain;
  }

  public void setExplain(BsonDocument explain) {
    this.explain = explain;
  }

  public List<VectorSearchTestVariant> getVariants() {
    return this.variants;
  }

  public Map<VectorSearchTestVariant, Result> getResultOverrides() {
    return this.resultOverrides;
  }

  @Override
  BsonDocument testSpecToBson() {
    return BsonDocumentBuilder.builder()
        .field(SearchIndexQueryTestSpec.Fields.EXPLAIN, this.explain)
        .field(SearchIndexQueryTestSpec.Fields.DOCUMENTS, this.documents)
        .build();
  }

  @Override
  public SearchStage getSearchStage() {
    return SearchStage.VECTOR_SEARCH;
  }
}
