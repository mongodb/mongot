package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonString;

public class SearchIndexQueryTestSpec extends ExplainableTestSpec {
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

    static final Field.WithDefault<Map<String, List<BsonDocument>>> SYNONYM_DOCUMENTS =
        Field.builder("synonymDocuments")
            .documentField()
            .asList()
            .asMap()
            .optional()
            .withDefault(Collections.emptyMap());
  }

  private final Map<String, List<BsonDocument>> synonymDocuments;

  private SearchIndexQueryTestSpec(
      String name,
      Optional<String> description,
      Optional<String> basedOnTestSpec,
      BsonDocument explain,
      Optional<IndexSpec> index,
      List<BsonDocument> documents,
      Map<String, List<BsonDocument>> synonymDocuments,
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
        explain,
        documents,
        shardZoneConfigs,
        minMongoDbVersion,
        minShardedMongoDbVersion,
        dynamicFeatureFlags,
        skipOnAtlas);
    this.synonymDocuments = synonymDocuments;
  }

  static SearchIndexQueryTestSpec fromBson(DocumentParser parser) throws BsonParseException {
    return new SearchIndexQueryTestSpec(
        parser.getField(TestSpec.Fields.NAME).unwrap(),
        parser.getField(TestSpec.Fields.DESCRIPTION).unwrap(),
        parser.getField(TestSpec.Fields.BASED_ON_TEST_SPEC).unwrap(),
        parser.getField(Fields.EXPLAIN).unwrap(),
        parser.getField(TestSpec.Fields.INDEX).unwrap(),
        parser.getField(Fields.DOCUMENTS).unwrap(),
        parser.getField(Fields.SYNONYM_DOCUMENTS).unwrap(),
        parser.getField(TestSpec.Fields.QUERY).unwrap(),
        parser.getField(TestSpec.Fields.AGGREGATION).unwrap(),
        parser.getField(TestSpec.Fields.RESULT).unwrap(),
        parser.getField(TestSpec.Fields.SHARD_ZONE_CONFIGS).unwrap(),
        parser.getField(TestSpec.Fields.MIN_MONGODB_VERSION).unwrap(),
        parser.getField(TestSpec.Fields.MIN_SHARDED_MONGODB_VERSION).unwrap(),
        parser.getField(TestSpec.Fields.DYNAMIC_FEATURE_FLAGS).unwrap(),
        parser.getField(TestSpec.Fields.SKIP_ON_ATLAS).unwrap());
  }

  @Override
  public SearchStage getSearchStage() {
    return isVectorSearch() ? SearchStage.VECTOR_SEARCH : SearchStage.SEARCH;
  }

  public Map<String, List<BsonDocument>> getSynonymDocuments() {
    return this.synonymDocuments;
  }

  /** Get synonym collections, with collection name keys modified to be prefixed by testName. */
  public Map<String, List<BsonDocument>> getSynonymDocumentsForTestName(String testName) {
    return this.synonymDocuments.entrySet().stream()
        .collect(
            Collectors.toMap(
                collName -> SearchIndexSpec.synonymCollectionName(testName, collName.getKey()),
                Map.Entry::getValue));
  }

  @Override
  public Type getType() {
    return Type.QUERY;
  }

  @Override
  BsonDocument testSpecToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.EXPLAIN, this.explain)
        .field(Fields.DOCUMENTS, this.documents)
        .field(Fields.SYNONYM_DOCUMENTS, this.synonymDocuments)
        .build();
  }
}
