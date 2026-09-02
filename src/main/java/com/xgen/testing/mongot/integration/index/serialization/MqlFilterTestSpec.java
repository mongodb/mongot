package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.testing.mongot.index.definition.BooleanFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.KnnVectorFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;

public class MqlFilterTestSpec implements DocumentEncodable {

  public static final String VECTOR_FIELD_NAME = "vectorField";

  public static final BsonArray VECTOR =
      new BsonArray(List.of(new BsonDouble(1.0), new BsonDouble(2.0), new BsonDouble(3.0)));

  private static final String FILTER_FIELD_NAME = "filterField";

  private static final String SUB_FIELD_NAME = "subField";

  private static final String ZERO_FIELD_NAME = "0";
  private static final SearchIndexDefinition SEARCH_INDEX =
      SearchIndexDefinitionBuilder.builder()
          .defaultMetadata()
          .mappings(
              DocumentFieldDefinitionBuilder.builder()
                  .dynamic(false)
                  .field(
                      VECTOR_FIELD_NAME,
                      FieldDefinitionBuilder.builder()
                          .knnVector(
                              KnnVectorFieldDefinitionBuilder.builder()
                                  .dimensions(3)
                                  .similarity(VectorSimilarity.EUCLIDEAN)
                                  .build())
                          .build())
                  .field(
                      FILTER_FIELD_NAME,
                      FieldDefinitionBuilder.builder()
                          .bool(BooleanFieldDefinitionBuilder.builder().build())
                          .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
                          .token(TokenFieldDefinitionBuilder.builder().build())
                          .document(
                              DocumentFieldDefinitionBuilder.builder()
                                  .field(
                                      ZERO_FIELD_NAME,
                                      FieldDefinitionBuilder.builder()
                                          .number(
                                              NumericFieldDefinitionBuilder.builder()
                                                  .buildNumberField())
                                          .build())
                                  .field(
                                      SUB_FIELD_NAME,
                                      FieldDefinitionBuilder.builder()
                                          .number(
                                              NumericFieldDefinitionBuilder.builder()
                                                  .buildNumberField())
                                          .build())
                                  .build())
                          .build())
                  .field(
                      SUB_FIELD_NAME,
                      FieldDefinitionBuilder.builder()
                          .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
                          .build())
                  .build())
          .build();

  static class Fields {
    static final Field.Required<String> NAME = Field.builder("name").stringField().required();

    static final Field.Optional<String> DESCRIPTION =
        Field.builder("description").stringField().optional().noDefault();

    static final Field.WithDefault<List<BsonDocument>> DOCUMENTS =
        Field.builder("documents")
            .documentField()
            .asList()
            .optional()
            .withDefault(Collections.emptyList());

    static final Field.Required<BsonDocument> FILTER =
        Field.builder("filter").documentField().required();

    static final Field.Required<MqlFilterTestResult> RESULT =
        Field.builder("result")
            .classField(MqlFilterTestResult::fromBson)
            .disallowUnknownFields()
            .required();

    static final Field.Optional<MongoDbVersionInfo> MIN_MONGODB_VERSION =
        Field.builder("minMongoDbVersion")
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

    static final Field.Optional<List<String>> EXPECTED_OPTIMIZER_RULES_FIRED =
        Field.builder("expectedOptimizerRulesFired").stringField().asList().optional().noDefault();
  }

  private final String name;
  private final Optional<String> description;
  private final SearchIndexDefinition searchIndexDefinition;
  private final List<BsonDocument> documents;
  private final BsonDocument filter;
  private final MqlFilterTestResult result;
  private final Optional<MongoDbVersionInfo> minMongoDbVersionInfo;
  private final Optional<List<DynamicFeatureFlagConfig>> dynamicFeatureFlags;
  private final Optional<List<String>> expectedOptimizerRulesFired;

  MqlFilterTestSpec(
      String name,
      Optional<String> description,
      List<BsonDocument> documents,
      BsonDocument filter,
      MqlFilterTestResult result,
      Optional<MongoDbVersionInfo> minMongoDbVersionInfo,
      Optional<List<DynamicFeatureFlagConfig>> dynamicFeatureFlags,
      Optional<List<String>> expectedOptimizerRulesFired) {
    this.name = name;
    this.description = description;
    this.documents = documents;
    this.filter = filter;
    this.result = result;
    this.minMongoDbVersionInfo = minMongoDbVersionInfo;
    this.dynamicFeatureFlags = dynamicFeatureFlags;
    this.expectedOptimizerRulesFired = expectedOptimizerRulesFired;

    // add the vector field to each document
    for (var document : this.documents) {
      document.append(VECTOR_FIELD_NAME, VECTOR);
    }

    this.searchIndexDefinition = SEARCH_INDEX;
  }

  static MqlFilterTestSpec fromBson(DocumentParser parser) throws BsonParseException {
    return new MqlFilterTestSpec(
        parser.getField(Fields.NAME).unwrap(),
        parser.getField(Fields.DESCRIPTION).unwrap(),
        parser.getField(Fields.DOCUMENTS).unwrap(),
        parser.getField(Fields.FILTER).unwrap(),
        parser.getField(Fields.RESULT).unwrap(),
        parser.getField(Fields.MIN_MONGODB_VERSION).unwrap(),
        parser.getField(Fields.DYNAMIC_FEATURE_FLAGS).unwrap(),
        parser.getField(Fields.EXPECTED_OPTIMIZER_RULES_FIRED).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    var document =
        BsonDocumentBuilder.builder()
            .field(Fields.NAME, this.name)
            .field(Fields.DESCRIPTION, this.description)
            .field(Fields.DOCUMENTS, this.documents)
            .field(Fields.FILTER, this.filter)
            .field(Fields.RESULT, this.result)
            .field(Fields.MIN_MONGODB_VERSION, this.minMongoDbVersionInfo)
            .field(Fields.DYNAMIC_FEATURE_FLAGS, this.dynamicFeatureFlags)
            .field(Fields.EXPECTED_OPTIMIZER_RULES_FIRED, this.expectedOptimizerRulesFired)
            .build();
    return document;
  }

  public BsonDocument getFilter() {
    return this.filter;
  }

  public MqlFilterTestPipelineResult getVectorSearchResult() {
    return this.result.getVectorSearchResult();
  }

  public MqlFilterTestPipelineResult getMatchResult() {
    return this.result.getMatchResult();
  }

  public SearchIndexDefinition getIndex() {
    return this.searchIndexDefinition;
  }

  public String getName() {
    return this.name;
  }

  public Optional<MongoDbVersionInfo> getMinMongoDbVersionInfo() {
    return this.minMongoDbVersionInfo;
  }

  public List<BsonDocument> getDocuments() {
    return this.documents;
  }

  public Optional<List<DynamicFeatureFlagConfig>> getDynamicFeatureFlags() {
    return this.dynamicFeatureFlags;
  }

  public Optional<List<String>> getExpectedOptimizerRulesFired() {
    return this.expectedOptimizerRulesFired;
  }
}
