package com.xgen.testing.mongot.integration.index.serialization;

import com.google.errorprone.annotations.CheckReturnValue;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class MqlFilterTestSuite implements DocumentEncodable {
  static class Fields {
    static final Field.Required<List<MqlFilterTestSpec>> TESTS =
        Field.builder("tests")
            .classField(MqlFilterTestSpec::fromBson)
            .disallowUnknownFields()
            .asList()
            .required();

    static final Field.Optional<List<DynamicFeatureFlagConfig>> DYNAMIC_FEATURE_FLAGS =
        Field.builder("dynamicFeatureFlags")
            .classField(DynamicFeatureFlagConfig::fromBson)
            .allowUnknownFields()
            .asList()
            .optional()
            .noDefault();
  }

  private final List<MqlFilterTestSpec> tests;
  private final Optional<List<DynamicFeatureFlagConfig>> dynamicFeatureFlags;

  private MqlFilterTestSuite(
      List<MqlFilterTestSpec> tests, Optional<List<DynamicFeatureFlagConfig>> dynamicFeatureFlags) {
    this.tests = tests;
    this.dynamicFeatureFlags = dynamicFeatureFlags;
  }

  /** Deserializes the suite from JSON, merging any suite-level DFFs into each test spec. */
  public static MqlFilterTestSuite fromJson(String json) throws BsonParseException {
    BsonDocument document = JsonCodec.fromJson(json);

    String suiteFieldName = Fields.DYNAMIC_FEATURE_FLAGS.getName();
    String testFieldName = MqlFilterTestSpec.Fields.DYNAMIC_FEATURE_FLAGS.getName();
    if (document.containsKey(suiteFieldName) && document.get(suiteFieldName).isArray()) {
      BsonArray fileLevelDffs = document.getArray(suiteFieldName);
      BsonArray testSpecList = document.getArray(Fields.TESTS.getName());
      for (int i = 0; i < testSpecList.size(); i++) {
        BsonDocument testSpecDoc = testSpecList.get(i).asDocument().clone();
        BsonArray testDffs =
            Optional.ofNullable(testSpecDoc.get(testFieldName))
                .filter(BsonValue::isArray)
                .map(BsonValue::asArray)
                .orElseGet(BsonArray::new);
        testSpecDoc.put(testFieldName, mergeDffs(testDffs, fileLevelDffs));
        testSpecList.set(i, testSpecDoc);
      }
      document.put(Fields.TESTS.getName(), testSpecList);
    }

    try (var parser = BsonDocumentParser.fromRoot(document).allowUnknownFields(true).build()) {
      return MqlFilterTestSuite.fromBson(parser);
    }
  }

  private static MqlFilterTestSuite fromBson(DocumentParser parser) throws BsonParseException {
    return new MqlFilterTestSuite(
        parser.getField(Fields.TESTS).unwrap(),
        parser.getField(Fields.DYNAMIC_FEATURE_FLAGS).unwrap());
  }

  @CheckReturnValue
  private static BsonArray mergeDffs(BsonArray testDffs, BsonArray fileDffs) {
    Set<String> testFlagNames = new HashSet<>();
    for (BsonValue entry : testDffs) {
      testFlagNames.add(entry.asDocument().getString("featureFlagName").getValue());
    }
    BsonArray merged = testDffs.clone();
    for (BsonValue entry : fileDffs) {
      String flagName = entry.asDocument().getString("featureFlagName").getValue();
      if (!testFlagNames.contains(flagName)) {
        merged.add(entry);
      }
    }
    return merged;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.TESTS, this.tests)
        .field(Fields.DYNAMIC_FEATURE_FLAGS, this.dynamicFeatureFlags)
        .build();
  }

  public List<MqlFilterTestSpec> getTests() {
    return this.tests;
  }

  public Optional<List<DynamicFeatureFlagConfig>> getDynamicFeatureFlags() {
    return this.dynamicFeatureFlags;
  }
}
