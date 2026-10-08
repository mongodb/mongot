package com.xgen.testing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.Encodable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Assert;

/**
 * Provides a way to test the correctness of serialization of objects of type T to BSON/JSON.
 *
 * <p>Test cases are loaded from a JSON file. We then run individual tests by providing an object of
 * type {@link T} that will be serialized and compared to what we have as expected output in the
 * test suite. See {@link BsonSerializationTestSuite#runTest(TestSpec)}.
 */
public class BsonSerializationTestSuite<T> {
  public interface BsonSerializer<T> {
    BsonValue serialize(T input);
  }

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().registerModule(new Jdk8Module());

  private final List<SuiteTestCase> testCases;
  private final BsonSerializer<T> serializer;

  private BsonSerializationTestSuite(List<SuiteTestCase> testCases, BsonSerializer<T> serializer) {
    this.testCases = testCases;
    this.serializer = serializer;
  }

  /** Creates a BsonSerializationTestSuite for a class that is Encodable. */
  public static <T extends Encodable> BsonSerializationTestSuite<T> fromEncodable(
      String suitePath, String suiteName) {
    return load(suitePath, suiteName, Encodable::toBson);
  }

  /** Creates a BsonSerializationTestSuite for a class with the supplied encoding function. */
  public static <T> BsonSerializationTestSuite<T> load(
      String suitePath, String suiteName, BsonSerializer<T> encoder) {
    var testSuite = JsonTestSuite.load(Paths.get(suitePath, suiteName + ".json"));
    return new BsonSerializationTestSuite<>(testSuite.tests, encoder);
  }

  /**
   * Runs the named test case: arms the lossless writer for the duration of the assertions and
   * restores the process default in a finally, so a failed assertion cannot leak the armed state to
   * later tests in the same JVM.
   *
   * <p>Arming happens per test rather than in the constructor because suites are built in static
   * initializers — a constructor-side set would run once at class-load and another test resetting
   * the supplier afterwards would strand an already-constructed suite. (CLOUDP-424155)
   */
  public void runTest(TestSpec<T> testSpec) throws Exception {
    JsonCodec.setLosslessWriterSupplier(() -> true);
    try {
      SuiteTestCase testCase =
          this.testCases.stream()
              .filter(t -> t.description.equals(testSpec.getName()))
              .findFirst()
              .orElseThrow(
                  () ->
                      new AssertionError(
                          String.format(
                              "could not find valid test case \"%s\"", testSpec.getName())));

      testBson(testSpec, testCase);
      testJson(testSpec, testCase);
    } finally {
      JsonCodec.setLosslessWriterSupplier(() -> false);
    }
  }

  private void testBson(TestSpec<T> testSpec, SuiteTestCase testCase) throws Exception {
    BsonValue expected = testCase.bson;
    BsonValue result = this.serializer.serialize(testSpec.getValue());
    String prettyExpected = bsonValueToJson(expected).toPrettyString();
    String prettyResult = bsonValueToJson(result).toPrettyString();
    String message =
        String.format(
            "%s BSON serialization did not match expected\nexpected:\n%s\nresult:\n%s",
            testSpec.getName(), prettyExpected, prettyResult);
    Assert.assertEquals(message, expected, result);
  }

  private void testJson(TestSpec<T> testSpec, SuiteTestCase testCase) throws Exception {
    JsonNode expected = testCase.json;
    BsonValue bsonResult = this.serializer.serialize(testSpec.getValue());
    JsonNode result = bsonValueToJson(bsonResult);
    String message =
        String.format(
            "%s JSON serialization did not match expected\nexpected:\n%s\nresult:\n%s",
            testSpec.getName(), expected.toPrettyString(), result.toPrettyString());
    Assert.assertEquals(message, expected, result);
  }

  private JsonNode bsonValueToJson(BsonValue value) throws Exception {
    if (value.isDocument()) {
      String json = JsonCodec.toJson(value.asDocument());
      return OBJECT_MAPPER.readTree(json);
    }

    // The bson library will only encode documents, so in order to serialize a non-document value,
    // wrap it in a document, and pull the value out once converted to JSON.
    BsonDocument wrapper = new BsonDocument("wrapped", value);
    String wrapperJson = JsonCodec.toJson(wrapper);
    JsonNode parsedWrapper = OBJECT_MAPPER.readTree(wrapperJson);
    return parsedWrapper.get("wrapped");
  }

  public static class TestSpec<T> {

    private final String name;
    private final T value;

    private TestSpec(String name, T value) {
      this.name = name;
      this.value = value;
    }

    public static <T> TestSpec<T> create(String name, T value) {
      return new TestSpec<>(name, value);
    }

    private String getName() {
      return this.name;
    }

    private T getValue() {
      return this.value;
    }

    @Override
    public String toString() {
      return this.name;
    }
  }

  private static class JsonTestSuite {

    private final List<SuiteTestCase> tests;

    @JsonCreator
    public JsonTestSuite(
        @JsonProperty(value = "tests", required = true) List<SuiteTestCase> tests) {
      this.tests = tests;
    }

    private static JsonTestSuite load(Path path) {
      try {
        String json = Files.readString(path);
        return OBJECT_MAPPER.readValue(json, JsonTestSuite.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** One test case from our suite. */
  private static class SuiteTestCase {

    public final String description;
    public final JsonNode json;
    public final BsonValue bson;

    @JsonCreator
    public SuiteTestCase(
        @JsonProperty(value = "description", required = true) String description,
        @JsonProperty("value") JsonNode value,
        @JsonProperty("json") JsonNode json,
        @JsonProperty("bson") JsonNode bson) {
      this.description = description;

      Optional<JsonNode> valueOptional = Optional.ofNullable(value);
      Optional<JsonNode> jsonOptional = Optional.ofNullable(json);
      Optional<JsonNode> bsonOptional = Optional.ofNullable(bson);

      if (valueOptional.isPresent() && !valueOptional.get().isMissingNode()) {
        if (jsonOptional.isPresent() && !jsonOptional.get().isMissingNode()
            || bsonOptional.isPresent() && !bsonOptional.get().isMissingNode()) {
          throw new RuntimeException("cannot supply explicit json/bson and value");
        }

        this.json = value;
        this.bson = jsonToBson(value);
        return;
      }

      if (jsonOptional.isEmpty()
          || jsonOptional.get().isMissingNode()
          || bsonOptional.isEmpty()
          || bsonOptional.get().isMissingNode()) {
        throw new RuntimeException("must supply both explicit json and bson");
      }
      this.json = json;
      this.bson = jsonToBson(bson);
    }

    private static BsonValue jsonToBson(JsonNode object) {
      // org.bson only supplies utilities for parsing BsonDocuments from JSON, but we want the value
      // to be able to be any JSON type.
      JsonNode wrapper = new ObjectNode(JsonNodeFactory.instance).set("wrapped", object);
      String json = wrapper.toString();
      BsonDocument bson = BsonDocument.parse(json);
      return bson.get("wrapped");
    }
  }
}
