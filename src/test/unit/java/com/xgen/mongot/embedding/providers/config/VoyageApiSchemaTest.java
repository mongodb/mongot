package com.xgen.mongot.embedding.providers.config;

import static com.xgen.mongot.embedding.providers.clients.VoyageClient.VOYAGE_API_FLEX_TIER;
import static com.xgen.mongot.embedding.providers.configs.VoyageApiSchema.EmbedResponse;
import static com.xgen.testing.BsonDeserializationTestSuite.fromRootDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.mongot.embedding.providers.configs.VoyageApiSchema.EmbedRequest;
import com.xgen.mongot.embedding.providers.configs.VoyageApiSchema.EmbedUsage;
import com.xgen.mongot.embedding.providers.configs.VoyageApiSchema.EmbedVector;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonDeserializationTestSuite.TestSpecWrapper;
import com.xgen.testing.BsonSerializationTestSuite;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      VoyageApiSchemaTest.ResponseDeserializationTest.class,
      VoyageApiSchemaTest.ResponseSerializationTest.class,
      VoyageApiSchemaTest.RequestSerializationTest.class
    })
public class VoyageApiSchemaTest {
  @RunWith(Parameterized.class)
  public static class ResponseDeserializationTest {
    private static final String SUITE_NAME = "voyage-api-schema-response-deserialization";
    private static final BsonDeserializationTestSuite<EmbedResponse> TEST_SUITE =
        fromRootDocument(
            "src/test/unit/resources/embedding/providers/config",
            SUITE_NAME,
            bsonDocument ->
                EmbedResponse.fromBson(
                    BsonDocumentParser.fromRoot(bsonDocument).allowUnknownFields(true).build()));

    private final TestSpecWrapper<EmbedResponse> testSpec;

    public ResponseDeserializationTest(TestSpecWrapper<EmbedResponse> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<EmbedResponse>> data() {
      return TEST_SUITE.withExamples(fullResponse(), partialResponse(), emptyListResponse());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<EmbedResponse> fullResponse() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "voyage-3-large response",
          new EmbedResponse(
              "list",
              List.of(
                  new EmbedVector(
                      "embedding",
                      Vector.fromFloats(new float[] {1f, 2f, 3f}, FloatVector.OriginalType.NATIVE),
                      0),
                  new EmbedVector(
                      "embedding",
                      Vector.fromFloats(new float[] {1f, 2f, 3f}, FloatVector.OriginalType.NATIVE),
                      1),
                  new EmbedVector(
                      "embedding",
                      Vector.fromFloats(new float[] {1f, 2f, 3f}, FloatVector.OriginalType.NATIVE),
                      2)),
              new EmbedUsage(100)));
    }

    private static BsonDeserializationTestSuite.ValidSpec<EmbedResponse> partialResponse() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "partial response without model",
          new EmbedResponse(
              "list",
              List.of(
                  new EmbedVector(
                      "embedding",
                      Vector.fromFloats(new float[] {4f, 5f, 6f}, FloatVector.OriginalType.NATIVE),
                      0),
                  new EmbedVector(
                      "embedding",
                      Vector.fromFloats(new float[] {4f, 5f, 6f}, FloatVector.OriginalType.NATIVE),
                      1),
                  new EmbedVector(
                      "embedding",
                      Vector.fromFloats(new float[] {4f, 5f, 6f}, FloatVector.OriginalType.NATIVE),
                      2)),
              new EmbedUsage(200)));
    }

    private static BsonDeserializationTestSuite.ValidSpec<EmbedResponse> emptyListResponse() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "response with empty vectors", new EmbedResponse("list", List.of(), new EmbedUsage(0)));
    }
  }

  @RunWith(Parameterized.class)
  public static class ResponseSerializationTest {
    private static final String SUITE_NAME = "voyage-api-schema-response-serialization";
    private static final BsonSerializationTestSuite<EmbedResponse> TEST_SUITE =
        fromEncodable("src/test/unit/resources/embedding/providers/config", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<EmbedResponse> testSpec;

    public ResponseSerializationTest(BsonSerializationTestSuite.TestSpec<EmbedResponse> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<EmbedResponse>> data() {
      return List.of(fullResponse());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<EmbedResponse> fullResponse() {
      return BsonSerializationTestSuite.TestSpec.create(
          "voyage-3-large response",
          new EmbedResponse(
              "list",
              List.of(
                  new EmbedVector(
                      "embedding",
                      Vector.fromFloats(new float[] {1f, 2f, 3f}, FloatVector.OriginalType.NATIVE),
                      0),
                  new EmbedVector(
                      "embedding",
                      Vector.fromFloats(new float[] {1f, 2f, 3f}, FloatVector.OriginalType.NATIVE),
                      1),
                  new EmbedVector(
                      "embedding",
                      Vector.fromFloats(new float[] {1f, 2f, 3f}, FloatVector.OriginalType.NATIVE),
                      2)),
              new EmbedUsage(100)));
    }
  }

  @RunWith(Parameterized.class)
  public static class RequestSerializationTest {
    private static final String SUITE_NAME = "voyage-api-schema-request-serialization";
    private static final BsonSerializationTestSuite<EmbedRequest> TEST_SUITE =
        fromEncodable("src/test/unit/resources/embedding/providers/config", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<EmbedRequest> testSpec;

    public RequestSerializationTest(BsonSerializationTestSuite.TestSpec<EmbedRequest> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<EmbedRequest>> data() {
      return List.of(
          v3LargeQueryRequest(),
          v3LiteDocumentRequest(),
          v3LargeCollectionScanRequest(),
          v4LargeQueryRequestWithMetadata());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<EmbedRequest> v3LargeQueryRequest() {
      return BsonSerializationTestSuite.TestSpec.create(
          "voyage-3-large query request",
          new EmbedRequest(
              "voyage-3-large",
              "query",
              List.of("one", "two", "three"),
              false,
              Optional.empty(),
              Optional.empty()));
    }

    private static BsonSerializationTestSuite.TestSpec<EmbedRequest> v3LiteDocumentRequest() {
      return BsonSerializationTestSuite.TestSpec.create(
          "voyage-3-lite index request",
          new EmbedRequest(
              "voyage-3-lite",
              "document",
              List.of("four", "five", "six"),
              true,
              Optional.empty(),
              Optional.empty()));
    }

    private static BsonSerializationTestSuite.TestSpec<EmbedRequest>
        v4LargeQueryRequestWithMetadata() {
      BsonDocument metadata = new BsonDocument();
      metadata.put("database", new BsonString("mydb"));
      metadata.put("collectionName", new BsonString("mycoll"));
      metadata.put("indexName", new BsonString("myindex"));
      return BsonSerializationTestSuite.TestSpec.create(
          "voyage-4-large query request with metadata",
          new EmbedRequest(
              "voyage-4-large",
              "query",
              List.of("one", "two", "three"),
              false,
              Optional.of(metadata),
              Optional.empty()));
    }

    private static BsonSerializationTestSuite.TestSpec<EmbedRequest>
        v3LargeCollectionScanRequest() {
      return BsonSerializationTestSuite.TestSpec.create(
          "voyage-3-large collection scan request with service_tier",
          new EmbedRequest(
              "voyage-3-large",
              "document",
              List.of("one", "two"),
              "base64",
              true,
              Optional.empty(),
              Optional.of(VOYAGE_API_FLEX_TIER)));
    }
  }
}
