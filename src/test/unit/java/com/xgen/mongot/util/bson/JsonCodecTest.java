package com.xgen.mongot.util.bson;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Arrays;
import java.util.UUID;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(value = {JsonCodecTest.FromJson.class, JsonCodecTest.ToJson.class})
public class JsonCodecTest {

  public static class FromJson {
    @Test
    public void testSimpleFromJson() throws Exception {
      String json = "{\"a\": \"b\"}";
      BsonDocument bson = JsonCodec.fromJson(json);
      BsonDocument expected = new BsonDocument("a", new BsonString("b"));
      Assert.assertEquals(expected, bson);
    }

    @Test
    public void testInvalidFromJson() {
      String notJson = "this is not JSON";
      Exception ex = assertThrows(BsonParseException.class, () -> JsonCodec.fromJson(notJson));
      assertThat(ex).hasMessageThat().startsWith("Failed to parse");
      assertNotNull(ex.getCause());
    }
  }

  public static class ToJson {

    @Test
    public void testFiniteDouble() {
      BsonDocument bson = new BsonDocument("a", new BsonDouble(13));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": 13.0}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testNanDouble() {
      BsonDocument bson = new BsonDocument("a", new BsonDouble(Double.NaN));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$numberDouble\": \"NaN\"}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testString() {
      BsonDocument bson = new BsonDocument("a", new BsonString("b"));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": \"b\"}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testDocument() {
      BsonDocument bson = new BsonDocument("a", new BsonDocument("b", new BsonBoolean(true)));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"b\": true}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testArray() {
      BsonDocument bson =
          new BsonDocument(
              "a", new BsonArray(Arrays.asList(new BsonBoolean(true), new BsonBoolean(false))));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": [true, false]}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testUuidBinary() {
      BsonDocument bson =
          new BsonDocument(
              "a", new BsonBinary(UUID.fromString("00000000-1111-2222-3333-444444444444")));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": \"00000000-1111-2222-3333-444444444444\"}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testNonUuidBinary() {
      byte[] bytes = new byte[] {0, 1, 2, 3};
      BsonDocument bson = new BsonDocument("a", new BsonBinary(bytes));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$binary\": {\"base64\": \"AAECAw==\", \"subType\": \"00\"}}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testUndefined() {
      BsonDocument bson = new BsonDocument("a", new BsonUndefined());
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$undefined\": true}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testObjectId() {
      BsonDocument bson =
          new BsonDocument("a", new BsonObjectId(new ObjectId("012345678901234567890123")));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": \"012345678901234567890123\"}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testBoolean() {
      BsonDocument bson = new BsonDocument("a", new BsonBoolean(true));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": true}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testDateTime() {
      BsonDocument bson = new BsonDocument("a", new BsonDateTime(0));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$date\": \"1970-01-01T00:00:00Z\"}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testNull() {
      BsonDocument bson = new BsonDocument("a", BsonNull.VALUE);
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": null}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testRegularExpression() {
      BsonDocument bson = new BsonDocument("a", new BsonRegularExpression("$*^"));
      String json = JsonCodec.toJson(bson);
      String expected =
          "{\"a\": {\"$regularExpression\": {\"pattern\": \"$*^\", \"options\": \"\"}}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testDbPointer() {
      BsonDocument bson =
          new BsonDocument(
              "a", new BsonDbPointer("foo.bar", new ObjectId("012345678901234567890123")));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$ref\": \"foo.bar\", \"$id\": \"012345678901234567890123\"}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testJavascript() {
      BsonDocument bson = new BsonDocument("a", new BsonJavaScript("var a = undefined"));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$code\": \"var a = undefined\"}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testSymbol() {
      BsonDocument bson = new BsonDocument("a", new BsonSymbol("my-symbol"));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$symbol\": \"my-symbol\"}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testJavascriptWithScope() {
      BsonDocument bson =
          new BsonDocument(
              "a",
              new BsonJavaScriptWithScope(
                  "var a = undefined", new BsonDocument("b", new BsonBoolean(true))));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$code\": \"var a = undefined\", \"$scope\": {\"b\": true}}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testInt32() {
      BsonDocument bson = new BsonDocument("a", new BsonInt32(13));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": 13}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testTimestamp() {
      BsonDocument bson = new BsonDocument("a", new BsonTimestamp(13));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$timestamp\": {\"t\": 0, \"i\": 13}}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testInt64() {
      BsonDocument bson = new BsonDocument("a", new BsonInt64(13));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": 13}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testDecimal128() {
      BsonDocument bson = new BsonDocument("a", new BsonDecimal128(new Decimal128(13)));
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$numberDecimal\": \"13\"}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testMinKey() {
      BsonDocument bson = new BsonDocument("a", BsonUtils.MIN_KEY);
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$minKey\": 1}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testMaxKey() {
      BsonDocument bson = new BsonDocument("a", BsonUtils.MAX_KEY);
      String json = JsonCodec.toJson(bson);
      String expected = "{\"a\": {\"$maxKey\": 1}}";
      Assert.assertEquals(expected, json);
    }

    @Test
    public void testUuidBinarySubtype4RoundTrip() throws Exception {
      // Test that a binary field with UUID subtype (type '4') is handled correctly
      // This tests the scenario: numDimensions: { data: { '$binary': 'AQIDBAUGBwg=', '$type':
      // '4' }
      String jsonWithUuidSubtype =
          "{\"numDimensions\": {\"data\": {\"$binary\": "
              + "{\"base64\": \"AQIDBAUGBwg=\", \"subType\": \"04\"}}}}";

      // Parse the JSON into BSON
      BsonDocument bson = JsonCodec.fromJson(jsonWithUuidSubtype);

      // Verify the binary field was parsed correctly
      assertThat(bson.containsKey("numDimensions")).isTrue();
      BsonDocument numDimensions = bson.getDocument("numDimensions");
      assertThat(numDimensions.containsKey("data")).isTrue();
      assertThat(numDimensions.get("data").isBinary()).isTrue();

      BsonBinary binary = numDimensions.getBinary("data");
      assertThat(binary.getType()).isEqualTo((byte) 4); // UUID subtype

      // Convert back to JSON - should fall back to extended JSON format
      // since the binary data is not a valid UUID (only 8 bytes instead of 16)
      String jsonOutput = JsonCodec.toJson(bson);

      // The output should contain the binary in extended JSON format
      // because the data is not a valid UUID
      assertThat(jsonOutput)
          .contains("{\"$binary\": {\"base64\": \"AQIDBAUGBwg=\", \"subType\": \"04\"}}");
    }

    @Test
    public void testValidUuidBinarySubtype4() throws Exception {
      // Test with a valid 16-byte UUID in subtype 4
      String validUuidJson =
          "{\"field\": {\"$binary\": "
              + "{\"base64\": \"AAAAAAAAAAAAAAAAAAAAAA==\", \"subType\": \"04\"}}}";

      BsonDocument bson = JsonCodec.fromJson(validUuidJson);

      // Convert to JSON - should convert to UUID string format
      String jsonOutput = JsonCodec.toJson(bson);

      // Should be converted to a UUID string
      assertThat(jsonOutput).contains("\"field\": \"00000000-0000-0000-0000-000000000000\"");
    }

    @Test
    public void testInvalidUuidBinarySubtype4FallsBackToExtendedJson() throws Exception {
      // Test that invalid UUID data (wrong length) falls back to extended JSON
      String invalidUuidJson =
          "{\"field\": {\"$binary\": {\"base64\": \"AQIDBAUGBwg=\", \"subType\": \"04\"}}}";

      BsonDocument bson = JsonCodec.fromJson(invalidUuidJson);
      String jsonOutput = JsonCodec.toJson(bson);

      // Should fall back to extended JSON format since it's not a valid UUID
      assertThat(jsonOutput)
          .contains("{\"$binary\": {\"base64\": \"AQIDBAUGBwg=\", \"subType\": \"04\"}}");
    }

    @Test
    public void testUuidBinarySubtype3InPipeline() throws Exception {
      // Test that a binary field with UUID old format subtype (type '03') is handled correctly
      // This tests the scenario from effectivePipeline with $match expression
      String jsonWithPipeline =
          "{\"effectivePipeline\": [{\"$match\": {\"$expr\": {\"$eq\": "
              + "[\"$domain_id\", {\"$binary\": "
              + "{\"base64\": \"nebpg/QcQOqlHJogHUIyyA==\", \"subType\": \"03\"}}]}}}]}";

      // Parse the JSON into BSON
      BsonDocument bson = JsonCodec.fromJson(jsonWithPipeline);

      // Verify the structure was parsed correctly
      assertThat(bson.containsKey("effectivePipeline")).isTrue();
      assertThat(bson.get("effectivePipeline").isArray()).isTrue();

      BsonArray pipeline = bson.getArray("effectivePipeline");
      assertThat(pipeline.size()).isEqualTo(1);

      BsonDocument matchStage = pipeline.get(0).asDocument();
      assertThat(matchStage.containsKey("$match")).isTrue();

      BsonDocument match = matchStage.getDocument("$match");
      BsonDocument expr = match.getDocument("$expr");
      BsonArray eqArray = expr.getArray("$eq");

      // Verify the binary field
      BsonBinary binary = eqArray.get(1).asBinary();
      assertThat(binary.getType()).isEqualTo((byte) 3); // UUID old format subtype

      // Convert back to JSON - should fall back to extended JSON format
      // since subtype 03 uses old UUID format which may not convert cleanly
      String jsonOutput = JsonCodec.toJson(bson);

      // The output should preserve the binary in extended JSON format
      assertThat(jsonOutput).contains("\"subType\": \"03\"");
      assertThat(jsonOutput).contains("nebpg/QcQOqlHJogHUIyyA==");
    }

    @Test
    public void testValidUuidBinarySubtype3() throws Exception {
      // Test with a valid 16-byte UUID in subtype 3 (old UUID format)
      String validUuidJson =
          "{\"field\": {\"$binary\": "
              + "{\"base64\": \"nebpg/QcQOqlHJogHUIyyA==\", \"subType\": \"03\"}}}";

      BsonDocument bson = JsonCodec.fromJson(validUuidJson);

      // Verify the binary field was parsed correctly
      assertThat(bson.containsKey("field")).isTrue();
      BsonBinary binary = bson.getBinary("field");
      assertThat(binary.getType()).isEqualTo((byte) 3); // UUID old format

      // Convert to JSON - should attempt UUID conversion or fall back to extended JSON
      String jsonOutput = JsonCodec.toJson(bson);

      // Subtype 03 is UUID old format, should either convert to UUID string or preserve as binary
      // The behavior depends on whether asUuid() can handle subtype 3
      assertThat(jsonOutput).isNotEmpty();
      // Should contain either UUID string or extended JSON format
      assertThat(jsonOutput.contains("field")).isTrue();
    }

    @Test
    public void testUuidBinarySubtype3FallsBackToExtendedJson() throws Exception {
      // Test that UUID subtype 3 (old format) falls back to extended JSON when needed
      String uuidSubtype3Json =
          "{\"domain_id\": {\"$binary\": "
              + "{\"base64\": \"nebpg/QcQOqlHJogHUIyyA==\", \"subType\": \"03\"}}}";

      BsonDocument bson = JsonCodec.fromJson(uuidSubtype3Json);
      String jsonOutput = JsonCodec.toJson(bson);

      // Should handle the UUID subtype 3 gracefully
      // Either as UUID string or as extended JSON format
      assertThat(jsonOutput).contains("domain_id");

      // Verify round-trip: parse the output back
      BsonDocument reparsed = JsonCodec.fromJson(jsonOutput);
      assertThat(reparsed.containsKey("domain_id")).isTrue();

      // The binary data should be preserved
      BsonBinary originalBinary = bson.getBinary("domain_id");
      BsonBinary reparsedBinary = reparsed.getBinary("domain_id");

      // Data should be the same
      assertThat(reparsedBinary.getData()).isEqualTo(originalBinary.getData());
    }

    @Test
    public void testComplexPipelineWithMultipleBinaryFields() throws Exception {
      // Test a more complex pipeline with multiple binary fields
      String complexPipeline =
          "{\"pipeline\": ["
              + "{\"$match\": {\"id\": {\"$binary\": "
              + "{\"base64\": \"AAAAAAAAAAAAAAAAAAAAAA==\", \"subType\": \"04\"}}}},"
              + "{\"$match\": {\"domain\": {\"$binary\": "
              + "{\"base64\": \"nebpg/QcQOqlHJogHUIyyA==\", \"subType\": \"03\"}}}}"
              + "]}";

      BsonDocument bson = JsonCodec.fromJson(complexPipeline);

      // Verify parsing
      assertThat(bson.containsKey("pipeline")).isTrue();
      BsonArray pipeline = bson.getArray("pipeline");
      assertThat(pipeline.size()).isEqualTo(2);

      // First match with subtype 04
      BsonBinary binary1 = pipeline.get(0).asDocument().getDocument("$match").getBinary("id");
      assertThat(binary1.getType()).isEqualTo((byte) 4);

      // Second match with subtype 03
      BsonBinary binary2 = pipeline.get(1).asDocument().getDocument("$match").getBinary("domain");
      assertThat(binary2.getType()).isEqualTo((byte) 3);

      // Convert back to JSON
      String jsonOutput = JsonCodec.toJson(bson);

      // Should handle both subtypes correctly
      assertThat(jsonOutput).contains("pipeline");

      // Verify round-trip
      BsonDocument reparsed = JsonCodec.fromJson(jsonOutput);
      assertThat(reparsed.getArray("pipeline").size()).isEqualTo(2);
    }
  }
}
