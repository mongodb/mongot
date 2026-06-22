package com.xgen.mongot.trace.parser;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Assert;
import org.junit.Test;

public class JsonConvertorTest {
  @Test
  public void testParseSampleLogs() throws Exception {
    String inputFilePath = "src/test/unit/resources/trace/parser/log-file-sample.txt";
    JSONParser parser = new JSONParser();
    String traceId1 = "ff1bab1f529eb1ad4a3d0c06044e99db";
    String traceId2 = "b76d8015ee13cbb155297b50800f7e69";
    JSONObject expectedTrace1 =
        (JSONObject)
            parser.parse(
                Files.readString(
                    Paths.get("src/test/unit/resources/trace/parser/" + "trace-1.json")));
    JSONObject expectedTrace2 =
        (JSONObject)
            parser.parse(
                Files.readString(
                    Paths.get("src/test/unit/resources/trace/parser/" + "trace-2.json")));
    Map<String, JSONObject> result = JsonConvertor.convert(inputFilePath, "test");
    Assert.assertEquals(expectedTrace1, result.get(traceId1));
    Assert.assertEquals(expectedTrace2, result.get(traceId2));
  }
}
