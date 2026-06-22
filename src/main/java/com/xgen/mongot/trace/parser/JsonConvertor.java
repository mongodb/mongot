package com.xgen.mongot.trace.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.Var;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonConvertor {
  private static final JSONParser parser = new JSONParser();
  private static final int OTLP_JSON_INDEX_OFFSET = 74;

  /**
   * The main method converts the OTLP JSON input file to Jaeger JSON format manually. Reads the
   * input log file provided in the command line arguments and parses the file after fetching the
   * OTLP traces from it. Uses the one-to-one conversion rules mentioned <a
   * href="https://opentelemetry.io/docs/specs/otel/trace/sdk_exporters/jaeger/">here</a>. The
   * resulting Jaeger JSON files that are separated and grouped by the traceId are written to the
   * output folder. These JSON files can be uploaded to the local running instance of Jaeger UI to
   * visualize the traces.
   *
   * @param args args[0] will be the input file path, and args[1] will be the root directory path
   * @throws Exception if input file path argument is not provided, or the input file cannot be read
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw new IllegalArgumentException("Missing the input file path as an argument");
    }

    Map<String, JSONObject> dataObjects = convert(args[0], args[1]);

    // write the data objects to files named [traceId].json
    for (String fileTraceId : dataObjects.keySet()) {
      if (!args[1].equals("test")) {
        try (FileWriter outputFile =
            new FileWriter(Paths.get(args[1]) + "/" + fileTraceId + ".json")) {
          outputFile.write(dataObjects.get(fileTraceId).toJSONString());
        }
      }
    }
  }

  private static ImmutableList<Tag> convertAttributesToTags(JSONArray attributes) {
    List<Tag> tags = new ArrayList<>();
    for (Object attr : attributes) {
      JSONObject attribute = (JSONObject) attr;
      String key = attribute.get("key").toString();
      JSONObject valueObj = (JSONObject) attribute.get("value");
      String typeStr = Iterables.getOnlyElement(valueObj.keySet()).toString();
      String type = typeStr.substring(0, typeStr.length() - "Value".length());
      String value = String.valueOf(valueObj.get(typeStr));
      Tag tag = new Tag(key, type, value);
      tags.add(tag);
    }
    return ImmutableList.copyOf(tags);
  }

  private static JaegerSpan generateJaegerSpan(
      JSONObject span,
      String parentSpanId,
      List<Reference> references,
      ImmutableList<Tag> spanTags,
      String pid) {
    String spanId = span.get("spanId").toString();
    String traceId = span.get("traceId").toString();
    String operationName = span.get("name").toString();
    long startTime = Long.parseLong(span.get("startTimeUnixNano").toString());
    long duration = Long.parseLong(span.get("endTimeUnixNano").toString()) - startTime;
    // start and end times are converted from nanoseconds to microseconds
    return new JaegerSpan(
        traceId,
        spanId,
        operationName,
        parentSpanId,
        references,
        startTime / 1000,
        duration / 1000,
        spanTags,
        pid);
  }

  private static JSONObject createDataObject(
      String traceId, ArrayList<JaegerSpan> jaegerSpans, JSONObject processes)
      throws IOException, ParseException {
    ObjectMapper mapper = new ObjectMapper();
    Data jaegerOutput = new Data(traceId, jaegerSpans, processes);
    String jsonString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jaegerOutput);
    JSONObject dataWrapper = new JSONObject();
    ArrayList<JSONObject> jaegerOutputs = new ArrayList<>();
    jaegerOutputs.add((JSONObject) parser.parse(jsonString));
    dataWrapper.put("data", jaegerOutputs);

    return dataWrapper;
  }

  public static Map<String, JSONObject> convert(String inputFilePath, String outputFilePath)
      throws Exception {
    File inputFile = Paths.get(inputFilePath).toFile();
    @Var String otlpJsonStr = "";
    if (inputFile.exists()) {
      List<String> allLines = Files.readAllLines(inputFile.toPath(), Charset.defaultCharset());
      if (allLines.isEmpty()) {
        return new HashMap<>();
      } else {
        for (String line : allLines) {
          if (line.contains("[BatchSpanProcessor_WorkerThread-1]")) {
            otlpJsonStr =
                line.substring(
                    line.indexOf("[BatchSpanProcessor_WorkerThread-1]") + OTLP_JSON_INDEX_OFFSET);
            break;
          }
        }
      }
    } else {
      throw new FileNotFoundException("Input file not found: " + inputFile.getAbsolutePath());
    }
    return parseInputJsonStr(otlpJsonStr);
  }

  private static Map<String, JSONObject> parseInputJsonStr(String otlpJsonStr) throws Exception {
    JSONObject jsonObject = (JSONObject) parser.parse(otlpJsonStr);
    // "scopeSpans" consists of a scope object and a span array
    JSONObject scopeSpans = (JSONObject) ((JSONArray) jsonObject.get("scopeSpans")).get(0);
    JSONArray spans = (JSONArray) scopeSpans.get("spans");
    HashMap<String, ArrayList<JaegerSpan>> allJaegerSpans = new HashMap<>();
    JSONObject processes = new JSONObject();
    JSONObject resource = (JSONObject) jsonObject.get("resource");
    JSONArray resourceAttributes = (JSONArray) resource.get("attributes");
    Map<String, JSONObject> allDataObjects = new HashMap<>();

    // get "process.pid" and "service.name" from resource attributes
    PidAndServiceName pidAndServiceName = getPidAndServiceName(resourceAttributes);
    String pid = pidAndServiceName.getPid() == null ? "" : pidAndServiceName.getPid();
    String serviceName =
        pidAndServiceName.getServiceName() == null ? "" : pidAndServiceName.getServiceName();

    for (Object spanObject : spans) {
      JSONObject span = (JSONObject) spanObject;
      String traceId = span.get("traceId").toString();
      Object parentSpanIdObject = span.get("parentSpanId");
      String parentSpanId = parentSpanIdObject == null ? "" : parentSpanIdObject.toString();

      // generate references from the parent span ID
      ArrayList<Reference> references = new ArrayList<>();
      Reference reference = new Reference("CHILD_OF", parentSpanId);
      references.add(reference);

      // generate tags from the span attributes
      JSONArray attributes = (JSONArray) span.get("attributes");
      ImmutableList<Tag> spanTags = convertAttributesToTags(attributes);

      // generate jaeger spans from OTLP spans
      List<JaegerSpan> jaegerSpans = allJaegerSpans.get(traceId);
      JaegerSpan jaegerSpan = generateJaegerSpan(span, parentSpanId, references, spanTags, pid);
      if (jaegerSpans != null) {
        jaegerSpans.add(jaegerSpan);
      } else {
        ArrayList<JaegerSpan> newJaegerSpans = new ArrayList<>();
        newJaegerSpans.add(jaegerSpan);
        allJaegerSpans.put(traceId, newJaegerSpans);
      }
    }

    // generate processes
    ImmutableList<Tag> processTags = convertAttributesToTags(resourceAttributes);
    Process process = new Process(serviceName, processTags);
    processes.put(pid, process);

    // generate data object with spans and processes
    for (Map.Entry<String, ArrayList<JaegerSpan>> entry : allJaegerSpans.entrySet()) {
      allDataObjects.put(
          entry.getKey(), createDataObject(entry.getKey(), entry.getValue(), processes));
    }
    return allDataObjects;
  }

  private static PidAndServiceName getPidAndServiceName(JSONArray resourceAttributes) {
    @Var int matchCount = 0;
    @Var String pid = "";
    @Var String serviceName = "";

    for (Object attribute : resourceAttributes) {
      JSONObject resourceAttribute = (JSONObject) attribute;
      if (resourceAttribute.get("key").toString().equals("process.pid")) {
        pid = ((JSONObject) resourceAttribute.get("value")).get("intValue").toString();
        matchCount++;
      }
      if (resourceAttribute.get("key").toString().equals("service.name")) {
        serviceName = ((JSONObject) resourceAttribute.get("value")).get("stringValue").toString();
        matchCount++;
      }
      if (matchCount >= 2) {
        // both pid and service name are found
        break;
      }
    }
    return new PidAndServiceName(pid, serviceName);
  }

  private static class PidAndServiceName {
    private final String pid;
    private final String serviceName;

    private PidAndServiceName(String pid, String serviceName) {
      this.pid = pid;
      this.serviceName = serviceName;
    }

    public String getPid() {
      return this.pid;
    }

    public String getServiceName() {
      return this.serviceName;
    }
  }
}
