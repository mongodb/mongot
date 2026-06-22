package com.xgen.mongot.logging;

import static com.google.common.truth.Truth.assertThat;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.ByteArrayOutputStream;
import org.junit.Test;

public class OmitEmptyFieldJsonDecoratorTest {

  // Helper function that accepts a runnable that generates JSON. Returns a string representation of
  // the JSON generated.
  private String serializeWithDecorator(JsonGeneratorRunnable jsonGenRunner) throws Exception {
    var outputStream = new ByteArrayOutputStream();
    JsonGenerator outputStreamGenerator = new JsonFactory().createGenerator(outputStream);
    JsonGenerator decoratedGenerator =
        new OmitEmptyFieldJsonDecorator().decorate(outputStreamGenerator);

    decoratedGenerator.writeStartObject();
    jsonGenRunner.run(decoratedGenerator);
    decoratedGenerator.writeEndObject();
    decoratedGenerator.close();

    return outputStream.toString();
  }

  @FunctionalInterface
  private interface JsonGeneratorRunnable {
    void run(JsonGenerator jsonGenerator) throws Exception;
  }

  @Test
  public void testOmitNullField() throws Exception {
    var jsonResult =
        serializeWithDecorator(
            gen -> {
              gen.writeFieldName("nullField");
              gen.writeNull();
            });
    assertThat(jsonResult).doesNotContain("\"nullField\"");
  }

  @Test
  public void testStringField() throws Exception {
    var emptyStringResult =
        serializeWithDecorator(
            gen -> {
              gen.writeFieldName("emptyStringField");
              gen.writeString("");
            });
    assertThat(emptyStringResult).doesNotContain("\"emptyStringField\"");

    var nonEmptyStringResult =
        serializeWithDecorator(
            gen -> {
              gen.writeFieldName("nonEmptyStringField");
              gen.writeString("test");
            });
    assertThat(nonEmptyStringResult).contains("\"nonEmptyStringField\":\"test\"");
  }

  @Test
  public void testArrayField() throws Exception {
    var emptyArrayResult =
        serializeWithDecorator(
            gen -> {
              gen.writeFieldName("emptyArrayField");
              gen.writeStartArray();
              gen.writeEndArray();
            });
    assertThat(emptyArrayResult).doesNotContain("\"emptyArrayField\"");

    var nonEmptyArrayResult =
        serializeWithDecorator(
            gen -> {
              gen.writeFieldName("nonEmptyArrayField");
              gen.writeStartArray();
              gen.writeString("test");
              gen.writeEndArray();
            });
    assertThat(nonEmptyArrayResult).contains("\"nonEmptyArrayField\":[\"test\"]");
  }

  @Test
  public void testObjectField() throws Exception {
    var emptyObjectResult =
        serializeWithDecorator(
            gen -> {
              gen.writeFieldName("emptyObjectField");
              gen.writeStartObject();
              gen.writeEndObject();
            });
    assertThat(emptyObjectResult).doesNotContain("\"emptyObjectField\"");

    var nonEmptyObjectResult =
        serializeWithDecorator(
            gen -> {
              gen.writeFieldName("nonEmptyObjectField");
              gen.writeStartObject();
              gen.writeFieldName("innerField");
              gen.writeString("test");
              gen.writeEndObject();
            });
    assertThat(nonEmptyObjectResult).contains("\"nonEmptyObjectField\":{\"innerField\":\"test\"}");
  }

  @Test
  public void testComposite() throws Exception {
    var nullArrayResult =
        serializeWithDecorator(
            gen -> {
              gen.writeFieldName("compositeArray");
              gen.writeStartArray();
              gen.writeNull();
              gen.writeEndArray();
            });
    assertThat(nullArrayResult).doesNotContain("\"compositeArray\"");

    var arrayWithNullObjectResult =
        serializeWithDecorator(
            gen -> {
              gen.writeFieldName("compositeArrayWithNullObject");
              gen.writeStartArray();
              gen.writeStartObject();
              gen.writeFieldName("innerField");
              gen.writeNull();
              gen.writeEndObject();
              gen.writeEndArray();
            });
    assertThat(arrayWithNullObjectResult).doesNotContain("\"compositeArrayWithNullObject\"");

    var arrayWithObjectResult =
        serializeWithDecorator(
            gen -> {
              gen.writeFieldName("compositeArrayWithObject");
              gen.writeStartArray();
              gen.writeStartObject();
              gen.writeFieldName("innerField");
              gen.writeString("test");
              gen.writeEndObject();
              gen.writeEndArray();
            });
    assertThat(arrayWithObjectResult)
        .contains("\"compositeArrayWithObject\":[{\"innerField\":\"test\"}]");

    var nullObjectResult =
        serializeWithDecorator(
            gen -> {
              gen.writeFieldName("compositeObject");
              gen.writeStartObject();
              gen.writeFieldName("innerField");
              gen.writeNull();
              gen.writeEndObject();
            });
    assertThat(nullObjectResult).doesNotContain("\"compositeObject\"");

    var objectWithNullArrayResult =
        serializeWithDecorator(
            gen -> {
              gen.writeFieldName("compositeObjectWithNullArray");
              gen.writeStartObject();
              gen.writeFieldName("innerField");
              gen.writeStartArray();
              gen.writeNull();
              gen.writeEndArray();
              gen.writeEndObject();
            });
    assertThat(objectWithNullArrayResult).doesNotContain("\"compositeObjectWithNullArray\"");

    var objectWithArrayResult =
        serializeWithDecorator(
            gen -> {
              gen.writeFieldName("compositeObjectWithArray");
              gen.writeStartObject();
              gen.writeFieldName("innerField");
              gen.writeStartArray();
              gen.writeString("test");
              gen.writeEndArray();
              gen.writeEndObject();
            });
    assertThat(objectWithArrayResult)
        .contains("\"compositeObjectWithArray\":{\"innerField\":[\"test\"]}");
  }
}
