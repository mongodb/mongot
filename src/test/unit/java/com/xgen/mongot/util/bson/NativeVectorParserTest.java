package com.xgen.mongot.util.bson;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.types.Decimal128;
import org.junit.Test;

public class NativeVectorParserTest {

  @Test
  public void parse_materializedArray_mixedNumericValues_returnsFloats() throws BsonParseException {
    var array = new BsonArray(List.of(new BsonInt32(10), new BsonInt64(20), new BsonDouble(3.03)));

    var result = NativeVectorParser.parse(array);

    assertArrayEquals(new float[] {10f, 20f, 3.03f}, result, 0f);
  }

  @Test
  public void parse_materializedArray_precisionAndRangeValues_narrowsToFloat()
      throws BsonParseException {
    var array =
        new BsonArray(
            List.of(
                new BsonDouble(123.172123),
                new BsonDouble(Double.MAX_VALUE),
                new BsonDouble(Double.MIN_VALUE)));

    var result = NativeVectorParser.parse(array);

    assertArrayEquals(new float[] {123.17213f, Float.POSITIVE_INFINITY, 0f}, result, 0f);
  }

  @Test
  public void parse_materializedArray_empty_returnsEmptyArray() throws BsonParseException {
    var result = NativeVectorParser.parse(new BsonArray());

    assertThat(result).isEmpty();
  }

  @Test
  public void parse_materializedArray_invalidValue_throwsBsonParseException() {
    var array = new BsonArray(List.of(new BsonString("not a number")));

    assertThrows(BsonParseException.class, () -> NativeVectorParser.parse(array));
  }

  @Test
  public void parse_materializedArray_decimal128_throwsBsonParseException() {
    var array = new BsonArray(List.of(new BsonDecimal128(new Decimal128(1))));

    assertThrows(BsonParseException.class, () -> NativeVectorParser.parse(array));
  }

  @Test
  public void parse_reader_mixedNumericValues_returnsFloatsWithoutMaterializing() {
    var document =
        new BsonDocument(
            "field",
            new BsonArray(List.of(new BsonInt32(10), new BsonInt64(20), new BsonDouble(3.03))));
    try (var reader = document.asBsonReader()) {
      reader.readStartDocument();
      reader.readName();

      var result = NativeVectorParser.parse(reader);

      assertThat(result).isPresent();
      assertArrayEquals(new float[] {10f, 20f, 3.03f}, result.orElseThrow(), 0f);
    }
  }

  @Test
  public void parse_reader_decimal128_returnsEmpty() {
    var document =
        new BsonDocument("field", new BsonArray(List.of(new BsonDecimal128(new Decimal128(1)))));
    try (var reader = document.asBsonReader()) {
      reader.readStartDocument();
      reader.readName();

      assertThat(NativeVectorParser.parse(reader)).isEmpty();
    }
  }
}
