package com.xgen.mongot.util;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;

import com.xgen.mongot.util.bson.DocumentUtil;
import java.nio.file.Paths;
import java.time.Instant;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;

public class BsonUtilsTest {

  @Test
  public void testIsOversized() throws Exception {
    BsonDocument smallBson =
        DocumentUtil.documentFromPath(
            Paths.get("src/test/unit/resources/util/documentUtilBasicData.json"));

    // This loop creates a ~17 MiB bson document.
    BsonDocument largeBson = new BsonDocument();
    for (int i = 0; i < 1_500_000; i++) {
      largeBson.append(String.valueOf(i), new BsonInt32(i));
    }

    Assert.assertFalse(BsonUtils.isOversized(smallBson));
    Assert.assertTrue(BsonUtils.isOversized(largeBson));
  }

  @Test
  public void emptyIsMinimal() {
    RawBsonDocument empty = BsonUtils.emptyDocument();
    RawBsonDocument canonical = RawBsonDocument.parse("{}");

    assertEquals(canonical, empty);
    assertEquals(canonical.getByteBuffer().remaining(), empty.getByteBuffer().array().length);
  }

  @Test
  public void emptyReturnsNewInstance() {
    RawBsonDocument modified = BsonUtils.emptyDocument();
    modified.getByteBuffer().put((byte) 42);
    RawBsonDocument empty = BsonUtils.emptyDocument();

    assertEquals(42, modified.getByteBuffer().get());
    assertEquals(5, empty.getByteBuffer().get());
  }

  @Test
  public void extractInstant() {
    int secondsSinceEpoch = 1787075757;
    int increment = 1;
    BsonTimestamp timestamp = new BsonTimestamp(secondsSinceEpoch, increment);

    assertThat(BsonUtils.extractInstant(timestamp))
        .isEqualTo(Instant.ofEpochSecond(secondsSinceEpoch));
  }

  @Test
  public void extractInstant_maxSecondsSinceEpoch() {
    long secondsSinceEpoch = (1L << 32) - 1;
    int increment = 1;
    BsonTimestamp timestamp = new BsonTimestamp((secondsSinceEpoch << 32) | increment);

    assertThat(BsonUtils.extractInstant(timestamp))
        .isEqualTo(Instant.ofEpochSecond(secondsSinceEpoch));
  }

  @Test
  public void toInstant() {
    long millisSinceEpoch = 1787075757123L;
    BsonDateTime dateTime = new BsonDateTime(millisSinceEpoch);

    assertThat(BsonUtils.toInstant(dateTime)).isEqualTo(Instant.ofEpochMilli(millisSinceEpoch));
  }

  @Test
  public void truncateToDateTime_milliseconds() {
    long millisSinceEpoch = 1787075757123L;
    Instant instant = Instant.ofEpochMilli(millisSinceEpoch);

    assertThat(BsonUtils.truncateToDateTime(instant)).isEqualTo(new BsonDateTime(millisSinceEpoch));
  }

  @Test
  public void truncateToDateTime_nanoseconds() {
    long millisSinceEpoch = 1787075757123L;
    long secondsSinceEpoch = millisSinceEpoch / 1_000;
    long millisAdjustment = millisSinceEpoch % 1_000;
    int nanosAdjustment = 1;
    Instant instant =
        Instant.ofEpochSecond(secondsSinceEpoch, nanosAdjustment + (millisAdjustment * 1_000_000));

    assertThat(BsonUtils.truncateToDateTime(instant)).isEqualTo(new BsonDateTime(millisSinceEpoch));
  }
}
