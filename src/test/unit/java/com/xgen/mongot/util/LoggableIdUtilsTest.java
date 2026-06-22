package com.xgen.mongot.util;

import static com.google.common.truth.Truth.assertThat;

import java.util.Optional;
import java.util.UUID;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Test;

/** Tests for {@link LoggableIdUtils}. */
public class LoggableIdUtilsTest {

  @After
  public void resetFeatureFlag() {
    // Reset to default disabled state after each test
    LoggableIdUtils.initialize(false);
  }

  // ==================== Feature flag disabled tests ====================

  @Test
  public void getLoggableId_whenDisabled_withObjectId_returnsUnknown() {
    LoggableIdUtils.initialize(false);

    ObjectId objectId = new ObjectId();
    BsonObjectId bsonObjectId = new BsonObjectId(objectId);

    String result = LoggableIdUtils.getLoggableId(bsonObjectId);

    assertThat(result).isEqualTo(LoggableIdUtils.UNKNOWN_LOGGABLE_ID);
  }

  @Test
  public void getLoggableId_whenDisabled_withStandardUuid_returnsUnknown() {
    LoggableIdUtils.initialize(false);

    UUID uuid = UUID.randomUUID();
    BsonBinary bsonUuid = new BsonBinary(uuid);

    String result = LoggableIdUtils.getLoggableId(bsonUuid);

    assertThat(result).isEqualTo(LoggableIdUtils.UNKNOWN_LOGGABLE_ID);
  }

  @Test
  public void getLoggableId_whenDisabled_withBsonString_returnsUnknown() {
    LoggableIdUtils.initialize(false);

    BsonString bsonString = new BsonString("test-id-123");

    String result = LoggableIdUtils.getLoggableId(bsonString);

    assertThat(result).isEqualTo(LoggableIdUtils.UNKNOWN_LOGGABLE_ID);
  }

  // ==================== Feature flag enabled tests ====================

  @Test
  public void getLoggableId_whenEnabled_withObjectId_returnsHexString() {
    LoggableIdUtils.initialize(true);

    ObjectId objectId = new ObjectId();
    BsonObjectId bsonObjectId = new BsonObjectId(objectId);

    String result = LoggableIdUtils.getLoggableId(bsonObjectId);

    assertThat(result).isEqualTo(objectId.toHexString());
  }

  @Test
  public void getLoggableId_whenEnabled_withStandardUuid_returnsUuidString() {
    LoggableIdUtils.initialize(true);

    UUID uuid = UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa");
    BsonBinary bsonUuid = new BsonBinary(uuid);

    String result = LoggableIdUtils.getLoggableId(bsonUuid);

    assertThat(result).isEqualTo(uuid.toString());
  }

  @Test
  public void getLoggableId_whenEnabled_withLegacyUuid_returnsUnloggable() {
    LoggableIdUtils.initialize(true);

    byte[] uuidBytes =
        new byte[] {
          (byte) 0xeb, (byte) 0x6c, (byte) 0x40, (byte) 0xca,
          (byte) 0xf2, (byte) 0x5e, (byte) 0x47, (byte) 0xe8,
          (byte) 0xb4, (byte) 0x8c, (byte) 0x02, (byte) 0xa0,
          (byte) 0x5b, (byte) 0x64, (byte) 0xa5, (byte) 0xaa
        };
    BsonBinary legacyUuid = new BsonBinary(BsonBinarySubType.UUID_LEGACY, uuidBytes);

    String result = LoggableIdUtils.getLoggableId(legacyUuid);

    assertThat(result).isEqualTo(LoggableIdUtils.UNLOGGABLE_ID_TYPE);
  }

  @Test
  public void getLoggableId_whenEnabled_withBsonString_returnsUnloggable() {
    LoggableIdUtils.initialize(true);

    BsonString bsonString = new BsonString("test-id-123");

    String result = LoggableIdUtils.getLoggableId(bsonString);

    assertThat(result).isEqualTo(LoggableIdUtils.UNLOGGABLE_ID_TYPE);
  }

  @Test
  public void getLoggableId_whenEnabled_withBsonInt32_returnsUnloggable() {
    LoggableIdUtils.initialize(true);

    BsonInt32 bsonInt = new BsonInt32(42);

    String result = LoggableIdUtils.getLoggableId(bsonInt);

    assertThat(result).isEqualTo(LoggableIdUtils.UNLOGGABLE_ID_TYPE);
  }

  @Test
  public void getLoggableId_whenEnabled_withNullBsonValue_returnsUnknown() {
    LoggableIdUtils.initialize(true);

    String result = LoggableIdUtils.getLoggableId((org.bson.BsonValue) null);

    assertThat(result).isEqualTo(LoggableIdUtils.UNKNOWN_LOGGABLE_ID);
  }

  // ==================== isEnabled tests ====================

  @Test
  public void isEnabled_whenInitializedTrue_returnsTrue() {
    LoggableIdUtils.initialize(true);

    assertThat(LoggableIdUtils.isEnabled()).isTrue();
  }

  @Test
  public void isEnabled_whenInitializedFalse_returnsFalse() {
    LoggableIdUtils.initialize(false);

    assertThat(LoggableIdUtils.isEnabled()).isFalse();
  }

  // ==================== binaryToUuidString tests ====================

  @Test
  public void binaryToUuidString_withStandardUuid_returnsUuidString() {
    UUID uuid = UUID.fromString("eb6c40ca-f25e-47e8-b48c-02a05b64a5aa");
    BsonBinary bsonUuid = new BsonBinary(uuid);

    Optional<String> result = LoggableIdUtils.binaryToUuidString(bsonUuid);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(uuid.toString());
  }

  @Test
  public void binaryToUuidString_withRandomStandardUuid_returnsCorrectUuidString() {
    UUID uuid = UUID.randomUUID();
    BsonBinary bsonUuid = new BsonBinary(uuid);

    Optional<String> result = LoggableIdUtils.binaryToUuidString(bsonUuid);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(uuid.toString());
  }

  @Test
  public void binaryToUuidString_withLegacyUuid_returnsEmpty() {
    byte[] uuidBytes =
        new byte[] {
          (byte) 0xeb, (byte) 0x6c, (byte) 0x40, (byte) 0xca,
          (byte) 0xf2, (byte) 0x5e, (byte) 0x47, (byte) 0xe8,
          (byte) 0xb4, (byte) 0x8c, (byte) 0x02, (byte) 0xa0,
          (byte) 0x5b, (byte) 0x64, (byte) 0xa5, (byte) 0xaa
        };
    BsonBinary legacyUuid = new BsonBinary(BsonBinarySubType.UUID_LEGACY, uuidBytes);

    Optional<String> result = LoggableIdUtils.binaryToUuidString(legacyUuid);

    assertThat(result).isEmpty();
  }

  @Test
  public void binaryToUuidString_withNullBinary_returnsEmpty() {
    Optional<String> result = LoggableIdUtils.binaryToUuidString(null);

    assertThat(result).isEmpty();
  }

  @Test
  public void binaryToUuidString_withGenericBinary_returnsEmpty() {
    byte[] data = new byte[] {0x01, 0x02, 0x03, 0x04};
    BsonBinary genericBinary = new BsonBinary(BsonBinarySubType.BINARY, data);

    Optional<String> result = LoggableIdUtils.binaryToUuidString(genericBinary);

    assertThat(result).isEmpty();
  }

  @Test
  public void binaryToUuidString_withWrongLengthData_returnsEmpty() {
    // Create a binary with UUID_STANDARD subtype but wrong data length
    byte[] wrongLengthData = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05};
    BsonBinary invalidBinary = new BsonBinary(BsonBinarySubType.UUID_STANDARD, wrongLengthData);

    Optional<String> result = LoggableIdUtils.binaryToUuidString(invalidBinary);

    assertThat(result).isEmpty();
  }

  @Test
  public void binaryToUuidString_preservesByteOrder() {
    // Verify the byte order is correctly interpreted as big-endian (standard UUID format)
    // UUID: 01020304-0506-0708-090a-0b0c0d0e0f10
    byte[] uuidBytes =
        new byte[] {
          0x01,
          0x02,
          0x03,
          0x04, // time_low
          0x05,
          0x06, // time_mid
          0x07,
          0x08, // time_hi_and_version
          0x09,
          0x0a, // clock_seq
          0x0b,
          0x0c,
          0x0d,
          0x0e,
          0x0f,
          0x10 // node
        };
    BsonBinary bsonUuid = new BsonBinary(BsonBinarySubType.UUID_STANDARD, uuidBytes);

    Optional<String> result = LoggableIdUtils.binaryToUuidString(bsonUuid);

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo("01020304-0506-0708-090a-0b0c0d0e0f10");
  }
}
