package com.xgen.mongot.util.bson;

import com.google.protobuf.ByteString;
import com.xgen.mongot.proto.bson.ProtoArray;
import com.xgen.mongot.proto.bson.ProtoBinaryData;
import com.xgen.mongot.proto.bson.ProtoDbPointer;
import com.xgen.mongot.proto.bson.ProtoDocument;
import com.xgen.mongot.proto.bson.ProtoJavaScriptWithScope;
import com.xgen.mongot.proto.bson.ProtoObjectId;
import com.xgen.mongot.proto.bson.ProtoRegularExpression;
import com.xgen.mongot.proto.bson.ProtoValue;
import com.xgen.mongot.proto.bson.ProtoValueless;
import com.xgen.mongot.util.BsonUtils;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

/**
 * This class defines an injective mapping between {@link ProtoValue} and {@link BsonValue} with
 * checked conversion between the two types.
 */
public class ProtoConverter {

  static final int DECIMAL128_BYTE_LENGTH = 16;
  static final int OBJECT_ID_BYTE_LENGTH = 12;

  private ProtoConverter() {
    // No need to instantiate static utility class
  }

  /**
   * Converts any {@link BsonValue} value into a protobuf representation.
   *
   * <p>Since BsonValues cannot be serialized on their own, they would otherwise have to be wrapped
   * in a {@link BsonDocument}, which adds a minimum of 7 bytes of overhead. Common Bson scalars,
   * however, can be represented efficiently in protobuf with only 1 byte of overhead.
   */
  public static ProtoValue convert(BsonValue value) {
    return switch (value.getBsonType()) {
      case INT32 -> ProtoValue.newBuilder().setInt32(value.asInt32().getValue()).build();
      case INT64 -> ProtoValue.newBuilder().setInt64(value.asInt64().getValue()).build();
      case DOUBLE -> ProtoValue.newBuilder().setDouble(value.asDouble().getValue()).build();
      case OBJECT_ID ->
          ProtoValue.newBuilder()
              .setObjectId(ByteString.copyFrom(value.asObjectId().getValue().toByteArray()))
              .build();
      case STRING -> ProtoValue.newBuilder().setString(value.asString().getValue()).build();
      case BOOLEAN -> ProtoValue.newBuilder().setBool(value.asBoolean().getValue()).build();
      case NULL -> ProtoValue.newBuilder().setValueless(ProtoValueless.NULL).build();
      case MIN_KEY -> ProtoValue.newBuilder().setValueless(ProtoValueless.MIN_KEY).build();
      case MAX_KEY -> ProtoValue.newBuilder().setValueless(ProtoValueless.MAX_KEY).build();
      case UNDEFINED -> ProtoValue.newBuilder().setValueless(ProtoValueless.UNDEFINED).build();
      case DATE_TIME ->
          ProtoValue.newBuilder().setUtcDatetime(value.asDateTime().getValue()).build();
      case DOCUMENT -> ProtoValue.newBuilder().setDocument(convert(value.asDocument())).build();
      case ARRAY -> {
        ProtoArray.Builder array = ProtoArray.newBuilder();
        for (BsonValue e : value.asArray()) {
          array.addValue(convert(e));
        }
        yield ProtoValue.newBuilder().setArray(array).build();
      }
      case BINARY -> {
        BsonBinary binary = value.asBinary();
        yield ProtoValue.newBuilder()
            .setBinary(
                ProtoBinaryData.newBuilder()
                    .setSubTypeValue(binary.getType())
                    .setData(ByteString.copyFrom(binary.getData())))
            .build();
      }
      case REGULAR_EXPRESSION -> {
        BsonRegularExpression regex = value.asRegularExpression();
        yield ProtoValue.newBuilder()
            .setRegularExpression(
                ProtoRegularExpression.newBuilder()
                    .setPattern(regex.getPattern())
                    .setOptions(regex.getOptions()))
            .build();
      }
      case JAVASCRIPT ->
          ProtoValue.newBuilder().setJavaScript(value.asJavaScript().getCode()).build();
      case TIMESTAMP ->
          ProtoValue.newBuilder().setTimestamp(value.asTimestamp().getValue()).build();
      case DECIMAL128 -> {
        Decimal128 quad = value.asDecimal128().getValue();
        ByteBuffer buffer = ByteBuffer.wrap(new byte[16]).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(quad.getLow()).putLong(quad.getHigh()).rewind();
        yield ProtoValue.newBuilder().setDecimal128(ByteString.copyFrom(buffer)).build();
      }
      case DB_POINTER -> {
        BsonDbPointer dbPointer = value.asDBPointer();
        yield ProtoValue.newBuilder()
            .setDbPointer(
                ProtoDbPointer.newBuilder()
                    .setNamespace(dbPointer.getNamespace())
                    .setObjectId(
                        ProtoObjectId.newBuilder()
                            .setRep(ByteString.copyFrom(dbPointer.getId().toByteArray()))))
            .build();
      }
      case SYMBOL -> ProtoValue.newBuilder().setSymbol(value.asSymbol().getSymbol()).build();
      case JAVASCRIPT_WITH_SCOPE -> {
        BsonJavaScriptWithScope jsScope = value.asJavaScriptWithScope();
        yield ProtoValue.newBuilder()
            .setJavaScriptWithScope(
                ProtoJavaScriptWithScope.newBuilder()
                    .setCode(jsScope.getCode())
                    .setScope(convert(jsScope.getScope())))
            .build();
      }
      case END_OF_DOCUMENT ->
          throw new IllegalArgumentException("Unsupported BsonType: " + value.getBsonType());
    };
  }

  private static ProtoDocument convert(BsonDocument doc) {
    ProtoDocument.Builder result = ProtoDocument.newBuilder();
    for (var entry : doc.entrySet()) {
      result.addField(
          ProtoDocument.Field.newBuilder()
              .setName(entry.getKey())
              .setValue(convert(entry.getValue())));
    }
    return result.build();
  }

  /**
   * Recovers a {@link BsonValue} encoded by {@link #convert(BsonValue)}.
   *
   * @throws TypeConversionException if {@code v} has no recognized fields set.
   */
  public static BsonValue convert(ProtoValue v) throws TypeConversionException {
    try {
      return switch (v.getTypeCase()) {
        case INT32 -> new BsonInt32(v.getInt32());
        case INT64 -> new BsonInt64(v.getInt64());
        case DOUBLE -> new BsonDouble(v.getDouble());
        case UTC_DATETIME -> new BsonDateTime(v.getUtcDatetime());
        case TIMESTAMP -> new BsonTimestamp(v.getTimestamp());
        case STRING -> new BsonString(v.getString());
        case OBJECT_ID -> new BsonObjectId(convertObjectId(v.getObjectId()));
        case BOOL -> new BsonBoolean(v.getBool());
        case VALUELESS ->
            switch (v.getValueless()) {
              case NULL -> BsonNull.VALUE;
              case MIN_KEY -> BsonUtils.MIN_KEY;
              case MAX_KEY -> BsonUtils.MAX_KEY;
              case UNDEFINED -> new BsonUndefined();
              case UNRECOGNIZED ->
                  throw new TypeConversionException("Unrecognized Valueless entry");
            };
        case BINARY -> {
          if (v.getBinary().getSubTypeValue() > Byte.MAX_VALUE) {
            throw new TypeConversionException(
                "Illegal BinarySubType value: " + v.getBinary().getSubTypeValue());
          }
          yield new BsonBinary(
              (byte) v.getBinary().getSubTypeValue(), v.getBinary().getData().toByteArray());
        }
        case DOCUMENT -> convert(v.getDocument());
        case ARRAY -> {
          BsonArray result = new BsonArray(v.getArray().getValueCount());
          for (ProtoValue element : v.getArray().getValueList()) {
            result.add(convert(element));
          }
          yield result;
        }
        case REGULAR_EXPRESSION ->
            new BsonRegularExpression(
                v.getRegularExpression().getPattern(), v.getRegularExpression().getOptions());
        case JAVA_SCRIPT -> new BsonJavaScript(v.getJavaScript());
        case DECIMAL128 -> {
          ByteBuffer bytes =
              v.getDecimal128().asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
          if (bytes.remaining() != DECIMAL128_BYTE_LENGTH) {
            throw new TypeConversionException("Incorrect Decimal128 length:" + bytes.remaining());
          }
          long low = bytes.getLong();
          long high = bytes.getLong();
          Decimal128 value = Decimal128.fromIEEE754BIDEncoding(high, low);
          yield new BsonDecimal128(value);
        }
        case DB_POINTER ->
            new BsonDbPointer(
                v.getDbPointer().getNamespace(),
                convertObjectId(v.getDbPointer().getObjectId().getRep()));
        case SYMBOL -> new BsonSymbol(v.getSymbol());
        case JAVA_SCRIPT_WITH_SCOPE ->
            new BsonJavaScriptWithScope(
                v.getJavaScriptWithScope().getCode(),
                convert(v.getJavaScriptWithScope().getScope()));
        case TYPE_NOT_SET -> throw new TypeConversionException("ProtoValue had no type set");
      };
    } catch (RuntimeException e) {
      throw new TypeConversionException("Exception in conversion to BsonValue", e);
    }
  }

  private static BsonDocument convert(ProtoDocument doc) throws TypeConversionException {
    BsonDocument result = new BsonDocument();
    for (var k : doc.getFieldList()) {
      result.put(k.getName(), convert(k.getValue()));
    }
    return result;
  }

  private static ObjectId convertObjectId(ByteString rep) throws TypeConversionException {
    ByteBuffer buffer = rep.asReadOnlyByteBuffer();
    if (buffer.remaining() != OBJECT_ID_BYTE_LENGTH) {
      throw new TypeConversionException("Incorrect ObjectID length:" + buffer.remaining());
    }
    return new ObjectId(buffer);
  }
}
