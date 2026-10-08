package com.xgen.mongot.util.bson;

import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.json.Converter;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;

/*
 * Json codec for Bson.
 *
 * Encodes: bson to json
 * Decodes: json to bson.
 */
public class JsonCodec {

  private static final JsonWriterSettings RELAXED_SETTINGS =
      JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();

  private static final Converter<BsonBinary> RELAXED_BINARY_CONVERTER =
      RELAXED_SETTINGS.getBinaryConverter();

  private static final Converter<BsonBinary> UUID_TO_STRING_CONVERTER =
      (value, writer) -> {
        if (BsonBinarySubType.isUuid(value.getType())) {
          String uuidString;
          try {
            uuidString = value.asUuid().toString();
          } catch (Exception e) {
            // fall back to relaxed binary converter
            RELAXED_BINARY_CONVERTER.convert(value, writer);
            return;
          }
          writer.writeString(uuidString);
        } else {
          RELAXED_BINARY_CONVERTER.convert(value, writer);
        }
      };

  private static final Converter<ObjectId> OBJECT_ID_TO_STRING_CONVERTER =
      (value, writer) -> writer.writeString(value.toHexString());

  /**
   * Encode BSON types that have directly corresponding JSON types into those types:
   *
   * <ul>
   *   <li>BsonString -> string
   *   <li>BsonDouble -> number
   *   <li>BsonArray -> array
   *   <li>BsonBoolean -> boolean
   *   <li>BsonNull -> null
   * </ul>
   *
   * <p>BSON types that do not have a corresponding JSON type should be encoded with Extended JSON
   * (see
   * https://github.com/mongodb/specifications/blob/master/source/extended-json.rst#conversion-table).
   *
   * <p>Relaxed Extended JSON will encode all of the above mentioned types into their corresponding
   * JSON types.
   */
  private static final JsonWriterSettings.Builder LOSSLESS_SETTINGS_BUILDER =
      JsonWriterSettings.builder().outputMode(JsonMode.RELAXED);

  private static final JsonWriterSettings LOSSLESS_SETTINGS = LOSSLESS_SETTINGS_BUILDER.build();
  private static final JsonWriterSettings LOSSLESS_PRETTY_SETTINGS =
      LOSSLESS_SETTINGS_BUILDER.indent(true).build();

  /**
   * The lossy writer, retained behind the {@code losslessJsonCodec} feature flag: it flattens
   * ObjectId and UUID to bare strings. That is the encoding every released mongot has written, and
   * the flag gates the switch between the two so that clusters with definitions already persisted
   * in the lossy form can be migrated deliberately -- the lossless writer is what makes an MMS-side
   * repair of a degraded pipeline stick; without it the next conf call re-degrades the pipeline and
   * the index rebuilds wrong again.
   */
  private static final JsonWriterSettings.Builder LOSSY_SETTINGS_BUILDER =
      JsonWriterSettings.builder()
          .outputMode(JsonMode.RELAXED)
          .binaryConverter(UUID_TO_STRING_CONVERTER)
          .objectIdConverter(OBJECT_ID_TO_STRING_CONVERTER);

  private static final JsonWriterSettings LOSSY_SETTINGS = LOSSY_SETTINGS_BUILDER.build();
  private static final JsonWriterSettings LOSSY_PRETTY_SETTINGS =
      LOSSY_SETTINGS_BUILDER.indent(true).build();

  private static volatile BooleanSupplier losslessWriterEnabled = () -> false;

  public static void setLosslessWriterSupplier(BooleanSupplier losslessWriterEnabled) {
    JsonCodec.losslessWriterEnabled = losslessWriterEnabled;
  }

  private static boolean losslessWriterEnabled() {
    return JsonCodec.losslessWriterEnabled.getAsBoolean();
  }

  /** Encodes the supplied BSON into JSON. */
  public static String toJson(BsonDocument bson) {
    return bson.toJson(losslessWriterEnabled() ? LOSSLESS_SETTINGS : LOSSY_SETTINGS);
  }

  /** Encodes the supplied DocumentEncodable into JSON. */
  public static String toJson(DocumentEncodable encodable) {
    return toJson(encodable.toBson());
  }

  /** Encodes the supplied BSON into JSON, indenting fields. */
  public static String toPrettyJson(BsonDocument bson) {
    return bson.toJson(losslessWriterEnabled() ? LOSSLESS_PRETTY_SETTINGS : LOSSY_PRETTY_SETTINGS);
  }

  /** Encodes the supplied DocumentEncodable into JSON, indenting fields. */
  public static String toPrettyJson(DocumentEncodable encodable) {
    return toPrettyJson(encodable.toBson());
  }

  /**
   * Decodes the supplied JSON into a BSON document, throwing a BsonParseException if an error is
   * encountered.
   */
  public static BsonDocument fromJson(String json) throws BsonParseException {
    try {
      return BsonDocument.parse(json);
    } catch (RuntimeException e) {
      throw new BsonParseException("Failed to parse:" + json, Optional.empty(), e);
    }
  }
}
