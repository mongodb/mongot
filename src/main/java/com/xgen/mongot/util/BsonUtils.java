package com.xgen.mongot.util;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonBinary;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonArrayCodec;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.BsonValueCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.types.ObjectId;

/**
 * This class contains constants and utility functions that do not depend on any Mongot code and
 * thus can be referenced from any package.
 *
 * <p>For Mongot-specific functions, see also {@link com.xgen.mongot.util.bson.DocumentUtil}.
 */
public class BsonUtils {

  /**
   * A thread-safe, lazily-initialized BsonDocumentCodec. A codec computes and caches a mapping of
   * {@code Function<Class<T>, Encoder<T>>} and therefore should be re-used to prevent redundant
   * computation.
   */
  public static final BsonDocumentCodec BSON_DOCUMENT_CODEC = new BsonDocumentCodec();

  public static final BsonValueCodec BSON_VALUE_CODEC = new BsonValueCodec();

  public static final BsonArrayCodec BSON_ARRAY_CODEC = new BsonArrayCodec();

  public static final Bytes MAX_BSON_SIZE = Bytes.ofMebi(16);

  public static final Function<BytesRef, BsonValue> STRING_CONVERTER =
      v -> new BsonString(v.utf8ToString());

  public static final Function<BytesRef, BsonValue> UUID_CONVERTER =
      v -> new BsonBinary(UUID.fromString(v.utf8ToString()));

  public static final Function<BytesRef, BsonValue> OBJECT_ID_CONVERTER =
      v -> new BsonObjectId(new ObjectId(ByteBuffer.wrap(v.bytes, v.offset, v.length)));

  public static final BsonMinKey MIN_KEY = new BsonMinKey();
  public static final BsonMaxKey MAX_KEY = new BsonMaxKey();

  private static final byte[] EMPTY_DOCUMENT_BYTES = new byte[] {5, 0, 0, 0, 0};

  /**
   * The default context to use when encoding a BsonDocument that optimizes for speed. The tradeoff
   * is that the _id field will not be automatically reordered as the first field in the document.
   * <br>
   * When to use:
   *
   * <ol>
   *   <li>If you know that _id appears first in the BsonDocument
   *   <li>If the document will not be inserted into a MongoDB collection or used for idLookup
   * </ol>
   *
   * <p>Otherwise, see {@code EncoderContext.builder().isEncodingCollectibleDocument(true).build()}
   */
  public static final EncoderContext DEFAULT_FAST_CONTEXT = EncoderContext.builder().build();

  /**
   * Converts the supplied BsonDocument into a RawBsonDocument.
   *
   * <p>Note that this requires fully serializing the supplied BsonDocument, and should be avoided
   * if possible.
   */
  public static RawBsonDocument documentToRaw(BsonDocument document) {
    return new RawBsonDocument(document, BsonUtils.BSON_DOCUMENT_CODEC);
  }

  private static Bytes bsonDocumentSerializedBytes(BsonDocument bsonDocument) {
    return Bytes.ofBytes(documentToRaw(bsonDocument).getByteBuffer().remaining());
  }

  public static boolean isOversized(BsonValue value) {
    return bsonValueSerializedBytes(value).compareTo(BsonUtils.MAX_BSON_SIZE) > 0;
  }

  /**
   * We can serialize a BsonDocument but not a BsonValue, so we calculate by constructing a dummy
   * BsonDocument from the BsonValue and subtract the surrounding bytes. {"a": "world"} →
   * \x16\x00\x00\x00 // total document size, 4 bytes \x02 // 0x02 = type String, 1 byte a\x00 //
   * field name, 2 bytes \x06\x00\x00\x00world\x00 // field value. This is used for the byte size of
   * the BsonValue. \x00 // 0x00 = type EOO ('endof object'), 1 byte
   */
  public static Bytes bsonValueSerializedBytes(BsonValue bsonValue) {
    if (bsonValue.isDocument()) {
      return bsonDocumentSerializedBytes(bsonValue.asDocument());
    }
    return bsonDocumentSerializedBytes(new BsonDocument("a", bsonValue)).subtract(Bytes.ofBytes(8));
  }

  /**
   * An empty RawBsonDocument.
   *
   * <p>This is equivalent to {@code new RawBsonDocument(new BsonDocument(), new
   * BsonDocumentCodec()))} but doesn't need to allocate a buffer or intermediate objects. The
   * returned object is also guaranteed to be backed by a minimally-sized array.
   */
  public static RawBsonDocument emptyDocument() {
    byte[] copy = EMPTY_DOCUMENT_BYTES.clone();
    return new RawBsonDocument(copy, 0, copy.length);
  }

  public static Document asDocument(BsonDocument bsonDocument) {
    DocumentCodec codec = new DocumentCodec();
    DecoderContext decoderContext = DecoderContext.builder().build();
    return codec.decode(new BsonDocumentReader(bsonDocument), decoderContext);
  }

  /**
   * Extracts the time component from a BSON timestamp.
   *
   * <p>{@link Instant} precision is higher (nanoseconds) than {@link BsonTimestamp} time precision
   * (seconds), so this conversion is lossless.
   */
  public static Instant extractInstant(BsonTimestamp timestamp) {
    return Instant.ofEpochSecond(Integer.toUnsignedLong(timestamp.getTime()));
  }

  /**
   * Converts a {@link BsonDateTime} to an {@link Instant}.
   *
   * <p>{@link Instant} precision is higher (nanoseconds) than {@link BsonDateTime} time precision
   * (milliseconds), so this conversion is lossless.
   */
  public static Instant toInstant(BsonDateTime dateTime) {
    return Instant.ofEpochMilli(dateTime.getValue());
  }

  /**
   * Converts an {@link Instant} to a {@link BsonDateTime} by truncating from nanosecond to
   * millisecond precision.
   */
  public static BsonDateTime truncateToDateTime(Instant instant) {
    return new BsonDateTime(instant.toEpochMilli());
  }
}
