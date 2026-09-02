package com.xgen.mongot.util.mongodb.serialization;

import com.xgen.mongot.util.Check;
import java.util.Optional;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

/**
 * FindCommandProxy is a proxy for an <em>find</em> command.
 *
 * <p>See https://docs.mongodb.com/manual/reference/command/find/#dbcmd.find
 */
public class FindCommandProxy implements Bson {

  private static final String FIND_FIELD = "find";
  private static final String SORT_FIELD = "sort";
  private static final String HINT_FIELD = "hint";
  private static final String PROJECTION_FIELD = "projection";
  private static final String READ_CONCERN_FIELD = "readConcern";
  private static final String NO_CURSOR_TIMEOUT_FIELD = "noCursorTimeout";
  private static final String MIN_FIELD = "min";
  private static final String FILTER_FIELD = "filter";
  private static final String LIMIT_FIELD = "limit";
  private static final String REQUEST_RESUME_TOKEN_FIELD = "$_requestResumeToken";
  private static final String SHOW_RECORD_ID_FIELD = "showRecordId";

  private final BsonString collection;
  private final Optional<Bson> sort;
  private final Optional<Bson> hint;
  private final Optional<Bson> projection;
  private final Optional<BsonDocument> readConcern;
  private final Optional<BsonBoolean> noCursorTimeout;
  private final Optional<Bson> min;
  private final Optional<Bson> filter;
  private final Optional<Integer> limit;
  private final Optional<BsonBoolean> requestResumeToken;
  private final Optional<BsonBoolean> showRecordId;

  /**
   * Constructs a proxy for a {@code find} command. Options left empty are omitted from the command
   * document entirely rather than sent as an explicit default, so a command carries only the fields
   * its caller set.
   */
  public FindCommandProxy(
      BsonString collection,
      Optional<Bson> sort,
      Optional<Bson> hint,
      Optional<Bson> projection,
      Optional<BsonBoolean> noCursorTimeout,
      Optional<Bson> min,
      Optional<Bson> filter,
      Optional<BsonDocument> readConcern,
      Optional<Integer> limit,
      Optional<BsonBoolean> requestResumeToken,
      Optional<BsonBoolean> showRecordId) {
    Check.argNotNull(collection, "collection");
    Check.argNotNull(sort, "sort");
    Check.argNotNull(hint, "hint");
    Check.argNotNull(projection, "projection");
    Check.argNotNull(noCursorTimeout, "noCursorTimeout");
    Check.argNotNull(min, "min");
    Check.argNotNull(filter, "filter");
    Check.argNotNull(readConcern, "readConcern");
    Check.argNotNull(limit, "limit");
    Check.argNotNull(requestResumeToken, "requestResumeToken");
    Check.argNotNull(showRecordId, "showRecordId");

    this.collection = collection;
    this.sort = sort;
    this.hint = hint;
    this.projection = projection;
    this.readConcern = readConcern;
    this.noCursorTimeout = noCursorTimeout;
    this.min = min;
    this.filter = filter;
    this.limit = limit;
    this.requestResumeToken = requestResumeToken;
    this.showRecordId = showRecordId;
  }

  @Override
  public <T> BsonDocument toBsonDocument(Class<T> documentClass, CodecRegistry codecRegistry) {
    Check.argNotNull(documentClass, "documentClass");
    Check.argNotNull(codecRegistry, "codecRegistry");

    BsonDocument doc = new BsonDocument().append(FIND_FIELD, this.collection);

    this.sort.ifPresent(
        bson -> doc.append(SORT_FIELD, bson.toBsonDocument(documentClass, codecRegistry)));
    this.hint.ifPresent(
        bson -> doc.append(HINT_FIELD, bson.toBsonDocument(documentClass, codecRegistry)));
    this.projection.ifPresent(
        bson -> doc.append(PROJECTION_FIELD, bson.toBsonDocument(documentClass, codecRegistry)));
    this.noCursorTimeout.ifPresent(bsonBoolean -> doc.append(NO_CURSOR_TIMEOUT_FIELD, bsonBoolean));
    this.min.ifPresent(
        bson -> doc.append(MIN_FIELD, bson.toBsonDocument(documentClass, codecRegistry)));
    this.filter.ifPresent(
        bson -> doc.append(FILTER_FIELD, bson.toBsonDocument(documentClass, codecRegistry)));
    this.limit.ifPresent(value -> doc.append(LIMIT_FIELD, new BsonInt32(value)));
    this.readConcern.ifPresent(concern -> doc.append(READ_CONCERN_FIELD, concern));
    this.requestResumeToken.ifPresent(
        bsonBoolean -> doc.append(REQUEST_RESUME_TOKEN_FIELD, bsonBoolean));
    this.showRecordId.ifPresent(bsonBoolean -> doc.append(SHOW_RECORD_ID_FIELD, bsonBoolean));

    return doc;
  }
}
