package com.xgen.mongot.util.mongodb;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.ReadConcern;
import com.xgen.mongot.util.mongodb.serialization.FindCommandProxy;
import java.util.Optional;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.conversions.Bson;

public class CollectionScanFindCommand {

  public static final BsonDocument NATURAL_ORDER = new BsonDocument("$natural", new BsonInt32(1));

  private final String collectionName;
  private final Optional<Bson> sort;
  private final Optional<Bson> hint;
  private final Optional<Bson> projection;
  private final Optional<BsonBoolean> noCursorTimeout;
  private final Optional<Bson> min;
  private final Optional<Bson> filter;
  private final Optional<BsonDocument> readConcernDocument;
  private final Optional<Integer> limit;
  private final Optional<Integer> batchSize;
  private final Optional<BsonBoolean> requestResumeToken;
  private final Optional<BsonBoolean> showRecordId;

  private CollectionScanFindCommand(
      String collectionName,
      Optional<Bson> sort,
      Optional<Bson> hint,
      Optional<Bson> projection,
      Optional<BsonBoolean> noCursorTimeout,
      Optional<Bson> min,
      Optional<Bson> filter,
      Optional<BsonDocument> readConcernDocument,
      Optional<Integer> limit,
      Optional<Integer> batchSize,
      Optional<BsonBoolean> requestResumeToken,
      Optional<BsonBoolean> showRecordId) {

    this.collectionName = collectionName;
    this.sort = sort;
    this.hint = hint;
    this.projection = projection;
    this.noCursorTimeout = noCursorTimeout;
    this.min = min;
    this.filter = filter;
    this.readConcernDocument = readConcernDocument;
    this.limit = limit;
    this.batchSize = batchSize;
    this.requestResumeToken = requestResumeToken;
    this.showRecordId = showRecordId;
  }

  public static class Builder {
    private static final String AFTER_CLUSTER_TIME_FIELD = "afterClusterTime";

    private final String collectionName;
    private Optional<Bson> sort;
    private Optional<Bson> hint;
    private Optional<Bson> projection;
    private Optional<BsonBoolean> noCursorTimeout;
    private Optional<Bson> min;
    private Optional<Bson> filter;
    private Optional<BsonDocument> readConcernDocument;
    private Optional<Integer> limit;
    private Optional<Integer> batchSize;
    private Optional<BsonBoolean> requestResumeToken;
    private Optional<BsonBoolean> showRecordId;

    /** Constructs a new Builder. */
    public Builder(String collectionName) {
      this.collectionName = collectionName;

      this.sort = Optional.empty();
      this.hint = Optional.empty();
      this.projection = Optional.empty();
      this.noCursorTimeout = Optional.empty();
      this.min = Optional.empty();
      this.filter = Optional.empty();
      this.readConcernDocument = Optional.empty();
      this.limit = Optional.empty();
      this.batchSize = Optional.empty();
      this.requestResumeToken = Optional.empty();
      this.showRecordId = Optional.empty();
    }

    /** Builds a new CollectionScanFindCommand. */
    public CollectionScanFindCommand build() {
      if (this.requestResumeToken.map(BsonBoolean::getValue).orElse(false)) {
        checkArg(
            this.hint.map(Bson::toBsonDocument).equals(Optional.of(NATURAL_ORDER)),
            "requestResumeToken requires hint {$natural: 1}");
        checkArg(
            this.sort.isEmpty() || NATURAL_ORDER.equals(this.sort.get().toBsonDocument()),
            "requestResumeToken requires sort unset or {$natural: 1}");
      }

      return new CollectionScanFindCommand(
          this.collectionName,
          this.sort,
          this.hint,
          this.projection,
          this.noCursorTimeout,
          this.min,
          this.filter,
          this.readConcernDocument,
          this.limit,
          this.batchSize,
          this.requestResumeToken,
          this.showRecordId);
    }

    /** Sets the sort. */
    public Builder sort(Bson sort) {
      this.sort = Optional.of(sort);
      return this;
    }

    /** Sets the hint. */
    public Builder hint(Bson hint) {
      this.hint = Optional.of(hint);
      return this;
    }

    /** Sets the projection. */
    public Builder projection(Bson projection) {
      this.projection = Optional.of(projection);
      return this;
    }

    /** Sets the readConcern. */
    public Builder readConcern(ReadConcern readConcern) {
      this.readConcernDocument = Optional.of(readConcern.asDocument());
      return this;
    }

    /** Sets the readConcern and afterClusterTime. */
    public Builder readConcern(ReadConcern readConcern, BsonTimestamp afterClusterTime) {
      this.readConcernDocument = Optional.of(readConcern.asDocument());
      this.readConcernDocument.get().append(AFTER_CLUSTER_TIME_FIELD, afterClusterTime);
      return this;
    }

    /** Sets the noCursorTimeout. */
    public Builder noCursorTimeout(BsonBoolean noCursorTimeout) {
      this.noCursorTimeout = Optional.of(noCursorTimeout);
      return this;
    }

    /** Sets the inclusive index lower bound. */
    public Builder min(Bson min) {
      this.min = Optional.of(min);
      return this;
    }

    /** Applies a filter. */
    public Builder filter(Bson filter) {
      this.filter = Optional.of(filter);
      return this;
    }

    /**
     * Caps the number of documents returned. The server closes the cursor once the limit is
     * reached, so a limited read leaves no cursor for the caller to clean up.
     */
    public Builder limit(int limit) {
      checkArg(limit > 0, "limit must be positive");
      this.limit = Optional.of(limit);
      return this;
    }

    /**
     * Sets how many documents the server puts in the first batch. Without it the server sends its
     * default 101 and holds a cursor open for the rest.
     */
    public Builder batchSize(int batchSize) {
      checkArg(batchSize > 0, "batchSize must be positive");
      this.batchSize = Optional.of(batchSize);
      return this;
    }

    /**
     * Requests a {@code postBatchResumeToken} on the cursor response, which carries the scan's
     * {@code $recordId} and the collection's {@code $initialSyncId}.
     *
     * <p>Requires {@code hint} to be exactly {@code {$natural: 1}}; {@link Builder#build()} rejects
     * anything else, as the server does.
     */
    public Builder requestResumeToken(BsonBoolean requestResumeToken) {
      this.requestResumeToken = Optional.of(requestResumeToken);
      return this;
    }

    /** Returns each document's {@code $recordId}, its position in the collection's storage. */
    public Builder showRecordId(BsonBoolean showRecordId) {
      this.showRecordId = Optional.of(showRecordId);
      return this;
    }
  }

  /** Constructs the proper proxy for the CollectionScanFindCommand. */
  public FindCommandProxy toProxy() {
    return new FindCommandProxy(
        new BsonString(this.collectionName),
        this.sort,
        this.hint,
        this.projection,
        this.noCursorTimeout,
        this.min,
        this.filter,
        this.readConcernDocument,
        this.limit,
        this.batchSize,
        this.requestResumeToken,
        this.showRecordId);
  }

  @VisibleForTesting
  public Optional<Bson> getMin() {
    return this.min;
  }

  @VisibleForTesting
  public Optional<BsonDocument> getReadConcernDocument() {
    return this.readConcernDocument;
  }
}
