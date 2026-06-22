package com.xgen.mongot.replication.mongodb.synonyms;

import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.SynonymSyncException;
import com.xgen.mongot.util.concurrent.OneShotSingleThreadExecutor;
import com.xgen.mongot.util.mongodb.ChangeStreamAggregateCommand;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a {@link ChangeStreamMongoClient} where the initial {@link
 * SynonymMappingHighWaterMark} can be reset after creation to skip ahead in the change stream. <br>
 * <br>
 *
 * <p>This class creates a {@link ChangeStreamMongoClient} and keeps re-using it for subsequent
 * requests until {@link #reset(SynonymMappingHighWaterMark)} ()} is called. At that time, the
 * delegate client is closed. After a client is closed, the next call to {@link #getNext()} will
 * open a new cursor using the new {@link SynonymMappingHighWaterMark}. <br>
 * <br>
 *
 * <h3>Why can't we re-use a cursor indefinitely? </h3>
 *
 * <p>Once a change is detected, {@link SynonymMappingManager} will do a full collection scan, which
 * yields a new {@link SynonymMappingHighWaterMark}. We need to close and re-open a new client to
 * skip to the end of the change stream. <br>
 * <br>
 *
 * <h3>Why not just create a client and kill the cursor after each request?</h3>
 *
 * <p>This works correctly, however, there is a race condition on sharded clusters where mongos
 * returns an empty batch and we kill the cursor while a mongod shard is still working. This creates
 * excessive error messages in mongod logs.
 */
@ThreadSafe
public class ResettableChangeStreamClient implements ChangeStreamMongoClient<SynonymSyncException> {

  private static final Logger LOG = LoggerFactory.getLogger(ResettableChangeStreamClient.class);

  private final SynonymSyncMongoClient mongoClient;
  private final MongoNamespace mongoNamespace;
  private final SynonymMappingDefinition definition;

  @GuardedBy("this")
  private Optional<ChangeStreamMongoClient<SynonymSyncException>> client = Optional.empty();

  @GuardedBy("this")
  private SynonymMappingHighWaterMark mark = SynonymMappingHighWaterMark.createEmpty();

  public ResettableChangeStreamClient(
      MongoNamespace namespace,
      SynonymSyncMongoClient mongoClient,
      SynonymMappingDefinition definition) {
    this.mongoClient = mongoClient;
    this.definition = definition;
    this.mongoNamespace = namespace;
  }

  /**
   * Gets the next batch from the underlying client.
   *
   * <p>If the last-used client was invalidated, a new client is created with the latest watermark
   *
   * <p>Note: On sharded clusters, the first call to {@code getNext()} will only establish a cursor
   * and never return results. We don't explicitly handle that here because the client is expected
   * to be reused for many requests.
   *
   * @throws SynonymSyncException if either a client is ready and calling {@link
   *     ChangeStreamMongoClient#getNext()} threw a {@link SynonymSyncException} or if no cached
   *     client was available and a SynonymSyncException was thrown during creation.
   */
  public synchronized ChangeStreamBatch getNext() throws SynonymSyncException {
    if (this.client.isEmpty()) {
      this.client = Optional.of(createNewClient());
    }
    return this.client.get().getNext();
  }

  /**
   * Closes the underlying client if one is open.
   *
   * <p>If {@link #getNext()} is called after close, it will throw a {@link SynonymSyncException}
   */
  @Override
  public void close() {
    new OneShotSingleThreadExecutor("ResettableChangeStreamClient.close")
        .execute(() -> reset(SynonymMappingHighWaterMark.createEmpty()));
  }

  /**
   * Close the wrapped client and kill the underlying cursor. A new client would be instantiated
   * lazily on the next call to {@link #getNext()}. This method is idempotent.
   */
  synchronized void reset(SynonymMappingHighWaterMark mark) {
    if (!mark.equals(this.mark)) {
      this.mark = mark;
      try {
        this.client.ifPresent(ChangeStreamMongoClient::close);
      } catch (Exception e) {
        // Before caching, an exception on close would just fail the request, but now it would
        // crash Mongot, so let's swallow it.
        LOG.warn("Exception while closing ResettableChangeStreamClient", e);
      } finally {
        this.client = Optional.empty();
      }
    }
  }

  @GuardedBy("this")
  private ChangeStreamMongoClient<SynonymSyncException> createNewClient()
      throws SynonymSyncException {
    if (!this.mark.isPresent()) {
      throw SynonymSyncException.createTransient("Cannot create client without watermark");
    }
    ChangeStreamAggregateCommand aggregateCommand =
        SynonymSyncMongoClient.changeStreamAggregateCommand(this.definition, this.mark);
    return this.mongoClient.getChangeStreamClient(aggregateCommand, this.mongoNamespace);
  }
}
