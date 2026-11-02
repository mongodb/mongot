package com.xgen.mongot.replication.mongodb.synonyms;

import com.google.errorprone.annotations.Var;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.version.SynonymMappingId;
import com.xgen.mongot.replication.mongodb.common.SynonymSyncException;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Sample;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.bson.BsonTimestamp;

/**
 * A single collection scan request. {@link SynonymMappingManager}s create {@link
 * SynonymCollectionScanRequest}s and enqueue them for work in {@link SynonymManager}.
 *
 * <p>A successful scan returns a {@link SynonymMappingHighWaterMark} with the operation time of the
 * collection scan; this is the point at which the generated synonym mapping was up to date. {@link
 * SynonymChangeStreamRequest} tasks can advance this {@link SynonymMappingHighWaterMark}, and
 * eventually inform a {@link SynonymMappingManager} when a change has occurred and a {@link
 * SynonymCollectionScanRequest} needs to be run again.
 *
 * <p>{@link SynonymCollectionScanRequest} is not thread-safe, and is expected to be externally
 * synchronized.
 */
class SynonymCollectionScanRequest extends SynonymSyncRequest {

  private final SynonymDocumentIndexer documentIndexer;

  private final Runnable onBegin;

  private final SynonymCollectionScanner.Factory collectionScannerFactory;

  private SynonymCollectionScanRequest(
      SynonymDocumentIndexer documentIndexer,
      SynonymMappingId mappingId,
      SynonymMappingDefinition synonymMappingDefinition,
      MongoNamespace namespace,
      Runnable onBegin,
      SynonymCollectionScanner.Factory collectionScannerFactory,
      CompletableFuture<SynonymMappingHighWaterMark> future,
      SynonymSyncMetrics metrics) {
    super(mappingId, synonymMappingDefinition, namespace, future, metrics);
    this.documentIndexer = documentIndexer;
    this.onBegin = onBegin;
    this.collectionScannerFactory = collectionScannerFactory;
  }

  static SynonymCollectionScanRequest create(
      SynonymDocumentIndexer documentIndexer,
      IndexGeneration indexGeneration,
      SynonymMappingDefinition synonymDefinition,
      Runnable synonymSyncStartingHandler,
      SynonymCollectionScanner.Factory collectionScannerFactory,
      SynonymSyncMetrics metrics) {
    return new SynonymCollectionScanRequest(
        documentIndexer,
        SynonymMappingId.from(indexGeneration.getGenerationId(), synonymDefinition.name()),
        synonymDefinition,
        SynonymSourceNamespace.of(indexGeneration, synonymDefinition),
        synonymSyncStartingHandler,
        collectionScannerFactory,
        new CompletableFuture<>(),
        metrics);
  }

  @Override
  SynonymMappingHighWaterMark doWork(SynonymSyncMongoClient client) throws SynonymSyncException {
    // Collection scans may fail silently during the cluster sharding process. The scan may
    // erroneously report that the collection has no documents. We perform a health check against
    // the collection to confirm that the mongod/mongos is fully operational before performing a
    // collection scan.
    try {
      client.mongosHealthCheck();
    } catch (SynonymSyncException e) {
      this.documentIndexer.completeExceptionally(e);
      throw e;
    }

    SynonymCollectionScanner scanner = this.collectionScannerFactory.create(client, this);

    this.onBegin.run();
    @Var Optional<BsonTimestamp> opTime = Optional.empty();
    Sample sample = Timer.start();
    try {
      opTime = Optional.of(scanner.scan());
      this.documentIndexer.complete();
      return SynonymMappingHighWaterMark.create(opTime.get());
    } catch (SynonymSyncException e) {
      this.documentIndexer.completeExceptionally(e);

      // For some exceptions, like an invalid document, the operation time of this scan is useful.
      // If we know the operationTime at which this synonym mapping was invalid, we can watch for
      // changes after that time and only re-attempt collection scans after changes are noticed.
      throw SynonymSyncException.withOpTime(e, opTime.or(scanner::getOperationTime));
    } finally {
      sample.stop(this.metrics.getCollScanDurationTimer());
    }
  }

  SynonymDocumentIndexer getDocumentIndexer() {
    return this.documentIndexer;
  }
}
