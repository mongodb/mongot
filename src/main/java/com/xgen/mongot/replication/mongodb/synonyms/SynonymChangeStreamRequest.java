package com.xgen.mongot.replication.mongodb.synonyms;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.SynonymMappingId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.SynonymSyncException;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SynonymChangeStreamRequest} is a unit of work that {@link SynonymMappingManager}s may
 * schedule on {@link SynonymManager}. {@link SynonymChangeStreamRequest}s open a change stream to
 * listen for changes to synonym documents in a synonym source collection.
 *
 * <p>When no changes are detected, {@link SynonymChangeStreamRequest} returns a {@link
 * SynonymMappingHighWaterMark} with the post batch resume token from its scan. This token may be
 * used to start the next {@link SynonymChangeStreamRequest}, which in turn may advance the {@link
 * SynonymMappingHighWaterMark} again.
 *
 * <p>{@link SynonymChangeStreamRequest} signals it has detected a change by returning an empty
 * {@link SynonymMappingHighWaterMark}.
 */
public class SynonymChangeStreamRequest extends SynonymSyncRequest {

  private final ResettableChangeStreamClient resettableChangeStreamClient;
  private final SynonymRegistry synonymRegistry;
  private static final Logger LOG = LoggerFactory.getLogger(SynonymChangeStreamRequest.class);

  SynonymChangeStreamRequest(
      ResettableChangeStreamClient resettableChangeStreamClient,
      SynonymMappingId mappingId,
      SynonymMappingDefinition synonymMappingDefinition,
      MongoNamespace namespace,
      CompletableFuture<SynonymMappingHighWaterMark> future,
      SynonymRegistry synonymRegistry,
      SynonymSyncMetrics metrics) {
    super(mappingId, synonymMappingDefinition, namespace, future, metrics);
    this.resettableChangeStreamClient = resettableChangeStreamClient;
    this.synonymRegistry = synonymRegistry;
  }

  static SynonymChangeStreamRequest create(
      ResettableChangeStreamClient cachedChangedStreamClient,
      IndexGeneration indexGeneration,
      SynonymMappingDefinition synonymDefinition,
      SynonymSyncMetrics metrics) {
    var index = indexGeneration.getIndex().asSearchIndex();
    return new SynonymChangeStreamRequest(
        cachedChangedStreamClient,
        SynonymMappingId.from(indexGeneration.getGenerationId(), synonymDefinition.name()),
        synonymDefinition,
        SynonymSourceNamespace.of(indexGeneration, synonymDefinition),
        new CompletableFuture<>(),
        index.getSynonymRegistry(),
        metrics);
  }

  @Override
  SynonymMappingHighWaterMark doWork(SynonymSyncMongoClient mongoClient)
      throws SynonymSyncException {
    ChangeStreamBatch batch = this.resettableChangeStreamClient.getNext();

    if (batch.getRawEvents().size() > 0) {
      // change events detected
      LOG.info("witnessed document event {}", batch.getRawEvents().get(0).toString());
      this.synonymRegistry.observeChange(this.getSynonymMappingDefinition().name());
      // Record that a change stream event was detected, which will trigger a collection scan
      this.metrics.getCollScansTriggeredByChangeStreamCounter().increment();
      return SynonymMappingHighWaterMark.createEmpty();
    }
    return SynonymMappingHighWaterMark.create(batch.getPostBatchResumeToken());
  }
}
