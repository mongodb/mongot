package com.xgen.mongot.replication.mongodb.synonyms;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.version.SynonymMappingId;
import com.xgen.mongot.replication.mongodb.common.SynonymSyncException;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link SynonymSyncRequest} represents a unit of work that can be scheduled on a {@link
 * SynonymManager}. The {@link SynonymSyncRequest#doWork(SynonymSyncMongoClient)} method performs
 * the work for a request.
 */
abstract class SynonymSyncRequest {

  private final SynonymMappingId mappingId;
  private final SynonymMappingDefinition synonymMappingDefinition;
  private final MongoNamespace namespace;
  private final CompletableFuture<SynonymMappingHighWaterMark> future;
  protected final SynonymSyncMetrics metrics;

  SynonymSyncRequest(
      SynonymMappingId mappingId,
      SynonymMappingDefinition synonymMappingDefinition,
      MongoNamespace namespace,
      CompletableFuture<SynonymMappingHighWaterMark> future,
      SynonymSyncMetrics metrics) {
    this.mappingId = mappingId;
    this.synonymMappingDefinition = synonymMappingDefinition;
    this.namespace = namespace;
    this.future = future;
    this.metrics = metrics;
  }

  abstract SynonymMappingHighWaterMark doWork(SynonymSyncMongoClient client)
      throws SynonymSyncException;

  public SynonymMappingId getMappingId() {
    return this.mappingId;
  }

  SynonymMappingDefinition getSynonymMappingDefinition() {
    return this.synonymMappingDefinition;
  }

  MongoNamespace getNamespace() {
    return this.namespace;
  }

  public CompletableFuture<SynonymMappingHighWaterMark> getFuture() {
    return this.future;
  }
}
