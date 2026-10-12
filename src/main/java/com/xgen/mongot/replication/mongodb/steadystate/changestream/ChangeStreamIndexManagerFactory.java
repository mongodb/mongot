package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.PartitionedChangeStreamProgressTracker;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

interface ChangeStreamIndexManagerFactory {

  ChangeStreamIndexManager create(
      IndexDefinition indexDefinition,
      IndexingWorkScheduler indexingWorkScheduler,
      DocumentIndexer documentIndexer,
      MongoNamespace namespace,
      Consumer<ChangeStreamResumeInfo> resumeInfoUpdater,
      IndexMetricsUpdater indexMetricsUpdater,
      CompletableFuture<Void> externalFuture,
      GenerationId generationId,
      Optional<PartitionedChangeStreamProgressTracker> partitionTracker);
}
