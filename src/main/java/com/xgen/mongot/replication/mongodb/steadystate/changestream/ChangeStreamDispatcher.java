package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import org.bson.types.ObjectId;

interface ChangeStreamDispatcher {

  void add(
      IndexDefinition indexDefinition,
      GenerationId generationId,
      ChangeStreamResumeInfo resumeInfo,
      ChangeStreamIndexManager indexManager,
      boolean removeMatchCollectionUuid)
      throws SteadyStateException;

  default void removeIndex(ObjectId indexId) {}

  void shutdown();
}
