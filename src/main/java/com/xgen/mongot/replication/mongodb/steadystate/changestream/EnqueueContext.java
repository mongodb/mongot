package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import java.util.Optional;

/**
 * Per-submission context handed to a {@link ChangeStreamTaskExecutor} alongside the runnable.
 *
 * @param indexDefinition the definition of the index this submission belongs to
 * @param previousBatch the just-processed batch; empty for the first submission
 * @param onDropped invoked if the task cannot be run (e.g. after shutdown) to release resources
 */
record EnqueueContext(
    IndexDefinition indexDefinition,
    Optional<ChangeStreamBatch> previousBatch,
    Runnable onDropped) {}
