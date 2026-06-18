package com.xgen.mongot.replication.mongodb.steadystate.changestream;

/**
 * Metadata about the change stream batch that was just processed by a {@link
 * ChangeStreamTaskExecutor}, supplied to the executor at re-enqueue time so that it can make
 * scheduling decisions (e.g., picking a queue based on whether the prior batch did any work).
 */
record PreviousBatchInfo(boolean hadDocuments) {}
