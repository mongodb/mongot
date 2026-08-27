package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;

public interface NamespaceResolver {

  /**
   * Looks up the current name of the collection with the UUID specified in the IndexDefinition.
   *
   * <p>If it exists, it updates the Index's lastObservedCollectionName and returns the sync
   * source's metadata for the collection, including its creation options.
   *
   * <p>If the collection cannot be resolved, it throws a DOES_NOT_EXIST
   * NamespaceResolutionException.
   *
   * <p>If there is an error resolving the namespace, it throws a TRANSIENT
   * NamespaceResolutionException.
   */
  MongoDbCollectionInfo resolveAndUpdateCollectionName(IndexDefinition indexDefinition)
      throws NamespaceResolutionException, InterruptedException, InitialSyncException;

  /**
   * Resolves the current collection name for Index, returning a boolean indicating whether or not
   * the collection name has changed.
   *
   * <p>If the collection cannot be resolved, it throws a DOES_NOT_EXIST
   * NamespaceResolutionException.
   *
   * <p>If there is an error resolving the namespace, it throws a TRANSIENT
   * NamespaceResolutionException.
   */
  boolean isCollectionNameChanged(IndexDefinition indexDefinition, String expectedCollectionName)
      throws NamespaceResolutionException, InterruptedException, InitialSyncException;
}
