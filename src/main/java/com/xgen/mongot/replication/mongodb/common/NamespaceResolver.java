package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.index.definition.IndexDefinition;

public interface NamespaceResolver {

  /**
   * Looks up the current name of the collection with the UUID specified in the IndexDefinition.
   *
   * <p>If it exists, it updates the Index's lastObservedCollectionName and returns the collection
   * name.
   *
   * <p>If the collection cannot be resolved, it throws a DOES_NOT_EXIST
   * NamespaceResolutionException.
   *
   * <p>If there is an error resolving the namespace, it throws a TRANSIENT
   * NamespaceResolutionException.
   */
  String resolveAndUpdateCollectionName(IndexDefinition indexDefinition)
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
