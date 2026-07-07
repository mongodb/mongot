package com.xgen.mongot.embedding.mongodb.common;

/**
 * Resolves the internal database name used for materialized view collections and auto-embedding
 * leases.
 */
public interface InternalDatabaseResolver {

  String DEFAULT_BASE_NAME = "__mdb_internal_search";

  /**
   * Resolves the internal database name for the given source database.
   *
   * @param sourceDatabaseName the database name from the index definition.
   * @return the resolved internal database name.
   */
  String resolve(String sourceDatabaseName);

  /**
   * Returns the base internal database name without any source database context. Use this when no
   * source database is available (e.g. during lease manager construction before any indexes are
   * added).
   */
  String resolveDefault();

  /** Returns true if this resolver runs on a dedicated (single-tenant) cluster. */
  default boolean isDedicatedCluster() {
    return false;
  }

  static String defaultBaseName() {
    return DEFAULT_BASE_NAME;
  }
}
