package com.xgen.mongot.embedding.mongodb.common;

import com.google.common.annotations.VisibleForTesting;

/**
 * Default implementation of {@link InternalDatabaseResolver}. Resolves every source database to the
 * same fixed base name.
 */
public class DefaultInternalDatabaseResolver implements InternalDatabaseResolver {

  private final String baseName;

  public DefaultInternalDatabaseResolver() {
    this(DEFAULT_BASE_NAME);
  }

  @VisibleForTesting
  public DefaultInternalDatabaseResolver(String baseName) {
    this.baseName = baseName;
  }

  @Override
  public String resolve(String sourceDatabaseName) {
    return this.baseName;
  }

  @Override
  public String resolveDefault() {
    return this.baseName;
  }
}
