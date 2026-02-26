package com.xgen.mongot.embedding.utils;

import com.mongodb.ConnectionString;
import com.xgen.mongot.util.mongodb.ConnectionStringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for connection string operations specific to the auto-embedding feature.
 *
 * <p>This is a temporary workaround for CLOUDP-360542 where MMS returns connection strings with
 * directConnection=true. Once MMS is updated to return directConnection=false, this class can be
 * removed.
 */
public final class EmbeddingConnectionStringUtils {

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddingConnectionStringUtils.class);

  private EmbeddingConnectionStringUtils() {}

  /**
   * Disables direct connection in a connection string to enable topology discovery.
   *
   * <p>MMS provides connection strings with directConnection=true, which forces the MongoDB driver
   * to connect only to the specified host. This prevents the driver from discovering the primary
   * node in a replica set. For operations that require routing to the primary (writes, linearizable
   * reads), we need to replace directConnection=true with directConnection=false.
   *
   * <p>TODO(CLOUDP-360542): Have MMS return connection strings with directConnection=false so this
   * workaround is no longer needed.
   *
   * @param connectionString the original connection string
   * @return a new connection string with directConnection=false, or the original if it wasn't true
   */
  public static ConnectionString disableDirectConnection(ConnectionString connectionString) {
    if (!Boolean.TRUE.equals(connectionString.isDirectConnection())) {
      return connectionString;
    }
    String originalUri = connectionString.getConnectionString();
    // (?i) an embedded flag expression that enables CASE_INSENSITIVE mode for the pattern (standard
    // Java regex syntax)
    String modifiedUri =
        originalUri.replaceAll("(?i)directConnection=true", "directConnection=false");
    try {
      return ConnectionStringUtil.fromString(modifiedUri);
    } catch (ConnectionStringUtil.InvalidConnectionStringException e) {
      LOG.atError().log("Failed to parse modified connection string, using original");
      return connectionString;
    }
  }
}
