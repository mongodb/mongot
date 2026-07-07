package com.xgen.mongot.util.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.spi.dns.DnsClient;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionStringUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionStringUtil.class);

  public static class InvalidConnectionStringException extends Exception {
    public InvalidConnectionStringException(Throwable cause) {
      super(cause);
    }
  }

  public static ConnectionString toConnectionString(String string)
      throws InvalidConnectionStringException {
    return toConnectionString(string, Optional.empty());
  }

  public static ConnectionString toConnectionString(String string, Optional<DnsClient> dnsClient)
      throws InvalidConnectionStringException {
    try {
      return dnsClient
          .map(client -> new ConnectionString(string, client))
          .orElseGet(() -> new ConnectionString(string));
    } catch (Exception e) {
      throw new InvalidConnectionStringException(e);
    }
  }

  public static ConnectionInfo toConnectionInfo(String string)
      throws InvalidConnectionStringException {
    return new ConnectionInfo(toConnectionString(string));
  }

  public static ConnectionInfo toConnectionInfoUnchecked(String string) {
    return new ConnectionInfo(new ConnectionString(string));
  }

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
      return toConnectionString(modifiedUri);
    } catch (ConnectionStringUtil.InvalidConnectionStringException e) {
      LOG.atError().log("Failed to parse modified connection string, using original");
      return connectionString;
    }
  }
}
