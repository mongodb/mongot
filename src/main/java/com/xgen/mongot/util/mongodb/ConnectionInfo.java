package com.xgen.mongot.util.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCredential;
import java.util.Optional;

public record ConnectionInfo(
    ConnectionString uri,
    Optional<SslContextFactory> sslContextFactory,
    Optional<MongoCredential> oidcCredential) {

  public ConnectionInfo(ConnectionString uri) {
    this(uri, Optional.empty(), Optional.empty());
  }

  public ConnectionInfo(ConnectionString uri, Optional<SslContextFactory> sslContextFactory) {
    this(uri, sslContextFactory, Optional.empty());
  }

  /**
   * Redacts secrets: {@link ConnectionString#toString()} returns the raw string, including any
   * SCRAM {@code user:password} userinfo, and {@link #oidcCredential()} holds the token callback.
   * Only the hosts are printed.
   */
  @Override
  public String toString() {
    return "ConnectionInfo[hosts="
        + this.uri.getHosts()
        + ", sslContextFactory="
        + this.sslContextFactory
        + ", oidcCredential="
        + (this.oidcCredential.isPresent() ? "<redacted>" : "<none>")
        + "]";
  }
}
