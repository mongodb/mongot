package com.xgen.mongot.util.mongodb;

import com.mongodb.ConnectionString;
import java.util.Optional;

public record ConnectionInfo(ConnectionString uri, Optional<SslContextFactory> sslContextFactory) {

  public ConnectionInfo(ConnectionString uri) {
    this(uri, Optional.empty());
  }
}
