package com.xgen.mongot.util.mongodb;

import com.mongodb.lang.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class SyncSourceConfig {

  /**
   * A direct connection URI targeting a single mongod replica-set member. Used for initial sync,
   * which must talk to one specific node with {@code directConnect=true}.
   *
   * <p>In community, selected at runtime by the {@code InitialSyncHostProvider} and empty while no
   * healthy host matching the configured read preference has been found. In Atlas, this is the
   * {@code mongoDbUri} from the conf-call response.
   */
  public final Optional<ConnectionInfo> mongodSingleHostReplicationUri;

  /**
   * A cluster-level URI for steady-state replication reads.
   *
   * <p>In community this is configured with the read preference from the {@code replicationReader}
   * config block and has {@code directConnection=false}. In dedicated Atlas this is the {@code
   * mongoDbClusterUri} from the conf-call response (with {@code directConnection=false}). In
   * coupled Atlas this retains {@code directConnection=true} to pin traffic to the co-located
   * mongod.
   */
  public final ConnectionInfo mongodClusterReplicationUri;

  /**
   * A cluster-level URI for operations that may write or that require reaching the primary (e.g.
   * index-management commands, auto-embedding). Always has {@code directConnection=false}.
   *
   * <p>In community this is built from the replica-set seed list. In Atlas this is derived from the
   * {@code mongoDbClusterUri} in the conf-call response with {@code directConnection=false}.
   */
  public final ConnectionInfo mongodClusterReadWriteUri;

  /**
   * A direct connection URI targeting a single, healthy mongos instance. Used for initial sync in
   * sharded deployments.
   *
   * <p>In community, selected at runtime by the {@code InitialSyncHostProvider} and empty while no
   * healthy mongos has been found. In Atlas, derived from the {@code mongosUri} in the conf-call
   * response. Empty in non-sharded deployments; see {@link #isSharded} to distinguish that from the
   * transient "host not yet selected" case.
   */
  public final Optional<ConnectionInfo> mongosSingleHostReplicationUri;

  /**
   * A cluster-level URI covering all mongos instances ({@code directConnection=false}). Used for
   * cluster-wide operations such as index-management commands and auto-embedding in sharded
   * deployments.
   *
   * <p>In community this is built from the router seed list with {@code secondaryPreferred} read
   * preference. In Atlas this is derived from the {@code mongosUri} in the conf-call response with
   * {@code directConnection=false}. Empty in non-sharded deployments.
   */
  public final Optional<ConnectionInfo> mongosClusterReadWriteUri;

  /**
   * {@code true} when the sync source is a sharded cluster (i.e. a mongos router is configured).
   * Distinguishes a non-sharded deployment from the transient case where {@link
   * #mongosSingleHostReplicationUri} is empty because no healthy mongos has been found yet.
   */
  public final boolean isSharded;

  /**
   * Per-host direct-connect URIs keyed by hostname. In dedicated Atlas clusters, used to pin each
   * initial sync to a specific replica-set member. Always empty in community deployments.
   */
  public final Optional<Map<String, ConnectionInfo>> mongodUris;

  private SyncSourceConfig(
      Optional<ConnectionInfo> mongodSingleHostReplicationUri,
      ConnectionInfo mongodClusterReplicationUri,
      ConnectionInfo mongodClusterReadWriteUri,
      Optional<ConnectionInfo> mongosSingleHostReplicationUri,
      Optional<ConnectionInfo> mongosClusterReadWriteUri,
      boolean isSharded,
      Optional<Map<String, ConnectionInfo>> mongodUris) {
    this.mongodSingleHostReplicationUri = mongodSingleHostReplicationUri;
    this.mongodClusterReplicationUri = mongodClusterReplicationUri;
    this.mongodClusterReadWriteUri = mongodClusterReadWriteUri;
    this.mongosSingleHostReplicationUri = mongosSingleHostReplicationUri;
    this.mongosClusterReadWriteUri = mongosClusterReadWriteUri;
    this.isSharded = isSharded;
    this.mongodUris = mongodUris;
  }

  /**
   * Returns {@code true} when all single-host URIs needed to begin initial sync are present: {@link
   * #mongodSingleHostReplicationUri} must always be set, and in sharded deployments {@link
   * #mongosSingleHostReplicationUri} must also be set.
   */
  public boolean hasReplicationUrisAvailable() {
    return this.mongodSingleHostReplicationUri.isPresent()
        && (!this.isSharded || this.mongosSingleHostReplicationUri.isPresent());
  }

  /**
   * Asserts that all single-host URIs needed to begin initial sync are present.
   *
   * <p>{@link #mongodSingleHostReplicationUri} must always be set. In sharded deployments, {@link
   * #mongosSingleHostReplicationUri} must also be set. These fields are {@code Optional} because
   * they are populated asynchronously — in community by the {@code InitialSyncHostProvider}, and in
   * Atlas derived from the conf-call response.
   *
   * <p>Call this before starting a {@code MongoDbReplicationManager} or auto-embedding {@code
   * MaterializedViewManager}. If the required URIs cannot be guaranteed to be present, use the
   * {@code MongoDbNoOpReplicationManager} instead.
   *
   * @throws IllegalStateException if a required single-host URI is absent.
   */
  public void validateReplicationUrisAvailable() {
    if (this.mongodSingleHostReplicationUri.isEmpty()) {
      throw new IllegalStateException(
          "syncSourceConfig.mongodSingleHostReplicationUri must be present");
    }

    if (this.isSharded && this.mongosSingleHostReplicationUri.isEmpty()) {
      throw new IllegalStateException(
          "syncSourceConfig.mongosSingleHostReplicationUri must be present if we're in a"
              + " sharded environment");
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private Optional<ConnectionInfo> mongodSingleHostReplicationUri = Optional.empty();
    @Nullable private ConnectionInfo mongodClusterReplicationUri = null;
    @Nullable private ConnectionInfo mongodClusterReadWriteUri = null;
    private Optional<ConnectionInfo> mongosSingleHostReplicationUri = Optional.empty();
    private Optional<ConnectionInfo> mongosClusterReadWriteUri = Optional.empty();
    private boolean isSharded = false;
    private Optional<Map<String, ConnectionInfo>> mongodUris = Optional.empty();

    private Builder() {}

    public Builder mongodSingleHostReplicationUri(Optional<ConnectionInfo> uri) {
      this.mongodSingleHostReplicationUri = uri;
      return this;
    }

    public Builder mongodSingleHostReplicationUri(ConnectionInfo uri) {
      this.mongodSingleHostReplicationUri = Optional.of(uri);
      return this;
    }

    public Builder mongodClusterReplicationUri(ConnectionInfo uri) {
      this.mongodClusterReplicationUri = uri;
      return this;
    }

    public Builder mongodClusterReadWriteUri(ConnectionInfo uri) {
      this.mongodClusterReadWriteUri = uri;
      return this;
    }

    public Builder mongosSingleHostReplicationUri(Optional<ConnectionInfo> uri) {
      this.mongosSingleHostReplicationUri = uri;
      return this;
    }

    public Builder mongosSingleHostReplicationUri(ConnectionInfo uri) {
      this.mongosSingleHostReplicationUri = Optional.of(uri);
      return this;
    }

    public Builder mongosClusterReadWriteUri(Optional<ConnectionInfo> uri) {
      this.mongosClusterReadWriteUri = uri;
      return this;
    }

    public Builder mongosClusterReadWriteUri(ConnectionInfo uri) {
      this.mongosClusterReadWriteUri = Optional.of(uri);
      return this;
    }

    public Builder isSharded(boolean isSharded) {
      this.isSharded = isSharded;
      return this;
    }

    public Builder mongodUris(Optional<Map<String, ConnectionInfo>> uris) {
      this.mongodUris = uris;
      return this;
    }

    public SyncSourceConfig build() {
      if (this.mongodClusterReplicationUri == null) {
        throw new IllegalArgumentException("mongodClusterReplicationUri must be set");
      }
      if (this.mongodClusterReadWriteUri == null) {
        throw new IllegalArgumentException("mongodClusterReadWriteUri must be set");
      }
      if (this.isSharded && this.mongosClusterReadWriteUri.isEmpty()) {
        throw new IllegalArgumentException(
            "mongosClusterReadWriteUri must be set in a sharded environment");
      }
      return new SyncSourceConfig(
          this.mongodSingleHostReplicationUri,
          this.mongodClusterReplicationUri,
          this.mongodClusterReadWriteUri,
          this.mongosSingleHostReplicationUri,
          this.mongosClusterReadWriteUri,
          this.isSharded,
          this.mongodUris);
    }
  }

  public SyncSourceConfig copyWithUpdatedReplicationUris(
      Optional<ConnectionInfo> mongodUri, Optional<ConnectionInfo> mongosUri) {
    return new SyncSourceConfig(
        mongodUri,
        this.mongodClusterReplicationUri,
        this.mongodClusterReadWriteUri,
        mongosUri,
        this.mongosClusterReadWriteUri,
        this.isSharded,
        this.mongodUris);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SyncSourceConfig that = (SyncSourceConfig) o;
    return this.isSharded == that.isSharded
        && this.mongodSingleHostReplicationUri.equals(that.mongodSingleHostReplicationUri)
        && this.mongodClusterReplicationUri.equals(that.mongodClusterReplicationUri)
        && this.mongodClusterReadWriteUri.equals(that.mongodClusterReadWriteUri)
        && this.mongosSingleHostReplicationUri.equals(that.mongosSingleHostReplicationUri)
        && this.mongosClusterReadWriteUri.equals(that.mongosClusterReadWriteUri)
        && this.mongodUris.equals(that.mongodUris);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.mongodSingleHostReplicationUri,
        this.mongodClusterReplicationUri,
        this.mongodClusterReadWriteUri,
        this.mongosSingleHostReplicationUri,
        this.mongosClusterReadWriteUri,
        this.isSharded,
        this.mongodUris);
  }
}
