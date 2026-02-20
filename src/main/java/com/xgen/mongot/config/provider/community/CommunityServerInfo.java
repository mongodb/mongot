package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.catalogservice.ServerStateEntry;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import org.bson.types.ObjectId;

/**
 * Stores the server's unique id and customer provided name.
 *
 * <p>The server-id is auto-generated on initial startup and should be considered immutable for the
 * lifetime of the server while the server-name which is provided by the customer may change on
 * process restarts based on what's present in the config file.
 *
 * @param id the server's unique id that's generated on initial startup, cached to disk and reused
 *     for the lifecycle of the server
 * @param name customer provided server name
 */
public record CommunityServerInfo(ObjectId id, Optional<String> name) {

  public CommunityServerInfo(CommunityConfig config) {
    this(
        CommunityServerIdProvider.getServerId(config.storageConfig().dataPath()),
        config.serverConfig().name());
  }

  /**
   * Returns the external name for the server.
   *
   * <p>If the customer provided name is present we concatenate it with the server id to provide
   * both a unique and identifiable name for the server.
   *
   * <p>If the server name was not provided by the customer we default to the serverId as the
   * external name.
   */
  public String getExternalName() {
    if (this.name.isEmpty()) {
      return this.id.toHexString();
    }
    return this.name.get() + "." + this.id.toHexString();
  }

  /**
   * Generates a ServerStateEntry based on this server info with the latest heartbeat ts.
   *
   * @return A server state entry with the latest heartbeat ts
   */
  public ServerStateEntry generateServerStateEntry() {
    return new ServerStateEntry(this.id, this.getExternalName(), Instant.now());
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CommunityServerInfo that = (CommunityServerInfo) o;
    return Objects.equals(this.id, that.id) && Objects.equals(this.name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.id, this.name);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CommunityServerInfo{");
    sb.append("id=").append(this.id);
    sb.append(", name=").append(this.name.orElse("<not-set>"));
    sb.append('}');
    return sb.toString();
  }
}
