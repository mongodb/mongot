package com.xgen.mongot.util.mongodb;

import com.google.common.net.HostAndPort;
import com.google.errorprone.annotations.Var;
import com.mongodb.ConnectionString;
import com.mongodb.lang.Nullable;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.ConnectionStringUtil.InvalidConnectionStringException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class ConnectionStringBuilder {
  public static final String SRV_SCHEME = "mongodb+srv";
  public static final String STANDARD_SCHEME = "mongodb";

  private final String scheme;

  @Nullable private String authenticationCredentials;
  @Nullable private List<HostAndPort> hostAndPorts;
  @Nullable private String authenticationDatabase;
  private boolean x509Authentication = false;

  private record Option(String name, String value) {}

  private final List<Option> options;

  private ConnectionStringBuilder(String scheme) {
    this.scheme = scheme;
    this.options = new ArrayList<>();
  }

  public static ConnectionStringBuilder srv() {
    return new ConnectionStringBuilder(SRV_SCHEME);
  }

  public static ConnectionStringBuilder standard() {
    return new ConnectionStringBuilder(STANDARD_SCHEME);
  }

  public ConnectionStringBuilder withAuthenticationCredentials(String username, String password) {
    String encodedUsername = URLEncoder.encode(username, StandardCharsets.UTF_8);
    String encodedPassword = URLEncoder.encode(password, StandardCharsets.UTF_8);

    this.authenticationCredentials = String.format("%s:%s", encodedUsername, encodedPassword);
    return this;
  }

  public ConnectionStringBuilder withHost(String host) {
    return this.withHostAndPort(HostAndPort.fromString(host));
  }

  public ConnectionStringBuilder withHostAndPort(HostAndPort hostAndPort) {
    this.hostAndPorts = List.of(hostAndPort);
    return this;
  }

  public ConnectionStringBuilder withHostAndPorts(Collection<HostAndPort> hostAndPorts) {
    Check.checkState(
        this.scheme.equals(STANDARD_SCHEME),
        "Host list is not supported with SRV connection strings");

    this.hostAndPorts = List.copyOf(hostAndPorts);
    return this;
  }

  public ConnectionStringBuilder withAuthenticationDatabase(String authenticationDatabase) {
    this.authenticationDatabase = authenticationDatabase;
    return this;
  }

  public ConnectionStringBuilder withX509Config() {
    this.withOption("authSource", "$external");
    this.withOption("authMechanism", "MONGODB-X509");

    this.x509Authentication = true;
    return this;
  }

  /**
   * Sets a single-valued option on the connection string. If an option with the same key was
   * previously set, it is replaced. {@code value} must be urlencoded if required.
   *
   * <p>Use this for options that may only appear once in a connection string (e.g. {@code
   * readPreference}, {@code tls}). For options that may appear multiple times, use {@link
   * #withRepeatableOption(String, String)}.
   */
  public ConnectionStringBuilder withOption(String key, String value) {
    this.options.removeIf(e -> e.name().equals(key));
    this.options.add(new Option(key, value));
    return this;
  }

  /**
   * Appends a multi-valued option to the connection string without replacing prior entries with the
   * same key. Each call adds one more occurrence of {@code key=value} to the query string.
   *
   * <p>Use this for options that the MongoDB driver expects to appear multiple times, such as
   * {@code readPreferenceTags} where each occurrence represents one tag set in priority order (e.g.
   * {@code readPreferenceTags=dc:east,rack:1&readPreferenceTags=dc:west}).
   *
   * <p>For single-valued options, use {@link #withOption(String, String)} instead.
   */
  public ConnectionStringBuilder withRepeatableOption(String key, String value) {
    this.options.add(new Option(key, value));
    return this;
  }

  public ConnectionString build() throws InvalidConnectionStringException {
    Check.stateNotNull(this.hostAndPorts, "At least one host must be provided");
    Check.checkState(!this.hostAndPorts.isEmpty(), "At least one host must be provided");

    String scheme = this.scheme;
    @Var String authenticationPrefix = "";
    @Var String authenticationDatabase = "";
    if (!this.x509Authentication) {
      authenticationPrefix =
          Optional.ofNullable(this.authenticationCredentials).map(s -> s + "@").orElse("");
      authenticationDatabase = Objects.requireNonNullElse(this.authenticationDatabase, "");
    }

    String hostList =
        this.hostAndPorts.stream().map(HostAndPort::toString).collect(Collectors.joining(","));
    String options =
        this.options.stream().map(e -> e.name() + "=" + e.value()).collect(Collectors.joining("&"));
    String optionsPrefix = options.isEmpty() ? "" : "?";

    return ConnectionStringUtil.toConnectionString(
        String.format(
            "%s://%s%s/%s%s%s",
            scheme,
            authenticationPrefix,
            hostList,
            authenticationDatabase,
            optionsPrefix,
            options));
  }
}
