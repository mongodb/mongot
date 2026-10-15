package com.xgen.mongot.config.provider.community;

import com.mongodb.MongoCredential.OidcCallback;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Supplies the driver {@link OidcCallback} that acquires access tokens for a configured {@link
 * OidcConfig}.
 *
 * <p>Equal {@link OidcConfig}s (a record, so value equality) always get the same callback instance.
 * {@code ConnectionInfo} equality includes the driver's {@code MongoCredential}, which compares its
 * callback by {@link Object#equals}; a callback built per call would make otherwise identical
 * connections unequal and look like a sync source change. A different config gets its own callback:
 * the builder receives the config, so implementations may vary per config.
 *
 * <p>The enterprise entry point supplies the builder and passes the factory into {@code
 * OnPremMongotBootstrapper}; this published tree never references an enterprise class.
 *
 * <p>One callback instance is shared by every client built from an equal config, so
 * builder-produced callbacks must be thread-safe.
 *
 * <p>The builder must not return null and must not call {@link #create} on the same factory. If it
 * throws, nothing is cached and the next call builds again. Callbacks are never evicted; the number
 * of distinct configs is small.
 *
 * <p>Create one factory and share it; the bootstrapper owns it.
 */
public final class OidcCallbackFactory {

  private final Function<OidcConfig, OidcCallback> builder;
  private final Map<OidcConfig, OidcCallback> callbacks = new ConcurrentHashMap<>();

  public OidcCallbackFactory(Function<OidcConfig, OidcCallback> builder) {
    this.builder = Objects.requireNonNull(builder);
  }

  /**
   * Community default: OIDC is unsupported, so building a callback always fails with {@link
   * IllegalStateException}. A backstop only: community rejects the {@code oidc} block at parse time
   * (the enterprise-gated field).
   */
  public static OidcCallbackFactory unsupported() {
    return new OidcCallbackFactory(
        config -> {
          throw new IllegalStateException(
              "OIDC authentication is not configured for this deployment: no OIDC callback factory"
                  + " is installed");
        });
  }

  /** Returns the callback for the config, building it once per equal config. */
  public OidcCallback create(OidcConfig config) {
    return this.callbacks.computeIfAbsent(config, this.builder);
  }
}
