package com.xgen.mongot.config.provider.community;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.mongodb.MongoCredential.OidcCallback;
import com.mongodb.MongoCredential.OidcCallbackContext;
import com.mongodb.MongoCredential.OidcCallbackResult;
import com.xgen.testing.mongot.config.provider.community.CustomOidcConfigBuilder;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class OidcCallbackFactoryTest {

  private static CustomOidcConfig config(String clientId) {
    return CustomOidcConfigBuilder.builder().clientId(clientId).build();
  }

  /** A new callback instance per call (a lambda could be reused by the JVM). */
  private static OidcCallback newCallback() {
    return new OidcCallback() {
      @Override
      public OidcCallbackResult onRequest(OidcCallbackContext context) {
        return new OidcCallbackResult("token");
      }
    };
  }

  /** A callback that remembers the config it was built from. */
  private static final class ConfigCallback implements OidcCallback {
    final OidcConfig config;

    ConfigCallback(OidcConfig config) {
      this.config = config;
    }

    @Override
    public OidcCallbackResult onRequest(OidcCallbackContext context) {
      return new OidcCallbackResult("token");
    }
  }

  @Test
  public void create_equalConfigs_returnsSameCallback() {
    var builds = new AtomicInteger();
    var factory =
        new OidcCallbackFactory(
            config -> {
              builds.incrementAndGet();
              return newCallback();
            });

    OidcCallback first = factory.create(config("mongot"));
    OidcCallback second = factory.create(config("mongot"));

    assertThat(second).isSameInstanceAs(first);
    assertThat(builds.get()).isEqualTo(1);
  }

  @Test
  public void create_differentConfigs_returnsDifferentCallbacks() {
    var factory = new OidcCallbackFactory(config -> newCallback());

    OidcCallback first = factory.create(config("mongot-a"));
    OidcCallback second = factory.create(config("mongot-b"));

    assertThat(second).isNotSameInstanceAs(first);
  }

  @Test
  public void create_differentConfigs_builderReceivesEachConfig() {
    var factory = new OidcCallbackFactory(ConfigCallback::new);
    OidcConfig configA = config("mongot-a");
    OidcConfig configB = config("mongot-b");

    var callbackA = (ConfigCallback) factory.create(configA);
    var callbackB = (ConfigCallback) factory.create(configB);

    assertThat(callbackA.config).isEqualTo(configA);
    assertThat(callbackB.config).isEqualTo(configB);
  }

  @Test
  public void create_builderThrowsOnce_retriesAndCachesLaterSuccess() {
    var builds = new AtomicInteger();
    var factory =
        new OidcCallbackFactory(
            config -> {
              if (builds.incrementAndGet() == 1) {
                throw new IllegalStateException("transient");
              }
              return newCallback();
            });
    OidcConfig config = config("mongot");

    assertThrows(IllegalStateException.class, () -> factory.create(config));
    OidcCallback first = factory.create(config);
    OidcCallback second = factory.create(config);

    assertThat(second).isSameInstanceAs(first);
    assertThat(builds.get()).isEqualTo(2);
  }

  @Test
  public void unsupported_create_throwsIllegalState() {
    var factory = OidcCallbackFactory.unsupported();

    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> factory.create(config("mongot")));

    assertThat(e).hasMessageThat().contains("no OIDC callback factory is installed");
  }
}
