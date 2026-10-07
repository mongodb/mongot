package com.xgen.mongot.util.mongodb;

import static com.google.common.truth.Truth.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCredential;
import com.mongodb.MongoCredential.OidcCallback;
import com.mongodb.MongoCredential.OidcCallbackContext;
import com.mongodb.MongoCredential.OidcCallbackResult;
import java.util.Optional;
import org.junit.Test;

public class ConnectionInfoTest {

  private static final String CALLBACK_MARKER = "SECRET-CALLBACK-MARKER";
  private static final String PASSWORD_MARKER = "SECRET-PASSWORD-MARKER";

  /** A callback whose string form is a unique marker, so any leak of it is detectable. */
  private static final class MarkerCallback implements OidcCallback {
    @Override
    public OidcCallbackResult onRequest(OidcCallbackContext context) {
      return new OidcCallbackResult("token");
    }

    @Override
    public String toString() {
      return CALLBACK_MARKER;
    }
  }

  private static ConnectionInfo infoWithOidc() {
    MongoCredential credential =
        MongoCredential.createOidcCredential(null)
            .withMechanismProperty(MongoCredential.OIDC_CALLBACK_KEY, new MarkerCallback());
    return new ConnectionInfo(
        new ConnectionString(
            "mongodb://host:27017/?authMechanism=MONGODB-OIDC&authSource=$external"),
        Optional.empty(),
        Optional.of(credential));
  }

  @Test
  public void toString_withOidcCredential_doesNotExposeCallback() {
    assertThat(infoWithOidc().toString()).doesNotContain(CALLBACK_MARKER);
  }

  @Test
  public void toString_withScramUserInfo_doesNotExposePassword() {
    ConnectionInfo info =
        new ConnectionInfo(
            new ConnectionString(
                "mongodb://" + "user" + ":" + PASSWORD_MARKER + "@" + "host:27017"));

    assertThat(info.toString()).doesNotContain(PASSWORD_MARKER);
    assertThat(info.toString()).contains("host:27017");
  }

  @Test
  public void toString_withoutOidcCredential_reportsNone() {
    assertThat(new ConnectionInfo(new ConnectionString("mongodb://host:27017")).toString())
        .contains("oidcCredential=<none>");
  }
}
