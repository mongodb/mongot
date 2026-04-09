package com.xgen.mongot.util.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.net.HostAndPort;
import org.junit.Test;

public class ConnectionStringBuilderTest {

  @Test
  public void testStandard() throws ConnectionStringUtil.InvalidConnectionStringException {
    var builtConnectionString =
        ConnectionStringBuilder.standard()
            .withAuthenticationCredentials("admin", "password") // kingfisher:ignore
            .withHost("localhost")
            .build()
            .toString();
    var expected = "mongodb://admin:password@localhost/"; // kingfisher:ignore

    assertEquals(expected, builtConnectionString);
  }

  @Test
  public void testWithPort() throws ConnectionStringUtil.InvalidConnectionStringException {
    var builtConnectionString =
        ConnectionStringBuilder.standard()
            .withAuthenticationCredentials("admin", "password") // kingfisher:ignore
            .withHost("localhost:9999")
            .build()
            .toString();
    var expected = "mongodb://admin:password@localhost:9999/"; // kingfisher:ignore

    assertEquals(expected, builtConnectionString);
  }

  @Test
  public void testWithAuthDatabase() throws ConnectionStringUtil.InvalidConnectionStringException {
    var builtConnectionString =
        ConnectionStringBuilder.standard()
            .withAuthenticationCredentials("admin", "password") // kingfisher:ignore
            .withHost("localhost:9999")
            .withAuthenticationDatabase("admin")
            .build()
            .toString();
    var expected = "mongodb://admin:password@localhost:9999/admin"; // kingfisher:ignore

    assertEquals(expected, builtConnectionString);
  }

  @Test
  public void testWithOptions() throws ConnectionStringUtil.InvalidConnectionStringException {
    var builtConnectionString =
        ConnectionStringBuilder.standard()
            .withHost("localhost")
            .withOption("tls", "true")
            .build()
            .toString();
    var expected = "mongodb://localhost/?tls=true";

    assertEquals(expected, builtConnectionString);
  }

  @Test
  public void testFull() throws ConnectionStringUtil.InvalidConnectionStringException {
    var builtConnectionString =
        ConnectionStringBuilder.standard()
            .withAuthenticationCredentials("admin", "password") // kingfisher:ignore
            .withHost("localhost:27017")
            .withAuthenticationDatabase("admin")
            .withOption("tls", "true")
            .withOption("readPreference", "primaryPreferred")
            .build()
            .toString();
    var expected =
        "mongodb://admin:password@localhost:27017/" // kingfisher:ignore
            + "admin?tls=true&readPreference=primaryPreferred";

    assertEquals(expected, builtConnectionString);
  }

  @Test
  public void testWithRepeatableOption_multipleValuesForSameKey()
      throws ConnectionStringUtil.InvalidConnectionStringException {
    var builtConnectionString =
        ConnectionStringBuilder.standard()
            .withHost("localhost")
            .withOption("readPreference", "nearest")
            .withRepeatableOption("readPreferenceTags", "dc:east,rack:1")
            .withRepeatableOption("readPreferenceTags", "dc:west")
            .build()
            .toString();

    assertEquals(
        "mongodb://localhost/?readPreference=nearest"
            + "&readPreferenceTags=dc:east,rack:1"
            + "&readPreferenceTags=dc:west",
        builtConnectionString);
  }

  @Test(expected = ConnectionStringUtil.InvalidConnectionStringException.class)
  public void testInvalid() throws ConnectionStringUtil.InvalidConnectionStringException {
    var ignored =
        ConnectionStringBuilder.standard()
            .withHost("localhost")
            .withOption("tls", "true")
            .withOption("ssl", "false")
            .build()
            .toString();
  }

  @Test
  public void testX509Config() throws ConnectionStringUtil.InvalidConnectionStringException {
    String uri =
        ConnectionStringBuilder.standard()
            .withHostAndPort(HostAndPort.fromString("mongod:27017"))
            .withX509Config()
            .withOption("tls", "true")
            .build()
            .toString();

    assertFalse("X.509 URI should not contain user:password", uri.contains("@"));
    assertTrue(
        "Should contain authMechanism=MONGODB-X509", uri.contains("authMechanism=MONGODB-X509"));
    assertTrue("Should contain authSource", uri.contains("authSource=$external"));
  }
}
