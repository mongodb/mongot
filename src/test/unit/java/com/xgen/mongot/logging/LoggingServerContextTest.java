package com.xgen.mongot.logging;

import static com.google.common.truth.Truth.assertThat;

import ch.qos.logback.classic.LoggerContext;
import java.util.Optional;
import org.junit.Test;

public class LoggingServerContextTest {

  @Test
  public void setServerContext_withName_setsBothContextProperties() {
    var context = new LoggerContext();

    Logging.setServerContext(context, "abc123", Optional.of("my-server"));

    assertThat(context.getProperty(Logging.SERVER_ID_PROPERTY)).isEqualTo("abc123");
    assertThat(context.getProperty(Logging.SERVER_NAME_PROPERTY)).isEqualTo("my-server");
  }

  @Test
  public void setServerContext_withoutName_setsOnlyServerId() {
    var context = new LoggerContext();

    Logging.setServerContext(context, "abc123", Optional.empty());

    assertThat(context.getProperty(Logging.SERVER_ID_PROPERTY)).isEqualTo("abc123");
    assertThat(context.getProperty(Logging.SERVER_NAME_PROPERTY)).isNull();
  }
}
