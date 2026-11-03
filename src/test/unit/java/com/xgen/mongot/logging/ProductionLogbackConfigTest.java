package com.xgen.mongot.logging;

import static com.google.common.truth.Truth.assertThat;

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import ch.qos.logback.core.ConsoleAppender;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.After;
import org.junit.Test;

/**
 * Loads the real {@code conf/mongot/production/logback.xml} and checks the JSON log line it
 * produces for different server identity properties on the logger context.
 */
public class ProductionLogbackConfigTest {

  private static final File PRODUCTION_LOGBACK = new File("conf/mongot/production/logback.xml");

  @After
  public void clearSystemProperties() {
    System.clearProperty("serverId");
    System.clearProperty("serverName");
    System.clearProperty(Logging.SERVER_ID_PROPERTY);
    System.clearProperty(Logging.SERVER_NAME_PROPERTY);
  }

  @Test
  public void log_serverIdAndNameSet_lineCarriesBoth() throws Exception {
    String line =
        logOneLine(
            Map.of(
                Logging.SERVER_ID_PROPERTY, "abc123", Logging.SERVER_NAME_PROPERTY, "rs0.mongot1"));

    assertThat(line).contains("\"svc\":\"MONGOT\"");
    assertThat(line).contains("\"serverId\":\"abc123\"");
    assertThat(line).contains("\"serverName\":\"rs0.mongot1\"");
  }

  @Test
  public void log_onlyServerIdSet_lineOmitsServerName() throws Exception {
    String line = logOneLine(Map.of(Logging.SERVER_ID_PROPERTY, "abc123"));

    assertThat(line).contains("\"serverId\":\"abc123\"");
    assertThat(line).doesNotContain("serverName");
  }

  @Test
  public void log_noServerIdentitySet_lineHasNeitherField() throws Exception {
    String line = logOneLine(Map.of());

    assertThat(line).contains("\"svc\":\"MONGOT\"");
    assertThat(line).doesNotContain("serverId");
    assertThat(line).doesNotContain("serverName");
  }

  // Negative test: logback's %property{key} falls back to JVM system properties, so the config only
  // reads the namespaced keys. Plain serverId/serverName flags set by a user must not leak in.
  @Test
  public void log_jvmSystemPropertiesWithoutMongotPrefix_areIgnored() throws Exception {
    System.setProperty("serverId", "from-a-jvm-flag");
    System.setProperty("serverName", "from-a-jvm-flag");

    String line = logOneLine(Map.of());

    assertThat(line).contains("\"svc\":\"MONGOT\"");
    assertThat(line).doesNotContain("from-a-jvm-flag");
  }

  @Test
  public void log_jvmSystemPropertiesWithMongotPrefix_areUsedWhenContextHasNone() throws Exception {
    System.setProperty(Logging.SERVER_ID_PROPERTY, "from-a-jvm-flag");
    System.setProperty(Logging.SERVER_NAME_PROPERTY, "from-a-jvm-flag");

    String line = logOneLine(Map.of());

    assertThat(line).contains("\"serverId\":\"from-a-jvm-flag\"");
    assertThat(line).contains("\"serverName\":\"from-a-jvm-flag\"");
  }

  /**
   * Configures a fresh logger context from the production file, sets the given context properties,
   * logs one line and returns the JSON line the file's console appender wrote.
   */
  @SuppressWarnings("unchecked")
  private static String logOneLine(Map<String, String> contextProperties) throws Exception {
    var context = new LoggerContext();
    // The async appender copies the MDC, which needs an adapter on a standalone context.
    context.setMDCAdapter(new LogbackMDCAdapter());
    var configurator = new JoranConfigurator();
    configurator.setContext(context);
    configurator.doConfigure(PRODUCTION_LOGBACK);
    contextProperties.forEach(context::putProperty);

    // Point the console appender configured by the file at a buffer instead of stdout.
    Logger root = context.getLogger(Logger.ROOT_LOGGER_NAME);
    var async = (AsyncAppender) root.getAppender("ASYNC");
    var console = (ConsoleAppender<ILoggingEvent>) async.getAppender("STDOUT");
    var captured = new ByteArrayOutputStream();
    console.setOutputStream(captured);

    context.getLogger("test").info("hello");
    // Stopping the context flushes the async appender.
    context.stop();

    String output = captured.toString(StandardCharsets.UTF_8);
    return output
        .lines()
        .filter(line -> line.contains("\"msg\":\"hello\""))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No log line in appender output: " + output));
  }
}
