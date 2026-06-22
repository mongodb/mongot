package com.xgen.mongot.logging;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertNull;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.HashMap;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.event.KeyValuePair;

public class DefaultKeyValueLoggerTest {

  private ListAppender<ILoggingEvent> listAppender;

  @Before
  public void setUp() {
    // Attach a list appender to the logger for this class to verify results.
    this.listAppender = new ListAppender<>();
    this.listAppender.start();
    Logger logger = (Logger) LoggerFactory.getLogger(DefaultKeyValueLoggerTest.class);
    logger.addAppender(this.listAppender);
  }

  @Test
  public void testInfoLoggingWithDefaultKeyValues() {
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", 123);
    defaultKeyValues.put("indexGeneration", 456);

    DefaultKeyValueLogger defaultKeyValueLogger =
        DefaultKeyValueLogger.getLogger(DefaultKeyValueLoggerTest.class, defaultKeyValues);
    defaultKeyValueLogger.info("Test message");

    assertThat(this.listAppender.list).hasSize(1);
    ILoggingEvent logEvent = this.listAppender.list.getFirst();
    assertThat(logEvent.getFormattedMessage()).isEqualTo("Test message");
    assertThat(logEvent.getKeyValuePairs())
        .containsExactly(
            new KeyValuePair("indexId", 123), new KeyValuePair("indexGeneration", 456));
  }

  @Test
  public void testInfoLoggingWithNoKeyValues() {
    DefaultKeyValueLogger defaultKeyValueLogger =
        DefaultKeyValueLogger.getLogger(DefaultKeyValueLoggerTest.class, new HashMap<>());
    defaultKeyValueLogger.info("Test message");

    assertThat(this.listAppender.list).hasSize(1);
    ILoggingEvent logEvent = this.listAppender.list.getFirst();
    assertThat(logEvent.getFormattedMessage()).isEqualTo("Test message");
    assertNull(logEvent.getKeyValuePairs());
  }

  @Test
  public void testInfoLoggingWithThrowable() {
    DefaultKeyValueLogger defaultKeyValueLogger =
        DefaultKeyValueLogger.getLogger(DefaultKeyValueLoggerTest.class, new HashMap<>());
    defaultKeyValueLogger.info("Test message", new Throwable("Test throwable"));

    assertThat(this.listAppender.list).hasSize(1);
    ILoggingEvent logEvent = this.listAppender.list.getFirst();
    assertThat(logEvent.getFormattedMessage()).isEqualTo("Test message");
    assertThat(logEvent.getThrowableProxy().getMessage()).isEqualTo("Test throwable");
  }

  @Test
  public void testInfoLoggingWithFormattedArgs() {
    DefaultKeyValueLogger defaultKeyValueLogger =
        DefaultKeyValueLogger.getLogger(DefaultKeyValueLoggerTest.class, new HashMap<>());
    defaultKeyValueLogger.info("Test message {} {}", "arg1", "arg2");

    assertThat(this.listAppender.list).hasSize(1);
    ILoggingEvent logEvent = this.listAppender.list.getFirst();
    assertThat(logEvent.getFormattedMessage()).isEqualTo("Test message arg1 arg2");
    assertNull(logEvent.getKeyValuePairs());
  }

  @Test
  public void testDuplicateDefaultKeyValue() {
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", 123);
    DefaultKeyValueLogger defaultKeyValueLogger =
        DefaultKeyValueLogger.getLogger(DefaultKeyValueLoggerTest.class, defaultKeyValues);
    defaultKeyValueLogger.atInfo().addKeyValue("indexId", 789).log("Test message");

    assertThat(this.listAppender.list).hasSize(1);
    ILoggingEvent logEvent = this.listAppender.list.getFirst();
    assertThat(logEvent.getFormattedMessage()).isEqualTo("Test message");
    assertThat(logEvent.getKeyValuePairs())
        .containsExactly(new KeyValuePair("indexId", 123), new KeyValuePair("indexId", 789));
  }

  @Test
  public void testMutatingMapDoesNotChangeDefaultKeyValues() {
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    DefaultKeyValueLogger defaultKeyValueLogger =
        DefaultKeyValueLogger.getLogger(DefaultKeyValueLoggerTest.class, defaultKeyValues);
    defaultKeyValues.put("indexId", 123);
    defaultKeyValueLogger.info("Test message");

    assertThat(this.listAppender.list).hasSize(1);
    ILoggingEvent logEvent = this.listAppender.list.getFirst();
    assertThat(logEvent.getFormattedMessage()).isEqualTo("Test message");
    assertNull(logEvent.getKeyValuePairs());
  }

  @Test
  public void testCanAppendNullValue() {
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", null);

    DefaultKeyValueLogger defaultKeyValueLogger =
        DefaultKeyValueLogger.getLogger(DefaultKeyValueLoggerTest.class, defaultKeyValues);
    defaultKeyValueLogger.info("Test message");

    assertThat(this.listAppender.list).hasSize(1);
    ILoggingEvent logEvent = this.listAppender.list.getFirst();
    assertThat(logEvent.getFormattedMessage()).isEqualTo("Test message");
    assertThat(logEvent.getKeyValuePairs()).containsExactly(new KeyValuePair("indexId", null));
  }
}
