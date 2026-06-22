package com.xgen.mongot.logging;

import static com.google.common.truth.Truth.assertThat;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.flogger.FluentLogger;
import com.google.common.flogger.MetadataKey;
import java.util.List;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.event.KeyValuePair;

public class FloggerTest {
  private static final FluentLogger flogger = FluentLogger.forEnclosingClass();
  private static final ch.qos.logback.classic.Logger testLogBack =
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(FloggerTest.class);

  private static final MetadataKey<String> LOG_STRING =
      MetadataKey.single("logString", String.class);
  private static final MetadataKey<Integer> LOG_INTEGER =
      MetadataKey.single("logInteger", Integer.class);
  private static final MetadataKey<Boolean> LOG_BOOLEAN =
      MetadataKey.single("logBoolean", Boolean.class);

  @Test
  public void testFloggerRoutesToLogBack() {
    ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
    listAppender.start();
    testLogBack.addAppender(listAppender);

    testLogBack.info("First logback");
    flogger.atInfo().log("Flogger test: %s.", "this was logged");
    testLogBack.info("Second logback");

    assertThat(listAppender.list.stream().map(ILoggingEvent::getMessage).toList())
        .containsExactly("First logback", "Flogger test: this was logged.", "Second logback")
        .inOrder();
  }

  @Test
  public void testFloggerLogsWithKeyValue() {
    ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
    listAppender.start();
    testLogBack.addAppender(listAppender);

    flogger
        .atInfo()
        .with(LOG_INTEGER, 42)
        .with(LOG_BOOLEAN, true)
        .with(LOG_STRING, "string_value")
        .log("Flogger test: %s.", "this was logged");

    // TODO(CLOUDP-358823): Improve Flogger SLF4J backend to log key value pairs in a way that
    // can be captured by Logback appenders. Currently, the key value pairs are not included in
    // the logging event's keyValuePairs, so we check the formatted message instead.
    ILoggingEvent event = listAppender.list.getFirst();
    List<KeyValuePair> keyValuePairs = event.getKeyValuePairs();
    assertThat(keyValuePairs).isNull();

    assertThat(event.getMessage())
        .isEqualTo(
            "Flogger test: this was logged. "
                + "[CONTEXT logInteger=42 logBoolean=true logString=\"string_value\" ]");
  }
}
