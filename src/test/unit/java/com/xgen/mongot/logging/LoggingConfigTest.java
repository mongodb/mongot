package com.xgen.mongot.logging;

import static com.google.common.truth.Truth.assertThat;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.LoggerFactory;

/**
 * This class tests logging usage. Tests in this class create new instances of loggers and appenders
 * and are isolated from the actual logging configuration of the application.
 */
public class LoggingConfigTest {

  private Logger logger;
  private ListAppender<ILoggingEvent> listAppender;

  @Rule public TestName testName = new TestName();

  @Before
  public void setUp() {
    // Create a new appender and logger for each test to prevent cross-test contamination.
    this.listAppender = new ListAppender<>();
    this.listAppender.start();
    this.logger = (Logger) LoggerFactory.getLogger(this.testName.getMethodName());
    this.logger.addAppender(this.listAppender);
  }

  @Test
  public void testJulToSlf4j() {
    Logging.bridgeJulToSlf4j();
    var julLogger = java.util.logging.Logger.getLogger(this.testName.getMethodName());

    this.logger.info("First logback");
    julLogger.info("JUL logger test");
    this.logger.info("Second logback");

    assertThat(this.listAppender.list.stream().map(ILoggingEvent::getMessage).toList())
        .containsExactly("First logback", "JUL logger test", "Second logback")
        .inOrder();
  }
}
