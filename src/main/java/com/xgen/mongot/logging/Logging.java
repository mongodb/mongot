package com.xgen.mongot.logging;

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.FileAppender;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class Logging {

  /** Attempts to stop logging, flushing any buffered log lines. */
  public static void shutdown() {
    try {
      ((LoggerContext) LoggerFactory.getILoggerFactory()).stop();
    } catch (Exception ignored) {
      // Swallow any exceptions, since we're just trying to shut down.
    }
  }

  /**
   * Attempts to set the root logger level to the provided Logback log level. If the provided level
   * is not valid, sets the root logger level to {@code INFO}.
   */
  public static void setRootLevel(String levelString) {
    Level level = Level.toLevel(levelString, Level.INFO);
    ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(level);
  }

  public static Level getRootLevel() {
    return ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).getLevel();
  }

  /**
   * Removes any existing handlers attached to the JUL root logger and installs an instance of
   * SLF4JBridgeHandler on the root logger as the sole JUL handler in the system.
   */
  public static void bridgeJulToSlf4j() {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  /**
   * Disables the console appender on the root logger and replaces it with a file appender. If the
   * async appender is not present or the console appender is not present, this will do nothing.
   *
   * <p>This method is not thread-safe and should only be called from a single-threaded environment.
   * If called from a multithreaded context, logs may be lost during the reconfiguration period.
   */
  public static void enableFileAppender(String logFile) {
    // Only one appender can ever be attached to an async appender, so iterating to its first
    // appender will return the console appender.
    if (LoggerFactory.getILoggerFactory() instanceof LoggerContext loggerContext
        && LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) instanceof Logger logger
        && logger.iteratorForAppenders().next() instanceof AsyncAppender asyncAppender
        && asyncAppender.iteratorForAppenders().next()
            instanceof ConsoleAppender<ILoggingEvent> consoleAppender) {

      // Create a file appender with the existing encoder.
      var encoder = consoleAppender.getEncoder();
      var fileAppender = new FileAppender<ILoggingEvent>();
      fileAppender.setContext(loggerContext);
      fileAppender.setFile(logFile);
      fileAppender.setEncoder(encoder);

      // Create a new async appender to wrap the file appender.
      var newAsyncAppender = new AsyncAppender();
      newAsyncAppender.setContext(loggerContext);

      // Detach the async console appender from the logger and start the new async file appender.
      logger.detachAndStopAllAppenders();
      fileAppender.start();
      newAsyncAppender.addAppender(fileAppender);
      newAsyncAppender.start();
      logger.addAppender(newAsyncAppender);
    }
  }
}
