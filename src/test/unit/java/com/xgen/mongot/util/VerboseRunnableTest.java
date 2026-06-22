package com.xgen.mongot.util;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

public class VerboseRunnableTest {

  @Test
  public void testTaskExecutedWhenExceptionIsThrown() {
    var taskExecuted = new AtomicBoolean(false);
    var expectedException = new Error("test");

    var task =
        new Task(
            () -> {
              taskExecuted.set(true);
              throw expectedException;
            },
            NOPLogger.NOP_LOGGER);
    Error actual = Assert.assertThrows(Error.class, task::run);

    Assert.assertEquals(expectedException, actual);
    Assert.assertTrue(taskExecuted.get());
  }

  private static class Task implements VerboseRunnable {

    private final Runnable action;
    private final Logger logger;

    private Task(Runnable action, Logger logger) {
      this.action = action;
      this.logger = logger;
    }

    @Override
    public void verboseRun() {
      this.action.run();
    }

    @Override
    public Logger getLogger() {
      return this.logger;
    }
  }
}
