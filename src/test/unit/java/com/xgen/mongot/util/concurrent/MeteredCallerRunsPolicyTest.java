package com.xgen.mongot.util.concurrent;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.Assert;
import org.junit.Test;

public class MeteredCallerRunsPolicyTest {

  @Test
  public void shouldIncrementCounterOnRejection() {
    var registry = new SimpleMeterRegistry();
    var counter = registry.counter("executorMetrics.rejectedExecutionCount");
    var policy = new MeteredCallerRunsPolicy(counter);

    policy.rejectedExecution(() -> {}, (ThreadPoolExecutor) Executors.newFixedThreadPool(2));
    Assert.assertEquals(1, counter.count(), 0.0);
  }
}
