package com.xgen.mongot.util.concurrent;

import com.xgen.mongot.util.concurrent.Executors.CountingNamedThreadFactory;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class DefaultNamedExecutorService implements NamedExecutorService {

  private final ExecutorService delegate;
  private final ExecutorService originalExecutor;
  private final String name;
  private final MeterRegistry meterRegistry;
  private final Optional<CountingNamedThreadFactory> threadFactory;

  DefaultNamedExecutorService(
      ExecutorService delegate,
      ExecutorService originalExecutor,
      String name,
      MeterRegistry meterRegistry,
      Optional<CountingNamedThreadFactory> threadFactory) {
    this.delegate = delegate;
    this.originalExecutor = originalExecutor;
    this.name = name;
    this.meterRegistry = meterRegistry;
    this.threadFactory = threadFactory;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public MeterRegistry getMeterRegistry() {
    return this.meterRegistry;
  }

  @Override
  public void shutdown() {
    this.delegate.shutdown();
    this.removeMetrics();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return this.delegate.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return this.delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return this.delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return this.delegate.awaitTermination(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return this.delegate.submit(task);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return this.delegate.submit(task, result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return this.delegate.submit(task);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return this.delegate.invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    return this.delegate.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return this.delegate.invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return this.delegate.invokeAny(tasks, timeout, unit);
  }

  @Override
  public void execute(Runnable command) {
    this.delegate.execute(command);
  }

  @Override
  public OptionalInt getActiveCount() {
    if (this.originalExecutor instanceof ThreadPoolExecutor tpe) {
      return OptionalInt.of(tpe.getActiveCount());
    }
    return OptionalInt.empty();
  }

  @Override
  public OptionalInt getMaxPoolSize() {
    if (this.originalExecutor instanceof ThreadPoolExecutor tpe) {
      return OptionalInt.of(tpe.getMaximumPoolSize());
    }
    return OptionalInt.empty();
  }

  @Override
  public OptionalInt getQueueSize() {
    if (this.originalExecutor instanceof ThreadPoolExecutor tpe) {
      return OptionalInt.of(tpe.getQueue().size());
    }
    return OptionalInt.empty();
  }

  @Override
  public Optional<LiveThreadIdsRegistry> getLiveThreadIdsRegistry() {
    return this.threadFactory.map(CountingNamedThreadFactory::getLiveThreadIdsRegistry);
  }
}
