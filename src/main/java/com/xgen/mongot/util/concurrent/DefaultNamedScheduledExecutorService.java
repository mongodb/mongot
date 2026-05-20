package com.xgen.mongot.util.concurrent;

import com.xgen.mongot.util.concurrent.Executors.CountingNamedThreadFactory;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class DefaultNamedScheduledExecutorService implements NamedScheduledExecutorService {

  private final ScheduledExecutorService delegate;
  private final String name;
  private final MeterRegistry meterRegistry;
  private final Optional<CountingNamedThreadFactory> threadFactory;

  DefaultNamedScheduledExecutorService(
      ScheduledExecutorService delegate,
      String name,
      MeterRegistry meterRegistry,
      Optional<CountingNamedThreadFactory> threadFactory) {
    this.delegate = delegate;
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
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    return this.delegate.schedule(command, delay, unit);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    return this.delegate.schedule(callable, delay, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
      Runnable command, long initialDelay, long period, TimeUnit unit) {
    return this.delegate.scheduleAtFixedRate(command, initialDelay, period, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(
      Runnable command, long initialDelay, long delay, TimeUnit unit) {
    return this.delegate.scheduleWithFixedDelay(command, initialDelay, delay, unit);
  }

  @Override
  public Optional<LiveThreadIdsRegistry> getLiveThreadIdsRegistry() {
    return this.threadFactory.map(CountingNamedThreadFactory::getLiveThreadIdsRegistry);
  }
}
