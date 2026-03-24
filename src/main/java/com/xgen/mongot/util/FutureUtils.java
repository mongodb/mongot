package com.xgen.mongot.util;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.functionalinterfaces.CheckedRunnable;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class FutureUtils {

  public static final CompletableFuture<Void> COMPLETED_FUTURE =
      CompletableFuture.completedFuture(null);

  private FutureUtils() {}

  /**
   * Unwraps a single layer of {@link ExecutionException} or {@link CompletionException} to return
   * the underlying cause. Returns the original throwable if it is not a wrapper type or has no
   * cause.
   */
  public static Throwable unwrapCause(Throwable exception) {
    if ((exception instanceof ExecutionException || exception instanceof CompletionException)
        && exception.getCause() != null) {
      return exception.getCause();
    }
    return exception;
  }

  /**
   * Ignores the return value of the supplied future and returns a future of Void type.
   *
   * @param source the future whose return value should be ignored
   * @return a future that completes when the source does, but returns a Void value
   */
  public static CompletableFuture<Void> voidFuture(CompletableFuture<?> source) {
    return source.thenApply(ignored -> null);
  }

  /**
   * Swallows the result (exceptional or successful) of the supplied future, returning a future that
   * completes when the source completes, regardless of whether or not it was successful or
   * exceptional.
   *
   * @param source The future whose result should be swallowed.
   * @return A future that completes successfully when the source completes, regardless of whether
   *     or not it was successful or * exceptional.
   */
  public static CompletableFuture<Void> swallowedFuture(CompletableFuture<?> source) {
    return swallowedFuture(source, Optional.empty());
  }

  /**
   * Swallows the result (exceptional or successful) of the supplied future, returning a future that
   * completes when the source completes, regardless of whether or not it was successful or
   * exceptional.
   *
   * @param source The future whose result should be swallowed.
   * @param exceptionCallback A function to call if the source completed exceptionally.
   * @return A future that completes successfully when the source completes, regardless of whether
   *     or not it was successful or exceptional.
   */
  public static CompletableFuture<Void> swallowedFuture(
      CompletableFuture<?> source, Consumer<Throwable> exceptionCallback) {
    return swallowedFuture(source, Optional.of(exceptionCallback));
  }

  private static CompletableFuture<Void> swallowedFuture(
      CompletableFuture<?> source, Optional<Consumer<Throwable>> exceptionCallback) {
    return source.handle(
        (result, throwable) -> {
          if (throwable != null && exceptionCallback.isPresent()) {
            exceptionCallback.get().accept(throwable);
          }

          return null;
        });
  }

  /**
   * Waits for the future to complete and swallows any kind of exceptions, (including {@link
   * InterruptedException}. The {@param exceptionCallback} can be used to react on the exceptions,
   * e.g. for logging purposes.
   */
  public static void getAndSwallow(
      CompletableFuture<Void> future, Consumer<Throwable> exceptionCallback) {
    try {
      future.get();
    } catch (Throwable t) {
      exceptionCallback.accept(t);
    }
  }

  /**
   * Returns a new CompletableFuture that is asynchronously completed by a task running in the given
   * executor after it runs the given action. The task may throw a single class of checked
   * exception.
   *
   * @param runnable the action to run before completing the returned CompletableFuture
   * @param executor the executor to use for asynchronous execution
   * @param exceptionClass the class of the exception type that runnable can throw
   * @return the new CompletableFuture
   */
  public static <E extends Exception> CompletableFuture<Void> checkedRunAsync(
      CheckedRunnable<E> runnable, Executor executor, Class<E> exceptionClass) {
    Runnable wrappedRunnable =
        () -> {
          try {
            runnable.run();
          } catch (Exception e) {
            wrapAndRethrow(e, exceptionClass);
            throw new AssertionError("wrapAndRethrow() didn't throw exception");
          }
        };

    return unwrap(CompletableFuture.runAsync(wrappedRunnable, executor));
  }

  /**
   * Returns a new CompletableFuture that is asynchronously completed by a task running in the given
   * executor with the value obtained by calling the given Supplier. The task may throw a single
   * class of checked exception.
   *
   * @param supplier a function returning the value to be used to complete the returned
   *     CompletableFuture
   * @param executor the executor to use for asynchronous execution
   * @param exceptionClass the class of the exception type that supplier can throw
   * @param <U> the function's return type
   * @return the new CompletableFuture
   */
  public static <U, E extends Exception> CompletableFuture<U> checkedSupplyAsync(
      CheckedSupplier<U, E> supplier, Executor executor, Class<E> exceptionClass) {
    Supplier<U> wrappedSupplier =
        () -> {
          try {
            return supplier.get();
          } catch (Exception e) {
            wrapAndRethrow(e, exceptionClass);
            throw new AssertionError("wrapAndRethrow() didn't throw exception");
          }
        };

    return unwrap(CompletableFuture.supplyAsync(wrappedSupplier, executor));
  }

  /**
   * Returns a new CompletableFuture that is asynchronously completed by a task running in the given
   * executor after it runs the given action. The task will be scheduled to be executed after the
   * supplied delay.
   *
   * @param runnable the action to run before completing the returned CompletableFuture
   * @param delay the amount of time to wait before scheduling the future to run
   * @param executor the executor to use for asynchronous execution
   * @return the new CompletableFuture
   */
  public static CompletableFuture<Void> runAsyncDelayed(
      Runnable runnable, Duration delay, Executor executor) {
    Executor delayedExecutor =
        delay.isZero()
            ? executor
            : CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS, executor);

    return CompletableFuture.runAsync(runnable, delayedExecutor);
  }

  private static class WrappedException extends RuntimeException {
    private final Exception wrapped;

    private WrappedException(Exception wrapped) {
      this.wrapped = wrapped;
    }
  }

  private static <E extends Exception> void wrapAndRethrow(Exception e, Class<E> exceptionClass) {
    if (exceptionClass.isInstance(e)) {
      throw new WrappedException(e);
    }

    throw (RuntimeException) e;
  }

  private static <V> CompletableFuture<V> unwrap(CompletableFuture<V> rawFuture) {
    BiFunction<? super V, Throwable, CompletableFuture<V>> unwrapper =
        (result, throwable) -> {
          if (throwable == null) {
            return CompletableFuture.completedFuture(result);
          }

          @Var Throwable unwrapped = throwable;
          if (throwable instanceof CompletionException) {
            Throwable cause = throwable.getCause();
            unwrapped =
                cause instanceof WrappedException ? ((WrappedException) cause).wrapped : cause;
          }

          return CompletableFuture.failedFuture(unwrapped);
        };

    return rawFuture
        // When the rawFuture completes, map it's result to a new CompletableFuture. If the
        // rawFuture completes successful, map it to a new successfully completed CompletableFuture.
        // If the rawFuture completes exceptionally, inspect the exception to see if it was a
        // wrapped checked exception, and return a failed CompletableFuture with the potentially
        // unwrapped checked exception.
        .handle(unwrapper)
        // Return the CompletableFuture produced by handle() as the result, effectively replacing
        // the result of the rawFuture with a new CompletableFuture, but with potentially a checked
        // exception as the cause of failure.
        .thenCompose(unwrapped -> unwrapped);
  }

  /**
   * Attempts to wait for all of the supplied futures to finish (regardless of the result), or until
   * the supplied timeout has passed.
   */
  public static void awaitAllComplete(Duration timeout, CompletableFuture<?>... futures)
      throws TimeoutException {
    List<CompletableFuture<?>> swallowedFutures =
        Arrays.stream(futures).map(FutureUtils::swallowedFuture).collect(Collectors.toList());
    CompletableFuture<Void> all = allOf(swallowedFutures);

    try {
      all.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (CancellationException | ExecutionException e) {
      throw new AssertionError("swallowed future threw exception", e);
    } catch (InterruptedException e) {
      Crash.because("interrupted while waiting for futures to complete")
          .withThreadDump()
          .withThrowable(e)
          .now();
    }
  }

  /** Returns a CompletableFuture that completes when all of the supplied futures completes. */
  public static CompletableFuture<Void> allOf(List<CompletableFuture<?>> futures) {
    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
  }

  /**
   * Returns a transposed CompletableFuture that collects all completed results into a future of
   * List when all of the supplied futures completes. If there is any exception in input futures,
   * exception will be passed to the returned future to be handled by caller.
   */
  public static <T> CompletableFuture<List<T>> transposeList(List<CompletableFuture<T>> futures) {
    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
        .thenApply(v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
  }

  /**
   * Returns a transposed CompletableFuture that collects all completed results into a future of Map
   * when all of the supplied futures completes. If there is any exception in input futures, *
   * exception will be passed to the returned future to be handled by caller. Note: Existing value
   * will be used if duplicated keys are found.
   */
  public static <K, V> CompletableFuture<Map<K, V>> transposeMap(
      List<CompletableFuture<Map<K, V>>> futures) {
    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
        .thenApply(
            v ->
                futures.stream()
                    .map(CompletableFuture::join)
                    .flatMap(map -> map.entrySet().stream())
                    .collect(
                        Collectors.toUnmodifiableMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (existing, updated) -> existing)));
  }

  /**
   * Returns a transposed CompletableFuture that collects all completed results into a future of Map
   * when all of the supplied futures completes. If there is any exception in input futures, *
   * exception will be passed to the returned future to be handled by caller.
   */
  public static <K, V> CompletableFuture<Map<K, V>> transposeMap(
      Map<K, CompletableFuture<V>> futures) {
    return CompletableFuture.allOf(futures.values().toArray(CompletableFuture[]::new))
        .thenApply(
            v ->
                futures.entrySet().stream()
                    .collect(
                        Collectors.toUnmodifiableMap(
                            Map.Entry::getKey,
                            entry -> entry.getValue().join(),
                            (existing, replacement) -> existing)));
  }
}
