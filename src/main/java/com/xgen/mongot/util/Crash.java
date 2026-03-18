package com.xgen.mongot.util;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.RestrictedApi;
import com.xgen.mongot.logging.Logging;
import com.xgen.mongot.util.functionalinterfaces.CheckedRunnable;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Crash {

  private static final Logger LOG = LoggerFactory.getLogger(Crash.class);
  private static final int FAIL_EXIT_CODE = 1;
  private static final Pattern FAILED_TO_SHUTDOWN_EXECUTOR =
      Pattern.compile("^failed to shut down .* executor.*");

  private static final AtomicReference<Optional<Path>> crashLogPath =
      new AtomicReference<>(Optional.empty());
  private static final Thread.UncaughtExceptionHandler FAILING_UNCAUGHT_EXCEPTION_HANDLER =
      new Crash.OnUncaughtExceptionHandler();
  private final String message;
  private Optional<Throwable> throwable;
  private boolean dumpThreads;
  private Optional<String> noCrashLogReason;
  private Optional<CrashCategory> crashCategory;
  private static final AtomicReference<Boolean> isShutdownStarted = new AtomicReference<>(false);

  // Overrides the halt call in now() for testing. When present, halt() is not called on
  // Runtime.INSTANCE; instead, the handler is invoked with the exit code.
  private static volatile Optional<Consumer<Integer>> haltHandlerForTesting = Optional.empty();

  @VisibleForTesting
  @RestrictedApi(
      explanation =
          "Only tests should override Crash.halt(). For compilation to succeed, annotate the call"
              + " site with @Crash.TestOnlyHaltHandler.",
      link = "https://jira.mongodb.org/browse/CLOUDP-388565",
      allowlistAnnotations = {TestOnlyHaltHandler.class})
  public static void setHaltHandlerForTesting(Consumer<Integer> handler) {
    haltHandlerForTesting = Optional.of(handler);
  }

  @VisibleForTesting
  @RestrictedApi(
      explanation =
          "Only tests should clear the Crash.halt() override. For compilation to succeed,"
              + " annotate the call site with @Crash.TestOnlyHaltHandler.",
      link = "https://jira.mongodb.org/browse/CLOUDP-388565",
      allowlistAnnotations = {TestOnlyHaltHandler.class})
  public static void clearHaltHandlerForTesting() {
    haltHandlerForTesting = Optional.empty();
  }

  private Crash(String message) {
    this.message = message;
    this.throwable = Optional.empty();
    this.dumpThreads = false;
    this.noCrashLogReason = Optional.empty();
    this.crashCategory = Optional.empty();
  }

  /**
   * Sets the path to write a crash log to in the event of failure.
   *
   * <p>Can only be invoked once.
   */
  public static void setCrashLogPath(Path crashLogPath) {
    var wasEmpty = Crash.crashLogPath.compareAndSet(Optional.empty(), Optional.of(crashLogPath));
    if (!wasEmpty) {
      Crash.because("CRASH_LOG_PATH already set").now();
    }
  }

  /** Crash any thread that has an exception reach the UncaughtExceptionHandler. */
  public static void setDefaultUncaughtExceptionHandler() {
    Thread.setDefaultUncaughtExceptionHandler(FAILING_UNCAUGHT_EXCEPTION_HANDLER);
  }

  /**
   * This is a flag set to true at the start of the shutdown process via a shutdownHook. Any crash
   * that occurs during the process of shutting down is tagged so we can prioritize investigating
   * crashes occurring during normal operation
   */
  public static void setShutdownStartedFlag() {
    var wasEmpty = Crash.isShutdownStarted.compareAndSet(false, true);
    if (!wasEmpty) {
      LOG.warn("Shutdown flag should only be set once");
    }
    LOG.info("Successfully set Crash Shutdown flag");
  }

  /** Includes the provided message in the crash logging. */
  @CheckReturnValue
  public static Crash because(String message) {
    return new Crash(message);
  }

  /**
   * Crashes if the thread does not join within the supplied timeout, or if the current thread is
   * interrupted while waiting.
   *
   * <p>Includes a message describing how the thread did not join and a thread dump in all crash
   * logging.
   */
  public static void ifDoesNotJoin(Thread thread, Duration timeout) {
    checkArg(!timeout.isNegative() && !timeout.isZero(), "timeout must be positive");

    Crash.because(String.format("Interrupted waiting for thread %s to finish", thread.getName()))
        .withThreadDump()
        .ifThrows(() -> thread.join(timeout.toMillis()));

    if (thread.isAlive()) {
      Crash.because(String.format("Timed out waiting for thread %s to finish", thread.getName()))
          .withThreadDump()
          .now();
    }
  }

  static String createStructuredLogs(
      String exceptionName,
      String crashReason,
      String stackTrace,
      String threadDump,
      String mongotVersion,
      CrashCategory category) {
    // structured logs for crash alert parsing
    BsonDocument crashDoc = new BsonDocument();
    crashDoc.put("type", new BsonString("mongot-crash"));
    crashDoc.put("exception", new BsonString(exceptionName));
    crashDoc.put("reason", new BsonString(crashReason));
    crashDoc.put("stackTrace", new BsonString(stackTrace));
    crashDoc.put("threadDump", new BsonString(threadDump));
    crashDoc.put("mongotVersion", new BsonString(mongotVersion));
    crashDoc.put("crashCategory", new BsonString(category.toString()));
    return crashDoc.toString();
  }

  static String buildCrashLogs(
      String exceptionName,
      String crashReason,
      String stackTrace,
      String threadDump,
      String mongotVersion,
      CrashCategory category) {
    String structuredLogs =
        createStructuredLogs(
            exceptionName, crashReason, stackTrace, threadDump, mongotVersion, category);
    StringBuilder crashLogBuilder = new StringBuilder().append(structuredLogs).append("\n");
    crashLogBuilder.append(crashReason).append("\n");
    crashLogBuilder.append(stackTrace);
    return crashLogBuilder.toString();
  }

  /** Includes a stack trace for the supplied throwable in the crash logging. */
  @CheckReturnValue
  public Crash withThrowable(Throwable t) {
    this.throwable = Optional.of(t);
    return this;
  }

  /**
   * Warning: Avoid using this unless you are certain that a crash log is undesirable. When crash
   * logs are created in Atlas it alerts us on slack. And so, we only disable them when we know a
   * crash is harmless and wish to mute the alert.
   *
   * <p>Call sites must be annotated with {@link SafeWithoutCrashLog} otherwise compilation will
   * fail.
   */
  @RestrictedApi(
      explanation =
          "No crash log means an alert won't be created. For compilation to succeed,"
              + " annotate the call site with @Crash.SafeWithoutCrashLog.",
      link = "https://jira.mongodb.org/browse/CLOUDP-109531",
      allowlistAnnotations = {SafeWithoutCrashLog.class})
  public Crash withoutCrashLog(String reason) {
    this.noCrashLogReason = Optional.of(reason);
    return this;
  }

  /** Includes a thread dump in the crash logging. */
  @CheckReturnValue
  public Crash withThreadDump() {
    this.dumpThreads = true;
    return this;
  }

  /**
   * Manually assign a CrashCategory to a crash
   *
   * @param category - the CrashCategory value to assign
   * @return the Crash
   */
  public Crash withCrashCategory(CrashCategory category) {
    this.crashCategory = Optional.of(category);
    return this;
  }

  /** Crashes if the runnable throws an exception or error. */
  public void ifThrowsExceptionOrError(CheckedRunnable<Exception> runnable) {
    try {
      runnable.run();
    } catch (Throwable e) {
      this.withThrowable(e).now();
    }
  }

  /** Crashes if the supplier throws an exception or error. */
  public <T> T ifThrowsExceptionOrError(CheckedSupplier<T, Exception> supplier) {
    try {
      return supplier.get();
    } catch (Throwable e) {
      this.withThrowable(e).now();
      return Check.unreachable("Crash.now() should have halted the JVM");
    }
  }

  /**
   * Crashes if the runnable throws an exception.
   *
   * @deprecated This method is dangerous as it only catches Exceptions and not Errors which can
   *     lead to unexpected behavior and subtle bugs. See HELP-90208 for an example. Use {@link
   *     #ifThrowsExceptionOrError(CheckedRunnable)} instead.
   */
  @Deprecated(forRemoval = true)
  public void ifThrows(CheckedRunnable<Exception> runnable) {
    try {
      runnable.run();
    } catch (Exception e) {
      this.withThrowable(e).now();
    }
  }

  /**
   * Crashes if the supplier throws an exception.
   *
   * @deprecated This method is dangerous as it only catches Exceptions and not Errors which can
   *     lead to unexpected behavior and subtle bugs. See HELP-90208 for an example. Use {@link
   *     #ifThrowsExceptionOrError(CheckedSupplier)} instead.
   */
  @Deprecated(forRemoval = true)
  public <T> T ifThrows(CheckedSupplier<T, Exception> supplier) {
    try {
      return supplier.get();
    } catch (Exception e) {
      this.withThrowable(e).now();
      return Check.unreachable("Crash.now() should have halted the JVM");
    }
  }

  /**
   * Crashes if the supplied future completes exceptionally.
   *
   * <p>Includes the exception the future failed with and a thread dump in all crash logging.
   */
  public <T> CompletableFuture<T> ifCompletesExceptionally(CompletableFuture<T> source) {
    return source.whenComplete(
        (ignored, throwable) -> {
          if (throwable != null) {
            this.withThrowable(throwable).withThreadDump().now();
          }
        });
  }

  /** Crashes immediately. */
  public void now() {
    try {
      LOG.atError()
          .addKeyValue("noCrashLogReason", this.noCrashLogReason)
          .addKeyValue("threadDump", this.dumpThreads ? Runtime.INSTANCE.getThreadDump() : "")
          .addKeyValue("shutdownStarted", Crash.isShutdownStarted.get())
          .setCause(this.throwable.orElse(null))
          .log(this.message);

      String throwableTrace =
          this.throwable.map(t -> "\n" + ExceptionUtils.getStackTrace(t)).orElse("");
      String exceptionName =
          this.throwable.map(Object::getClass).map(Class::getTypeName).orElse("");

      String threadDump =
          this.dumpThreads ? "\nthread dump:\n" + Runtime.INSTANCE.getThreadDump() : "";

      // Use MongotVersionResolver.getOptional() instead of create().getVersion() to prevent a
      // loop, because create() Crashes if the version can't be found.
      String mongotVersion = MongotVersionResolver.getOptional().orElse("unknown");

      CrashCategory category = this.crashCategory.orElse(assignCrashCategory(throwableTrace));

      String crashLogs =
          buildCrashLogs(
              exceptionName, this.message, throwableTrace, threadDump, mongotVersion, category);

      if (this.noCrashLogReason.isEmpty()) {
        this.writeCrashLog(crashLogs);
      }

      // Shut down the logger so that we flush any buffered log lines.
      Logging.shutdown();
    } finally {
      // Our shutdown hooks are intended for graceful shutdown, and as such may enter some
      // synchronized methods.
      // However, we want to be able to Fail within other synchronized methods. If you simply
      // exit() in a method that is synchronized with one called in a shutdown hook, you will
      // deadlock and never shut down.
      // So instead, we'll simply halt (which skips shutdown hooks) instead.
      haltHandlerForTesting.ifPresentOrElse(
          h -> h.accept(FAIL_EXIT_CODE), () -> Runtime.INSTANCE.halt(FAIL_EXIT_CODE));
    }
  }

  private void writeCrashLog(String crashLogs) {
    try {
      var crashLogPath = Crash.crashLogPath.get();
      if (crashLogPath.isPresent()) {
        Files.writeString(crashLogPath.get(), crashLogs);
      }
    } catch (Exception e) {
      LOG.error("Caught error attempting to write crash log, ignoring.", e);
    }
  }

  public @interface SafeWithoutCrashLog {}

  public @interface TestOnlyHaltHandler {}

  private static class OnUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    @Override
    public void uncaughtException(Thread t, Throwable e) {
      try {
        Crash.because("Uncaught exception thrown from thread " + t.getName())
            .withThrowable(e)
            .now();
      } finally {
        // Likely another exception or JVM Error (like OOM) happened when we tried to notify and
        // halt. Let's halt silently.
        Runtime.INSTANCE.halt(FAIL_EXIT_CODE);
      }
    }
  }

  public CrashCategory assignCrashCategory(String stackTrace) {
    CrashCategory category;
    if (isShutdownStarted.get()) {
      category = CrashCategory.SHUTDOWN;
    } else if (stackTrace.contains("No space left on device")) {
      return CrashCategory.DISK_FULL;
    } else if (this.message.contains("OOM") || stackTrace.contains("OutOfMemoryError")) {
      category = CrashCategory.OOM;
    } else if (FAILED_TO_SHUTDOWN_EXECUTOR.matcher(this.message).matches()) {
      category = CrashCategory.EXECUTOR_SHUTDOWN;
    } else {
      category = CrashCategory.OTHER;
    }
    return category;
  }

  public enum CrashCategory {
    SHUTDOWN,
    DISK_FULL,
    OOM,
    EXECUTOR_SHUTDOWN,
    OTHER
  }
}
