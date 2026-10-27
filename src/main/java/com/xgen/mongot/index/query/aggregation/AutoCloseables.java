package com.xgen.mongot.index.query.aggregation;

import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 * Closes several {@link AutoCloseable}s as one resource.
 *
 * <p>A failure from one {@code close()} must not skip the rest. The first failure is thrown after
 * every resource has been closed, and later failures are suppressed on it. Use with
 * try-with-resources so a failure from the body keeps the close failure as a suppressed exception
 * instead of replacing the body failure.
 */
public final class AutoCloseables {
  private AutoCloseables() {}

  /** A closeable whose {@code close()} does not throw a checked exception. */
  @FunctionalInterface
  public interface SilentCloseable extends AutoCloseable {
    @Override
    void close();
  }

  /** One resource that closes every element of {@code closeables}. */
  public static SilentCloseable allOf(Iterable<? extends AutoCloseable> closeables) {
    List<AutoCloseable> snapshot = new ArrayList<>();
    closeables.forEach(snapshot::add);
    List<AutoCloseable> frozen = List.copyOf(snapshot);
    return () -> closeAll(frozen);
  }

  /** Closes every resource. The first failure is thrown; later ones are suppressed on it. */
  public static void closeAll(Iterable<? extends AutoCloseable> closeables) {
    @Var RuntimeException thrown = null;
    for (AutoCloseable closeable : closeables) {
      try {
        closeable.close();
      } catch (RuntimeException exception) {
        thrown = suppress(thrown, exception);
      } catch (Exception exception) {
        thrown = suppress(thrown, new IllegalStateException(exception));
      }
    }
    if (thrown != null) {
      throw thrown;
    }
  }

  private static RuntimeException suppress(
      @Nullable RuntimeException thrown, RuntimeException next) {
    if (thrown == null) {
      return next;
    }
    thrown.addSuppressed(next);
    return thrown;
  }
}
