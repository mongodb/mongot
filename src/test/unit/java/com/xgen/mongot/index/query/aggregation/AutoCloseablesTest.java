package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

public class AutoCloseablesTest {

  @Test
  public void closeAll_closesEveryResourceWhenTheFirstFails() {
    List<String> closed = new ArrayList<>();
    IllegalStateException first = new IllegalStateException("first");
    IllegalStateException second = new IllegalStateException("second");

    try {
      AutoCloseables.closeAll(
          List.of(
              closing("a", closed, first),
              closing("b", closed, null),
              closing("c", closed, second)));
      fail("expected the first close failure");
    } catch (IllegalStateException exception) {
      assertSame(first, exception);
      assertEquals(1, exception.getSuppressed().length);
      assertSame(second, exception.getSuppressed()[0]);
    }

    assertEquals(List.of("a", "b", "c"), closed);
  }

  @Test
  public void allOf_suppressesCloseFailureOnTheBodyException() {
    AtomicInteger closed = new AtomicInteger();
    IllegalStateException body = new IllegalStateException("body");
    IllegalStateException close = new IllegalStateException("close");
    AutoCloseable resource =
        () -> {
          closed.incrementAndGet();
          throw close;
        };

    try (var ignored = AutoCloseables.allOf(List.of(resource))) {
      throw body;
    } catch (IllegalStateException exception) {
      assertSame(body, exception);
      assertEquals(1, exception.getSuppressed().length);
      assertSame(close, exception.getSuppressed()[0]);
    }

    assertEquals(1, closed.get());
  }

  private static AutoCloseable closing(
      String name, List<String> closed, @Nullable RuntimeException failure) {
    return () -> {
      closed.add(name);
      if (failure != null) {
        throw failure;
      }
    };
  }
}
