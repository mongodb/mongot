package com.xgen.mongot.util.regex;

import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Thread-confined character-read accounting for custom analyzer regexes.
 *
 * <p>This mitigates backtracking that repeatedly accesses the input. It is NOT an engine-step or
 * wall-clock limit: java.util.regex can do work without calling CharSequence methods. In
 * particular, nested repetitions of empty matches still need engine-level containment
 * (CLOUDP-442862).
 */
public final class RegexBudget {
  public static final long MAX_STAGE_READS = 50_000_000;
  public static final long MAX_OPERATION_READS = 100_000_000;

  private static final ThreadLocal<RegexBudget> CURRENT = new ThreadLocal<>();

  private final @Nullable RegexBudget parent;
  private long remaining;

  private RegexBudget(long limit, @Nullable RegexBudget parent) {
    if (limit <= 0) {
      throw new IllegalArgumentException("Regex read limit must be positive");
    }
    this.remaining = limit;
    this.parent = parent;
  }

  /** A fresh stage budget, created on TokenStream.reset(), not on each token or Matcher.reset(). */
  public static RegexBudget forStage() {
    return new RegexBudget(MAX_STAGE_READS, CURRENT.get());
  }

  /**
   * Aggregate all regex stages executed synchronously for one document, request or synonym record.
   */
  public static Scope openScope() {
    return openScope(MAX_OPERATION_READS);
  }

  /** Opens a bounded scope. Nested scopes also charge their enclosing operation. */
  public static Scope openScope(long limit) {
    @Nullable RegexBudget previous = CURRENT.get();
    CURRENT.set(new RegexBudget(limit, previous));
    return new Scope(previous);
  }

  public static void checkInterrupted() {
    if (Thread.currentThread().isInterrupted()) {
      throw new RegexInterruptedException();
    }
  }

  private void read() {
    if (this.remaining == 0) {
      throw new RegexLimitExceededException();
    }
    // Keep cancellation cooperative without a volatile interrupt read on every character access.
    if ((this.remaining & 1023) == 0) {
      checkInterrupted();
    }
    this.remaining--;
    if (this.parent != null) {
      this.parent.read();
    }
  }

  public CharSequence wrap(CharSequence input) {
    checkInterrupted();
    return new BudgetedSequence(input, 0, input.length());
  }

  private final class BudgetedSequence implements CharSequence {
    private final CharSequence input;
    private final int start;
    private final int end;

    private BudgetedSequence(CharSequence input, int start, int end) {
      this.input = input;
      this.start = start;
      this.end = end;
    }

    @Override
    public int length() {
      return this.end - this.start;
    }

    @Override
    public char charAt(int index) {
      Objects.checkIndex(index, length());
      read();
      return this.input.charAt(this.start + index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
      Objects.checkFromToIndex(start, end, length());
      return new BudgetedSequence(this.input, this.start + start, this.start + end);
    }

    @Override
    public String toString() {
      return this.input.subSequence(this.start, this.end).toString();
    }
  }

  public static final class Scope implements AutoCloseable {
    private final @Nullable RegexBudget previous;

    private Scope(@Nullable RegexBudget previous) {
      this.previous = previous;
    }

    @Override
    public void close() {
      if (this.previous == null) {
        CURRENT.remove();
      } else {
        CURRENT.set(this.previous);
      }
    }
  }
}
