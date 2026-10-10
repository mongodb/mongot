package com.xgen.mongot.util.regex;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.regex.Pattern;
import org.junit.Test;

public class RegexBudgetTest {
  private static final Pattern LINEAR = Pattern.compile("a+");

  @Test
  public void charAt_interruptedAfterWrapping_abortsMatchingWithoutClearingFlag() {
    var matcher = LINEAR.matcher(RegexBudget.forStage().wrap("a".repeat(5000)));
    Thread.currentThread().interrupt();
    try {
      assertThrows(RegexInterruptedException.class, matcher::find);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void charAt_repeatedAccess_exhaustsBudget() {
    try (var ignored = RegexBudget.openScope(2)) {
      CharSequence text = RegexBudget.forStage().wrap("abc");

      assertThat(text.charAt(0)).isEqualTo('a');
      assertThat(text.charAt(0)).isEqualTo('a');

      assertThrows(RegexLimitExceededException.class, () -> text.charAt(0));
    }
  }

  @Test
  public void subSequence_access_chargesOriginalBudget() {
    try (var ignored = RegexBudget.openScope(1)) {
      CharSequence text = RegexBudget.forStage().wrap("abc");
      CharSequence slice = text.subSequence(1, 3);

      assertThat(slice.toString()).isEqualTo("bc");
      assertThat(slice.charAt(0)).isEqualTo('b');

      assertThrows(RegexLimitExceededException.class, () -> text.charAt(0));
    }
  }

  @Test
  public void forStage_multipleStages_shareOperationLimit() {
    try (var ignored = RegexBudget.openScope(1)) {
      CharSequence first = RegexBudget.forStage().wrap("a");
      CharSequence second = RegexBudget.forStage().wrap("b");

      assertThat(first.charAt(0)).isEqualTo('a');

      assertThrows(RegexLimitExceededException.class, () -> second.charAt(0));
    }
  }

  @Test
  public void close_nestedScope_restoresAndChargesParent() {
    try (var outer = RegexBudget.openScope(1)) {
      try (var inner = RegexBudget.openScope(10)) {
        assertThat(RegexBudget.forStage().wrap("a").charAt(0)).isEqualTo('a');
      }

      assertThrows(
          RegexLimitExceededException.class, () -> RegexBudget.forStage().wrap("b").charAt(0));
    }

    assertThat(RegexBudget.forStage().wrap("c").charAt(0)).isEqualTo('c');
  }

  @Test
  public void wrap_interrupted_preservesInterruptAndReportsCancellation() {
    Thread.currentThread().interrupt();
    try {
      assertThrows(RegexInterruptedException.class, () -> RegexBudget.forStage().wrap("a"));
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
  }
}
