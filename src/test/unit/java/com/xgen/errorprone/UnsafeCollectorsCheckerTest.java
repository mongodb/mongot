package com.xgen.errorprone;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * This test demonstrates the ErrorProne checker functionality by showing examples of: - Unsafe
 * collector usage (would fail compilation without suppression when ErrorProne plugin is applied) -
 * Safe collector usage (compiles successfully)
 *
 * <p><b>Note:</b> This test itself does not run the ErrorProne checker to avoid circular
 * dependencies. The UnsafeCollectors ErrorProne checker is applied globally via the {@code
 * unsafe_collectors_plugin} in {@code //bazel/java:package.bzl}.
 *
 * <p><b>To verify the ErrorProne checker is working on production code:</b>
 *
 * <ol>
 *   <li>Create a test file with unsafe {@code Collectors.toMap()} usage (2 arguments)
 *   <li>Try to build it with: {@code bazel build //path/to/your:target}
 *   <li>Compilation should FAIL with UnsafeCollectors errors
 *   <li>Add {@code @SuppressWarnings("UnsafeCollectors")} or use safe 3-argument version
 *   <li>Compilation should succeed
 * </ol>
 *
 * <p>This verifies that:
 *
 * <ul>
 *   <li>The ErrorProne checker is properly integrated and running globally
 *   <li>It correctly detects unsafe collector usage in production code
 *   <li>{@code @SuppressWarnings("UnsafeCollectors")} properly suppresses the warnings
 *   <li>Safe 3-argument collector methods are allowed
 * </ul>
 */
@RunWith(JUnit4.class)
public class UnsafeCollectorsCheckerTest {

  /** Test unsafe toMap() usage - would fail compilation without suppression. */
  @Test
  @SuppressWarnings("UnsafeCollectors") // Remove this to test ErrorProne checker
  public void testUnsafeToMapUsage() {
    List<String> items = List.of("a", "b", "c");
    // This should trigger UnsafeCollectors error when suppression is removed
    Map<String, String> result =
        items.stream().collect(Collectors.toMap(s -> s, s -> s.toUpperCase()));
    assertEquals(3, result.size());
  }

  /** Test unsafe toUnmodifiableMap() usage - would fail compilation without suppression. */
  @Test
  @SuppressWarnings("UnsafeCollectors") // Remove this to test ErrorProne checker
  public void testUnsafeToUnmodifiableMapUsage() {
    List<String> items = List.of("a", "b", "c");
    // This should trigger UnsafeCollectors error when suppression is removed
    Map<String, String> result =
        items.stream().collect(Collectors.toUnmodifiableMap(s -> s, s -> s.toUpperCase()));
    assertEquals(3, result.size());
  }

  /** Test unsafe toConcurrentMap() usage - would fail compilation without suppression. */
  @Test
  @SuppressWarnings("UnsafeCollectors") // Remove this to test ErrorProne checker
  public void testUnsafeToConcurrentMapUsage() {
    List<String> items = List.of("a", "b", "c");
    // This should trigger UnsafeCollectors error when suppression is removed
    ConcurrentMap<String, String> result =
        items.stream().collect(Collectors.toConcurrentMap(s -> s, s -> s.toUpperCase()));
    assertEquals(3, result.size());
  }

  /** Test unsafe method references - would fail compilation without suppression. */
  @Test
  @SuppressWarnings("UnsafeCollectors") // Remove this to test ErrorProne checker
  public void testUnsafeMethodReferences() {
    List<String> items = List.of("a", "b", "c");
    // This should trigger UnsafeCollectors error when suppression is removed
    Map<String, String> result =
        items.stream().collect(Collectors.toMap(String::toString, String::toUpperCase));
    assertEquals(3, result.size());
  }

  /** Test safe collector usage - these should always compile successfully. */
  @Test
  public void testSafeCollectorsWork() {
    List<String> items = List.of("a", "b", "c");

    // These should NOT trigger UnsafeCollectors errors - they have merge functions
    Map<String, String> result1 =
        items.stream()
            .collect(
                Collectors.toMap(
                    s -> s, s -> s.toUpperCase(), (existing, replacement) -> existing));
    assertEquals(3, result1.size());

    Map<String, String> result2 =
        items.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    s -> s, s -> s.toUpperCase(), (existing, replacement) -> existing));
    assertEquals(3, result2.size());

    ConcurrentMap<String, String> result3 =
        items.stream()
            .collect(
                Collectors.toConcurrentMap(
                    s -> s, s -> s.toUpperCase(), (existing, replacement) -> existing));
    assertEquals(3, result3.size());
  }
}
