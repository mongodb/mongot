package com.xgen.testing.mongot.index.lucene.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Fails when the pinned Lucene version moves, so code copied out of Lucene gets re-diffed. */
public final class LucenePinCanary {

  private static final String LUCENE_PIN_REPO_PATH = "bazel/java/maven_search_query.MODULE.bazel";

  private static final Pattern LUCENE_FORK_VERSION_PATTERN =
      Pattern.compile("_LUCENE_FORK_VERSION = \"([^\"]+)\"");

  /** The version the copied methods were last diffed against. */
  private static final String EXPECTED_LUCENE_FORK_VERSION = "10.4.0-3";

  private LucenePinCanary() {}

  /**
   * Canary that fails whenever the pinned Lucene version changes, as a reminder that the methods
   * copied from Lucene into {@code callerDescription} may need upstream's changes ported. On
   * failure, re-diff the copied methods against Lucene's BaseKnnVectorsFormatTestCase, port any
   * upstream test changes, and then update the expected version here.
   *
   * @param callerDescription the class holding the copied methods, named in the failure message
   */
  public static void assertLucenePinUnchanged(String callerDescription) throws IOException {
    // TEST_SRCDIR and TEST_WORKSPACE locate the runfiles tree under bazel test; fall back to a
    // repo-relative path for non-bazel runners such as plain IDE JUnit run configurations.
    String testSrcdir = System.getenv("TEST_SRCDIR");
    String testWorkspace = System.getenv("TEST_WORKSPACE");
    Path lucenePinFile =
        testSrcdir != null && testWorkspace != null
            ? Path.of(testSrcdir, testWorkspace, LUCENE_PIN_REPO_PATH)
            : Path.of(LUCENE_PIN_REPO_PATH);
    Matcher lucenePinMatcher = LUCENE_FORK_VERSION_PATTERN.matcher(Files.readString(lucenePinFile));
    assertTrue(
        "Lucene version pin not found in " + lucenePinFile + "; update this canary",
        lucenePinMatcher.find());
    assertEquals(
        "The pinned Lucene version changed: re-diff the copied methods in "
            + callerDescription
            + " against Lucene's BaseKnnVectorsFormatTestCase and port any upstream test changes,"
            + " then update LucenePinCanary.EXPECTED_LUCENE_FORK_VERSION",
        EXPECTED_LUCENE_FORK_VERSION,
        lucenePinMatcher.group(1));
  }
}
