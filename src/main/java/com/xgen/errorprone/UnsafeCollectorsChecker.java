package com.xgen.errorprone;

import static com.google.errorprone.matchers.Description.NO_MATCH;

import com.google.auto.service.AutoService;
import com.google.errorprone.BugPattern;
import com.google.errorprone.BugPattern.LinkType;
import com.google.errorprone.BugPattern.SeverityLevel;
import com.google.errorprone.VisitorState;
import com.google.errorprone.bugpatterns.BugChecker;
import com.google.errorprone.bugpatterns.BugChecker.MethodInvocationTreeMatcher;
import com.google.errorprone.fixes.SuggestedFix;
import com.google.errorprone.matchers.Description;
import com.google.errorprone.matchers.method.MethodMatchers;
import com.sun.source.tree.MethodInvocationTree;

/**
 * Errorprone checker that prevents unsafe usage of Collectors.toMap() methods.
 *
 * <p>The 2-argument versions of {@code toMap()}, {@code toUnmodifiableMap()}, and {@code
 * toConcurrentMap()} can throw {@code IllegalStateException} at runtime if duplicate keys are
 * encountered. This checker enforces the use of 3-argument versions that include a merge function
 * to handle duplicates gracefully.
 */
@AutoService(BugChecker.class)
@BugPattern(
    name = "UnsafeCollectors",
    summary = "Unsafe usage of Collectors.toMap() methods without merge functions",
    severity = SeverityLevel.ERROR,
    linkType = LinkType.NONE)
public class UnsafeCollectorsChecker extends BugChecker implements MethodInvocationTreeMatcher {

  @SuppressWarnings(
      "deprecation") // MethodMatchers.MethodMatcher is deprecated but still functional
  private static final MethodMatchers.MethodMatcher TO_MAP =
      MethodMatchers.staticMethod()
          .onClass("java.util.stream.Collectors")
          .namedAnyOf("toMap", "toUnmodifiableMap", "toConcurrentMap");

  @Override
  public Description matchMethodInvocation(MethodInvocationTree tree, VisitorState state) {
    if (TO_MAP.matches(tree, state) && tree.getArguments().size() < 3) {
      // Edge case handling: Ensure we have at least 2 arguments before accessing them
      if (tree.getArguments().size() < 2) {
        // This should not happen for valid Collectors.toMap() calls, but handle gracefully
        return this.buildDescription(tree)
            .setMessage("Invalid toMap() call: requires at least key and value mappers.")
            .build();
      }

      // Create suggested fix by adding a merge function parameter
      // Insert the merge function after the last argument
      SuggestedFix fix =
          SuggestedFix.builder()
              .postfixWith(tree.getArguments().getLast(), ", (existing, replacement) -> existing")
              .setShortDescription("Add merge function to handle duplicate keys safely")
              .build();

      String message =
          "Specify a merge function for toMap() to handle duplicate keys safely. Without it,"
              + " duplicate keys cause runtime IllegalStateException. Common merge functions:"
              + " (existing, replacement) -> existing (keep first), (existing, replacement) ->"
              + " replacement (keep last), or (a, b) -> { throw new"
              + " IllegalStateException(\"Duplicate key: \" + a); } (explicit error).";

      return this.buildDescription(tree).setMessage(message).addFix(fix).build();
    }

    return NO_MATCH;
  }
}
