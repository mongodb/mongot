package com.xgen.mongot.util.regex;

import com.xgen.mongot.util.UserFacingException;

/** A custom analyzer could not complete regex matching within its resource limit. */
public final class RegexLimitExceededException extends RuntimeException
    implements UserFacingException {
  private static final String MESSAGE =
      "Custom analyzer regex exceeded its execution limit. Simplify the analyzer's pattern.";

  public RegexLimitExceededException() {
    super(MESSAGE);
  }

  @Override
  public String getMessage() {
    return MESSAGE;
  }
}
