package com.xgen.mongot.util.regex;

import com.xgen.mongot.util.UserFacingException;
import java.util.concurrent.CancellationException;

/** Cooperative cancellation is not a persistent failure of an index's analyzer. */
public final class RegexInterruptedException extends CancellationException
    implements UserFacingException {
  public RegexInterruptedException() {
    super("Custom analyzer regex execution was interrupted.");
  }
}
