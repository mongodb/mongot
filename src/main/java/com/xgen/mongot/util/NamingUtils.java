package com.xgen.mongot.util;

import java.util.Optional;

/** Utility class for naming conventions. */
public class NamingUtils {

  public static String upperToLowerCamelCase(Optional<String> threadName, String defaultName) {
    return threadName
        .filter(s -> !s.isEmpty())
        .map(s -> Character.toLowerCase(s.charAt(0)) + s.substring(1))
        .orElse(defaultName);
  }
}
