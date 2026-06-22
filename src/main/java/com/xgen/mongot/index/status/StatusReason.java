package com.xgen.mongot.index.status;

public interface StatusReason {

  String formatMessage(Object... details);

  static String getString(String text, Object[] details) {
    int placeholderCount = text.split("%s", -1).length - 1;
    if (placeholderCount == 0) {
      return text;
    }

    if (placeholderCount != details.length) {
      throw new IllegalArgumentException("Wrong number of details provided.");
    }

    return String.format(text, details);
  }
}
