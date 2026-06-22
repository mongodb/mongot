package com.xgen.mongot.logging;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.filter.FilteringGeneratorDelegate;
import com.fasterxml.jackson.core.filter.TokenFilter;
import com.fasterxml.jackson.core.filter.TokenFilter.Inclusion;
import net.logstash.logback.decorate.JsonGeneratorDecorator;

/**
 * This class is used by logback.xml configurations to customize JSON logging formats. It filters
 * empty fields such that keys with a value that is null, empty string, empty array, or empty object
 * are omitted. The filter is applied to composite fields (i.e., arrays of all nulls are omitted).
 */
public final class OmitEmptyFieldJsonDecorator implements JsonGeneratorDecorator {

  private static final TokenFilter NULL_EXCLUDING_FILTER =
      new TokenFilter() {
        // Omit null.
        @Override
        public boolean includeNull() {
          return false;
        }

        // Omit empty strings.
        @Override
        public boolean includeString(String value) {
          return value != null && !value.isEmpty();
        }
      };

  @Override
  public JsonGenerator decorate(JsonGenerator generator) {
    return new FilteringGeneratorDelegate(
        generator, NULL_EXCLUDING_FILTER, Inclusion.INCLUDE_ALL_AND_PATH, true);
  }
}
