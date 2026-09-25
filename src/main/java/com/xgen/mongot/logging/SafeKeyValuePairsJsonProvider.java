package com.xgen.mongot.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import net.logstash.logback.composite.AbstractJsonProvider;
import org.jetbrains.annotations.Nullable;
import org.slf4j.event.KeyValuePair;

/**
 * SafeKeyValuePairsJsonProvider is a custom JSON provider that ensures values are safe JSON types
 * to prevent logging errors.
 *
 * <p>Unlike {@code KeyValuePairsJsonProvider}, it avoids unsafe behavior such as generating
 * unreadable object representations or throwing exceptions when complex objects are logged as
 * values.
 */
public class SafeKeyValuePairsJsonProvider extends AbstractJsonProvider<ILoggingEvent> {

  /**
   * Safely writes key-value pairs from the logging event to the JSON generator.
   *
   * <p>This method filters key-value pairs in the following scenarios:
   *
   * <ul>
   *   <li>If the key is null or blank
   *   <li>If the value is an empty {@link Optional}
   *   <li>If the value is an empty string (matching {@code OmitEmptyFieldJsonDecorator})
   *   <li>If the key has been overridden by a subsequent key value pair
   * </ul>
   *
   * @param generator the JsonGenerator to write to
   * @param event the logging event containing the key-value pairs
   */
  @Override
  public void writeTo(JsonGenerator generator, ILoggingEvent event) throws IOException {
    for (KeyValuePair keyValuePair : printableKeyValuePairs(event)) {
      generator.writeFieldName(keyValuePair.key);
      generator.writeObject(keyValuePair.value);
    }
  }

  /**
   * Returns the key-value pairs the local JSON encoder prints, with values already coerced to
   * JSON-safe types, applying the same rules as {@link #writeTo}: non-null, non-blank keys,
   * non-empty {@link Optional} values, non-empty string values, and last duplicate key wins. Shared
   * with consumers that must not ship more than the local sink writes.
   */
  public static List<KeyValuePair> printableKeyValuePairs(ILoggingEvent event) {
    List<KeyValuePair> keyValuePairs = event.getKeyValuePairs();
    if (keyValuePairs == null || keyValuePairs.isEmpty()) {
      return List.of();
    }
    return handleDuplicates(keyValuePairs).stream()
        .filter(
            keyValuePair ->
                keyValuePair.key != null
                    && !keyValuePair.key.isBlank()
                    // Must run before coercion: String.valueOf(Optional.empty()) is non-empty.
                    && !Optional.empty().equals(keyValuePair.value))
        .map(keyValuePair -> new KeyValuePair(keyValuePair.key, toSafeObject(keyValuePair.value)))
        .filter(keyValuePair -> !String.valueOf(keyValuePair.value).isEmpty())
        .toList();
  }

  /**
   * Coerces a value to a JSON-safe type: {@code String}/{@code Number}/{@code Boolean} as-is, and
   * {@link String#valueOf} for anything else so no nested object representation leaks. Coercion
   * happens exactly once, inside {@link #printableKeyValuePairs}.
   */
  private static Object toSafeObject(@Nullable Object value) {
    // Return value as-is for primitive types.
    if (value instanceof String || value instanceof Number || value instanceof Boolean) {
      return value;
    }
    return String.valueOf(value);
  }

  // Returns a list of key value pairs with key value pairs removed when there is a subsequent
  // duplicate key.
  private static List<KeyValuePair> handleDuplicates(List<KeyValuePair> keyValuePairs) {
    Set<String> keysSeen = new HashSet<>();
    return keyValuePairs.reversed().stream().filter(e -> keysSeen.add(e.key)).toList().reversed();
  }
}
