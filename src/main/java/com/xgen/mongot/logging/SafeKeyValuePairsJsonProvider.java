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
   *   <li>If the key has been overridden by a subsequent key value pair
   * </ul>
   *
   * @param generator the JsonGenerator to write to
   * @param event the logging event containing the key-value pairs
   */
  @Override
  public void writeTo(JsonGenerator generator, ILoggingEvent event) throws IOException {
    List<KeyValuePair> keyValuePairs = event.getKeyValuePairs();
    if (keyValuePairs == null || keyValuePairs.isEmpty()) {
      return;
    }

    List<KeyValuePair> keyValuePairsWithoutDuplicates = handleDuplicates(keyValuePairs);

    for (KeyValuePair keyValuePair : keyValuePairsWithoutDuplicates) {
      if (keyValuePair.key != null
          && !keyValuePair.key.isBlank()
          && shouldPrintValue(keyValuePair.value)) {
        generator.writeFieldName(keyValuePair.key);
        generator.writeObject(toSafeObject(keyValuePair.value));
      }
    }
  }

  // Returns a list of key value pairs with key value pairs removed when there is a subsequent
  // duplicate key.
  private List<KeyValuePair> handleDuplicates(List<KeyValuePair> keyValuePairs) {
    Set<String> keysSeen = new HashSet<>();
    return keyValuePairs.reversed().stream().filter(e -> keysSeen.add(e.key)).toList().reversed();
  }

  private boolean shouldPrintValue(Object value) {
    return !Optional.empty().equals(value);
  }

  private Object toSafeObject(@Nullable Object value) {
    // Return value as-is for primitive types.
    if (value instanceof String || value instanceof Number || value instanceof Boolean) {
      return value;
    }
    return String.valueOf(value);
  }
}
