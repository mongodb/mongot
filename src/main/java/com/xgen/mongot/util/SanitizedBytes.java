package com.xgen.mongot.util;

import com.xgen.mongot.util.functionalinterfaces.CheckedConsumer;
import com.xgen.mongot.util.functionalinterfaces.CheckedFunction;
import java.util.Arrays;

/**
 * A wrapper for sensitive byte data that prevents accidental leakage through logging and toString
 * calls.
 *
 * <p>This class is meant to avoid common cases of accidental string serialization of sensitive
 * data. It should never be used to defend against malicious actors or prevent access to sensitive
 * data.
 *
 * <p>The raw bytes are only accessible via {@link #withBytes(CheckedConsumer)} or {@link
 * #mapBytes(CheckedFunction)}, which pass a defensive copy to the callback.
 *
 * <p>Note: The static factory {@link #wrapAndZeroInput(byte[])} zeros the caller's input array
 * after copying. This is done to encourage the caller does not retain the data outside the returned
 * instance.
 */
public final class SanitizedBytes {
  private static final String SANITIZED_PLACEHOLDER = "xxx-sanitized-xxx";

  private final byte[] data;

  private SanitizedBytes(byte[] data) {
    this.data = data;
  }

  /**
   * Creates a new {@link SanitizedBytes} from the given byte array. The input array is copied and
   * then zeroed to encourage the caller to ensure the only copy of the data lives inside the
   * returned instance.
   *
   * @param bytes the sensitive bytes to wrap
   * @return a new {@link SanitizedBytes} instance
   * @throws IllegalArgumentException if bytes is null
   */
  public static SanitizedBytes wrapAndZeroInput(byte[] bytes) {
    Check.argNotNull(bytes, "bytes");
    byte[] copy = Arrays.copyOf(bytes, bytes.length);
    Arrays.fill(bytes, (byte) 0);
    return new SanitizedBytes(copy);
  }

  /**
   * Provides access to the raw bytes via a callback. A defensive copy is passed to the consumer to
   * prevent mutation of internal state. The caller should not retain a reference to the byte array
   * beyond the scope of the callback.
   *
   * @param consumer the callback that receives the byte data
   * @param <E> the checked exception type the consumer may throw
   * @throws E if the consumer throws
   */
  public <E extends Throwable> void withBytes(CheckedConsumer<byte[], E> consumer) throws E {
    Check.argNotNull(consumer, "consumer");
    consumer.accept(Arrays.copyOf(this.data, this.data.length));
  }

  /**
   * Provides access to the raw bytes via a callback that returns a value. A defensive copy is
   * passed to the function to prevent mutation of internal state. The caller should not retain a
   * reference to the byte array beyond the scope of the callback.
   *
   * @param function the callback that receives the byte data and returns a result
   * @param <R> the return type
   * @param <E> the checked exception type the function may throw
   * @return the result of applying the function to the byte data
   * @throws E if the function throws
   */
  public <R, E extends Throwable> R mapBytes(CheckedFunction<byte[], R, E> function) throws E {
    Check.argNotNull(function, "function");
    return function.apply(Arrays.copyOf(this.data, this.data.length));
  }

  @Override
  public String toString() {
    // THIS SHOULD NEVER RETURN DATA DERIVED FROM THE BYTE ARRAY.
    return SANITIZED_PLACEHOLDER;
  }

  /**
   * Computes a hash code derived from the raw byte content via {@link Arrays#hashCode(byte[])}.
   * Note: this value is generated from a non-secure hash function and could potentially be
   * reversible. Its result should not be logged.
   *
   * @return the hash of the underlying byte array.
   */
  @Override
  public int hashCode() {
    return Arrays.hashCode(this.data);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SanitizedBytes other)) {
      return false;
    }
    return Arrays.equals(this.data, other.data);
  }
}
