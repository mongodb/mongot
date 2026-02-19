package com.xgen.testing.util;

/** Utility class for performing operations and conversions on byte arrays. */
public class ByteArrayUtil {
  /**
   * Converts an array of shorts to an array of bytes.
   *
   * <p>Primary Use Case: This method allows defining byte arrays using hex literals (e.g., 0x80,
   * 0xFF) in a {@code short[]} array to avoid explicit casting. In Java, byte literals > 0x7F
   * require a manual cast to (byte) because bytes are signed.
   */
  public static byte[] toByteArray(short[] data) {
    byte[] byteData = new byte[data.length];
    for (int i = 0; i < data.length; i++) {
      byteData[i] = (byte) data[i];
    }
    return byteData;
  }
}
