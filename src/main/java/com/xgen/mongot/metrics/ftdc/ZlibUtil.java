package com.xgen.mongot.metrics.ftdc;

import java.io.ByteArrayOutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/** Utility to facilitate (de)compression with Zlib. */
class ZlibUtil {
  private static final int BUFFER_SIZE = 1024;

  static byte[] zlibCompress(byte[] in) {
    Deflater deflater = new Deflater();
    try {
      deflater.setInput(in);
      deflater.finish();

      ByteArrayOutputStream outputStream = new ByteArrayOutputStream(in.length / 2);
      byte[] buffer = new byte[BUFFER_SIZE];

      while (!deflater.finished()) {
        int count = deflater.deflate(buffer);
        outputStream.write(buffer, 0, count);
      }
      // outputStream is in memory, no need to close
      return outputStream.toByteArray();
    } finally {
      deflater.end(); // Free native memory
    }
  }

  static byte[] zlibDecompress(byte[] in) throws DataFormatException {
    Inflater inflater = new Inflater();
    inflater.setInput(in);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(in.length);

    byte[] buffer = new byte[BUFFER_SIZE];
    while (!inflater.finished()) {
      int count = inflater.inflate(buffer);
      outputStream.write(buffer, 0, count);
    }

    // outputStream is in memory, no need to close
    return outputStream.toByteArray();
  }
}
