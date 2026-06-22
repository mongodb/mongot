package com.xgen.mongot.index.lucene.abortable;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;

/** Unit tests for {@link AbortableDirectory}. */
public class AbortableDirectoryTest {

  /**
   * Test that AbortableDirectory throws IOException when merge is aborted during write operations.
   */
  @Test
  public void testAbortDuringWrite() throws IOException {
    // Create a mock merge that will be aborted after the first check
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(true);

    // Create an AbortableDirectory wrapping an in-memory directory
    Directory baseDir = new ByteBuffersDirectory();
    AbortableDirectory abortableDir = new AbortableDirectory(baseDir, mockMerge);

    // Create an output file
    IndexOutput output = abortableDir.createOutput("test.dat", IOContext.DEFAULT);

    // Write data to trigger abort check (after 1 MB)
    // We need to write CHECK_ABORT_INTERVAL_BYTES to trigger the check
    long bytesToWrite = AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES;
    byte[] largeData = new byte[(int) bytesToWrite];

    // This should throw IOException because merge is aborted
    IOException exception =
        assertThrows(IOException.class, () -> output.writeBytes(largeData, 0, largeData.length));

    assertTrue(
        "Exception message should mention merge abortion",
        exception.getMessage().contains("Merge aborted via IO-level interruption"));

    output.close();
    abortableDir.close();
  }

  /** Test that AbortableDirectory allows normal writes when merge is not aborted. */
  @Test
  public void testNormalWriteWhenNotAborted() throws IOException {
    // Create a mock merge that is never aborted
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(false);

    // Create an AbortableDirectory wrapping an in-memory directory
    Directory baseDir = new ByteBuffersDirectory();
    AbortableDirectory abortableDir = new AbortableDirectory(baseDir, mockMerge);

    // Create an output file
    IndexOutput output = abortableDir.createOutput("test.dat", IOContext.DEFAULT);

    // Write data larger than CHECK_ABORT_INTERVAL_BYTES
    long bytesToWrite = AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES * 2;
    byte[] largeData = new byte[(int) bytesToWrite];
    for (int i = 0; i < largeData.length; i++) {
      largeData[i] = (byte) (i % 256);
    }

    // This should succeed because merge is not aborted
    output.writeBytes(largeData, 0, largeData.length);

    output.close();

    // Verify the file was written correctly
    assertEquals(bytesToWrite, baseDir.fileLength("test.dat"));

    abortableDir.close();
  }

  /** Test that AbortableDirectory checks abort status periodically based on bytes written. */
  @Test
  public void testPeriodicAbortCheck() throws IOException {
    // Create a mock merge that becomes aborted after the second check
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    // Return false for the first check, then true for subsequent checks
    when(mockMerge.isAborted()).thenReturn(false).thenReturn(true);

    // Create an AbortableDirectory wrapping an in-memory directory
    Directory baseDir = new ByteBuffersDirectory();
    AbortableDirectory abortableDir = new AbortableDirectory(baseDir, mockMerge);

    // Create an output file
    IndexOutput output = abortableDir.createOutput("test.dat", IOContext.DEFAULT);

    // Write a large chunk that will trigger the first abort check (should succeed)
    byte[] largeData1 = new byte[(int) AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES];
    output.writeBytes(largeData1, 0, largeData1.length);

    // Now write another large chunk that will trigger the second abort check (should fail)
    byte[] largeData2 = new byte[(int) AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES];

    // This should throw IOException because merge is now aborted
    IOException exception =
        assertThrows(IOException.class, () -> output.writeBytes(largeData2, 0, largeData2.length));

    assertTrue(
        "Exception message should mention merge abortion",
        exception.getMessage().contains("Merge aborted via IO-level interruption"));

    output.close();
    abortableDir.close();
  }

  /** Test that AbortableDirectory works with createTempOutput as well. */
  @Test
  public void testAbortDuringTempOutput() throws IOException {
    // Create a mock merge that will be aborted
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(true);

    // Create an AbortableDirectory wrapping an in-memory directory
    Directory baseDir = new ByteBuffersDirectory();
    AbortableDirectory abortableDir = new AbortableDirectory(baseDir, mockMerge);

    // Create a temp output file
    IndexOutput output = abortableDir.createTempOutput("test", ".tmp", IOContext.DEFAULT);

    // Write data to trigger abort check
    byte[] largeData = new byte[(int) AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES];

    // This should throw IOException because merge is aborted
    IOException exception =
        assertThrows(IOException.class, () -> output.writeBytes(largeData, 0, largeData.length));

    assertTrue(
        "Exception message should mention merge abortion",
        exception.getMessage().contains("Merge aborted via IO-level interruption"));

    output.close();
    abortableDir.close();
  }

  /**
   * Test that different write methods (writeByte, writeInt, writeLong, etc.) all check for abort.
   */
  @Test
  public void testAbortCheckOnDifferentWriteMethods() throws IOException {
    // Create a mock merge that will be aborted
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(false);

    // Create an AbortableDirectory wrapping an in-memory directory
    Directory baseDir = new ByteBuffersDirectory();
    AbortableDirectory abortableDir = new AbortableDirectory(baseDir, mockMerge);

    // Create an output file
    IndexOutput output = abortableDir.createOutput("test.dat", IOContext.DEFAULT);

    // Test various write methods
    output.writeByte((byte) 1);
    output.writeShort((short) 2);
    output.writeInt(3);
    output.writeLong(4L);

    // Write enough data to trigger abort check
    byte[] data = new byte[(int) AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES];
    output.writeBytes(data, 0, data.length);

    output.close();

    // Verify the file was written
    long expectedSize =
        1
            + Short.BYTES
            + Integer.BYTES
            + Long.BYTES
            + AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES;
    assertEquals(expectedSize, baseDir.fileLength("test.dat"));

    abortableDir.close();
  }

  /** Test that AbortableDirectory properly delegates getFilePointer and getChecksum. */
  @Test
  public void testDelegationMethods() throws IOException {
    // Create a mock merge that is never aborted
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(false);

    // Create an AbortableDirectory wrapping an in-memory directory
    Directory baseDir = new ByteBuffersDirectory();
    AbortableDirectory abortableDir = new AbortableDirectory(baseDir, mockMerge);

    // Create an output file
    IndexOutput output = abortableDir.createOutput("test.dat", IOContext.DEFAULT);

    // Test getFilePointer
    assertEquals(0, output.getFilePointer());

    // Write some data
    byte[] data = new byte[100];
    output.writeBytes(data, 0, data.length);

    // Test getFilePointer after write
    assertEquals(100, output.getFilePointer());

    // Test getChecksum (should not throw)
    long checksum = output.getChecksum();
    assertTrue("Checksum should be non-negative", checksum >= 0);

    output.close();
    abortableDir.close();
  }

  /** Test that AbortableDirectory wraps IndexInput for .vec files with abort detection. */
  @Test
  public void testAbortDuringReadVecFile() throws IOException {
    // Create a mock merge that will be aborted
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(true);

    // Create an in-memory directory and write a .vec file
    Directory baseDir = new ByteBuffersDirectory();
    try (IndexOutput output = baseDir.createOutput("test.vec", IOContext.DEFAULT)) {
      byte[] data = new byte[(int) AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES];
      output.writeBytes(data, 0, data.length);
    }

    AbortableDirectory abortableDir = new AbortableDirectory(baseDir, mockMerge);
    IndexInput input = abortableDir.openInput("test.vec", IOContext.DEFAULT);
    byte[] readBuffer = new byte[(int) AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES];
    IOException exception =
        assertThrows(IOException.class, () -> input.readBytes(readBuffer, 0, readBuffer.length));
    assertTrue(
        "Exception message should mention merge abortion",
        exception.getMessage().contains("Merge aborted via IO-level interruption"));
    input.close();
    abortableDir.close();
  }

  /** Test that AbortableDirectory wraps IndexInput for .vex files with abort detection. */
  @Test
  public void testAbortDuringReadVexFile() throws IOException {
    // Create a mock merge that will be aborted
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(true);

    // Create an in-memory directory and write a .vex file
    Directory baseDir = new ByteBuffersDirectory();
    try (IndexOutput output = baseDir.createOutput("test.vex", IOContext.DEFAULT)) {
      byte[] data = new byte[(int) AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES];
      output.writeBytes(data, 0, data.length);
    }

    AbortableDirectory abortableDir = new AbortableDirectory(baseDir, mockMerge);
    IndexInput input = abortableDir.openInput("test.vex", IOContext.DEFAULT);
    byte[] readBuffer = new byte[(int) AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES];
    IOException exception =
        assertThrows(IOException.class, () -> input.readBytes(readBuffer, 0, readBuffer.length));
    assertTrue(
        "Exception message should mention merge abortion",
        exception.getMessage().contains("Merge aborted via IO-level interruption"));

    input.close();
    abortableDir.close();
  }

  /** Test that AbortableDirectory does NOT wrap IndexInput for non-vector files. */
  @Test
  public void testNoAbortCheckForNonVectorFiles() throws IOException {
    // Create a mock merge that is aborted
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(true);

    // Create an in-memory directory and write a .dat file (non-vector)
    Directory baseDir = new ByteBuffersDirectory();
    byte[] testData = new byte[(int) AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES];
    for (int i = 0; i < testData.length; i++) {
      testData[i] = (byte) (i % 256);
    }
    try (IndexOutput output = baseDir.createOutput("test.dat", IOContext.DEFAULT)) {
      output.writeBytes(testData, 0, testData.length);
    }

    // Create an AbortableDirectory wrapping the base directory
    AbortableDirectory abortableDir = new AbortableDirectory(baseDir, mockMerge);

    // Open the .dat file for reading - should NOT be wrapped
    IndexInput input = abortableDir.openInput("test.dat", IOContext.DEFAULT);

    // Read data - should NOT throw even though merge is aborted (non-vector file)
    byte[] readBuffer = new byte[(int) AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES];
    input.readBytes(readBuffer, 0, readBuffer.length);

    // Verify data was read correctly
    assertArrayEquals(testData, readBuffer);

    input.close();
    abortableDir.close();
  }

  /** Test that AbortableDirectory allows normal reads when merge is not aborted. */
  @Test
  public void testNormalReadWhenNotAborted() throws IOException {
    // Create a mock merge that is never aborted
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(false);

    // Create an in-memory directory and write a .vec file
    Directory baseDir = new ByteBuffersDirectory();
    byte[] testData = new byte[(int) (AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES * 2)];
    for (int i = 0; i < testData.length; i++) {
      testData[i] = (byte) (i % 256);
    }
    try (IndexOutput output = baseDir.createOutput("test.vec", IOContext.DEFAULT)) {
      output.writeBytes(testData, 0, testData.length);
    }

    // Create an AbortableDirectory wrapping the base directory
    AbortableDirectory abortableDir = new AbortableDirectory(baseDir, mockMerge);

    // Open the .vec file for reading
    IndexInput input = abortableDir.openInput("test.vec", IOContext.DEFAULT);

    // Read data - should succeed because merge is not aborted
    byte[] readBuffer = new byte[testData.length];
    input.readBytes(readBuffer, 0, readBuffer.length);

    // Verify data was read correctly
    assertArrayEquals(testData, readBuffer);

    input.close();
    abortableDir.close();
  }

  /** Test that AbortableIndexInput checks abort status periodically based on bytes read. */
  @Test
  public void testPeriodicAbortCheckOnRead() throws IOException {
    // Create a mock merge that becomes aborted after the second check
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(false).thenReturn(true);

    // Create an in-memory directory and write a .vec file
    Directory baseDir = new ByteBuffersDirectory();
    byte[] testData = new byte[(int) (AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES * 3)];
    try (IndexOutput output = baseDir.createOutput("test.vec", IOContext.DEFAULT)) {
      output.writeBytes(testData, 0, testData.length);
    }

    // Create an AbortableDirectory wrapping the base directory
    AbortableDirectory abortableDir = new AbortableDirectory(baseDir, mockMerge);

    // Open the .vec file for reading
    IndexInput input = abortableDir.openInput("test.vec", IOContext.DEFAULT);

    // First read should succeed (first abort check returns false)
    byte[] readBuffer1 = new byte[(int) AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES];
    input.readBytes(readBuffer1, 0, readBuffer1.length);

    // Second read should fail (second abort check returns true)
    byte[] readBuffer2 = new byte[(int) AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES];
    IOException exception =
        assertThrows(IOException.class, () -> input.readBytes(readBuffer2, 0, readBuffer2.length));

    assertTrue(
        "Exception message should mention merge abortion",
        exception.getMessage().contains("Merge aborted via IO-level interruption"));

    input.close();
    abortableDir.close();
  }

  /** Test that AbortableIndexInput properly delegates getFilePointer, seek, and length. */
  @Test
  public void testIndexInputDelegationMethods() throws IOException {
    // Create a mock merge that is never aborted
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(false);

    // Create an in-memory directory and write a .vec file
    Directory baseDir = new ByteBuffersDirectory();
    byte[] testData = new byte[1000];
    for (int i = 0; i < testData.length; i++) {
      testData[i] = (byte) (i % 256);
    }
    try (IndexOutput output = baseDir.createOutput("test.vec", IOContext.DEFAULT)) {
      output.writeBytes(testData, 0, testData.length);
    }

    // Create an AbortableDirectory wrapping the base directory
    AbortableDirectory abortableDir = new AbortableDirectory(baseDir, mockMerge);

    // Open the .vec file for reading
    IndexInput input = abortableDir.openInput("test.vec", IOContext.DEFAULT);

    // Test length
    assertEquals(1000, input.length());

    // Test getFilePointer (initial position)
    assertEquals(0, input.getFilePointer());

    // Read some bytes
    byte[] buffer = new byte[100];
    input.readBytes(buffer, 0, buffer.length);

    // Test getFilePointer after read
    assertEquals(100, input.getFilePointer());

    // Test seek
    input.seek(500);
    assertEquals(500, input.getFilePointer());

    // Read after seek
    input.readBytes(buffer, 0, buffer.length);
    assertEquals(600, input.getFilePointer());

    input.close();
    abortableDir.close();
  }

  /** Test that AbortableIndexInput clone() preserves abort detection. */
  @Test
  public void testIndexInputClone() throws IOException {
    // Create a mock merge that will be aborted
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(true);

    // Create an in-memory directory and write a .vec file
    Directory baseDir = new ByteBuffersDirectory();
    byte[] testData = new byte[(int) AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES];
    try (IndexOutput output = baseDir.createOutput("test.vec", IOContext.DEFAULT)) {
      output.writeBytes(testData, 0, testData.length);
    }

    // Create an AbortableDirectory wrapping the base directory
    AbortableDirectory abortableDir = new AbortableDirectory(baseDir, mockMerge);

    // Open the .vec file for reading
    IndexInput input = abortableDir.openInput("test.vec", IOContext.DEFAULT);

    // Clone the input
    IndexInput clonedInput = input.clone();
    assertNotNull(clonedInput);

    // Read from cloned input should also trigger abort check
    byte[] readBuffer = new byte[(int) AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES];
    IOException exception =
        assertThrows(
            IOException.class, () -> clonedInput.readBytes(readBuffer, 0, readBuffer.length));

    assertTrue(
        "Exception message should mention merge abortion",
        exception.getMessage().contains("Merge aborted via IO-level interruption"));

    clonedInput.close();
    input.close();
    abortableDir.close();
  }

  /** Test that AbortableIndexInput slice() preserves abort detection. */
  @Test
  public void testIndexInputSlice() throws IOException {
    // Create a mock merge that will be aborted
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(true);

    // Create an in-memory directory and write a .vec file
    Directory baseDir = new ByteBuffersDirectory();
    byte[] testData = new byte[(int) (AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES * 2)];
    try (IndexOutput output = baseDir.createOutput("test.vec", IOContext.DEFAULT)) {
      output.writeBytes(testData, 0, testData.length);
    }

    // Create an AbortableDirectory wrapping the base directory
    AbortableDirectory abortableDir = new AbortableDirectory(baseDir, mockMerge);

    // Open the .vec file for reading
    IndexInput input = abortableDir.openInput("test.vec", IOContext.DEFAULT);

    // Create a slice
    IndexInput slicedInput =
        input.slice("test-slice", 0, AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES);
    assertNotNull(slicedInput);

    // Read from sliced input should also trigger abort check
    byte[] readBuffer = new byte[(int) AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES];
    IOException exception =
        assertThrows(
            IOException.class, () -> slicedInput.readBytes(readBuffer, 0, readBuffer.length));

    assertTrue(
        "Exception message should mention merge abortion",
        exception.getMessage().contains("Merge aborted via IO-level interruption"));

    slicedInput.close();
    input.close();
    abortableDir.close();
  }

  /** Test that readByte() also triggers abort check after enough bytes are read. */
  @Test
  public void testAbortCheckOnReadByte() throws IOException {
    // Create a mock merge that will be aborted
    MergePolicy.OneMerge mockMerge = mock(MergePolicy.OneMerge.class);
    when(mockMerge.isAborted()).thenReturn(true);

    // Create an in-memory directory and write a .vec file
    Directory baseDir = new ByteBuffersDirectory();
    byte[] testData = new byte[(int) AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES + 1];
    try (IndexOutput output = baseDir.createOutput("test.vec", IOContext.DEFAULT)) {
      output.writeBytes(testData, 0, testData.length);
    }

    // Create an AbortableDirectory wrapping the base directory
    AbortableDirectory abortableDir = new AbortableDirectory(baseDir, mockMerge);

    // Open the .vec file for reading
    IndexInput input = abortableDir.openInput("test.vec", IOContext.DEFAULT);

    // Read bytes one at a time until we hit the threshold
    // This should eventually throw when we cross CHECK_ABORT_INTERVAL_BYTES
    IOException exception =
        assertThrows(
            IOException.class,
            () -> {
              for (int i = 0; i <= AbortableDirectory.CHECK_ABORT_INTERVAL_BYTES; i++) {
                input.readByte();
              }
            });

    assertTrue(
        "Exception message should mention merge abortion",
        exception.getMessage().contains("Merge aborted via IO-level interruption"));

    input.close();
    abortableDir.close();
  }
}
