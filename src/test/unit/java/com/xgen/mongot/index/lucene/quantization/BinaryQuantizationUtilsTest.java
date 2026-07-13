package com.xgen.mongot.index.lucene.quantization;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.primitives.Floats;
import com.xgen.testing.TestUtils;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.Test;

public class BinaryQuantizationUtilsTest {

  @Test
  public void requiredBytes() {
    assertEquals(0, BinaryQuantizationUtils.requiredBytes(0));
    assertEquals(1, BinaryQuantizationUtils.requiredBytes(1));
    assertEquals(1, BinaryQuantizationUtils.requiredBytes(7));
    assertEquals(1, BinaryQuantizationUtils.requiredBytes(8));
    assertEquals(1024, BinaryQuantizationUtils.requiredBytes(8190));
    assertEquals(1024, BinaryQuantizationUtils.requiredBytes(8192));
  }

  @Test
  public void compressBytes_singleByte_packsLeftToRight() {
    byte[] input = {0, 1, 0, 0, 1, 1, 0, 1};
    var output = new byte[1];

    BinaryQuantizationUtils.compressBytes(input, output);

    assertArrayEquals(new byte[] {0, 1, 0, 0, 1, 1, 0, 1}, input);
    assertArrayEquals(new byte[] {0x4D}, output);
  }

  @Test
  public void compressBytes_withPadding_packsLeftToRight() {
    byte[] input = {0, 1, 0, 0, 1, 1, 0, 1, 0, 1};
    var output = new byte[2];

    BinaryQuantizationUtils.compressBytes(input, output);

    assertArrayEquals(new byte[] {0, 1, 0, 0, 1, 1, 0, 1, 0, 1}, input);
    assertArrayEquals(new byte[] {0x4D, 0x40}, output);
  }

  @Test
  public void compressBytes_outputTooSmall_throwsException() {
    byte[] input = {0, 1, 0, 0, 1, 1, 0, 1, 1};
    var output = new byte[1];

    assertThrows(
        IllegalArgumentException.class, () -> BinaryQuantizationUtils.compressBytes(input, output));
  }

  @Test
  public void dequantize_unrolled_8dims() {
    int dimensions = 8;
    byte[] compressedVector = {(byte) 0b1101_0100};
    float[] dequantized = new float[dimensions];

    BinaryQuantizationUtils.dequantize(compressedVector, dequantized);

    float[] expected = {1f, 1f, -1f, 1f, -1f, 1f, -1f, -1f};
    assertArrayEquals(expected, dequantized, TestUtils.EPSILON);
  }

  @Test
  public void dequantize_unrolled_allOnes() {
    int numBytes = 512;
    byte[] compressedVector = new byte[numBytes];
    Arrays.fill(compressedVector, (byte) 0xFF);
    float[] dequantized = new float[numBytes * 8];

    BinaryQuantizationUtils.dequantize(compressedVector, dequantized);

    HashSet<Float> result = new HashSet<>(Floats.asList(dequantized));
    assertThat(result).containsExactly(1f);
  }

  @Test
  public void dequantize_unrolled_allZeros() {
    int numBytes = 512;
    byte[] compressedVector = new byte[numBytes];
    Arrays.fill(compressedVector, (byte) 0x00);
    float[] dequantized = new float[numBytes * 8];

    BinaryQuantizationUtils.dequantize(compressedVector, dequantized);

    HashSet<Float> result = new HashSet<>(Floats.asList(dequantized));
    assertThat(result).containsExactly(-1f);
  }

  @Test
  public void dequantize_unrolled_asymmetricNibbles() {
    int numBytes = 512;
    byte[] compressedVector = new byte[numBytes];
    Arrays.fill(compressedVector, (byte) 0b0101_0111);
    float[] dequantized = new float[numBytes * 8];

    BinaryQuantizationUtils.dequantize(compressedVector, dequantized);

    float[] expected = new float[dequantized.length];
    for (int i = 0; i < expected.length; i += 8) {
      expected[i] = -1f;
      expected[i + 1] = 1f;
      expected[i + 2] = -1f;
      expected[i + 3] = 1f;
      expected[i + 4] = -1f;
      expected[i + 5] = 1f;
      expected[i + 6] = 1f;
      expected[i + 7] = 1f;
    }
    assertArrayEquals(expected, dequantized, TestUtils.EPSILON);
  }

  @Test
  public void dequantize_tail_decodesLeftToRight() {
    byte[] compressedVector = {(byte) 0b0000_1111};
    float[] dequantized = new float[7];

    BinaryQuantizationUtils.dequantize(compressedVector, dequantized);

    float[] expected = {-1f, -1f, -1f, -1f, 1f, 1f, 1f};
    assertArrayEquals(expected, dequantized, TestUtils.EPSILON);
  }

  @Test
  public void dequantize_bytes_unrolled_8dims() {
    int dimensions = 8;
    byte[] compressedVector = {(byte) 0b1101_0100};
    byte[] dequantized = new byte[dimensions];

    BinaryQuantizationUtils.dequantize(compressedVector, dequantized);

    byte[] expected = {1, 1, -1, 1, -1, 1, -1, -1};
    assertArrayEquals(expected, dequantized);
  }

  @Test
  public void dequantize_bytes_tail_decodesLeftToRight() {
    byte[] compressedVector = {(byte) 0b0000_1111};
    byte[] dequantized = new byte[7];

    BinaryQuantizationUtils.dequantize(compressedVector, dequantized);

    byte[] expected = {-1, -1, -1, -1, 1, 1, 1};
    assertArrayEquals(expected, dequantized);
  }
}
