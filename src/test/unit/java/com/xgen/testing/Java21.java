package com.xgen.testing;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Assert;
import org.junit.Test;

public class Java21 {

  record StringRecord(String data) {}

  @Test
  public void testRecord() {
    // Records are new in Java 21
    StringRecord record = new StringRecord("a");

    Assert.assertEquals("a", record.data);
    Assert.assertEquals("a", record.data());
  }

  @Test
  public void testUnicode150() {
    // Kawi is new in Unicode 15.0
    // https://www.unicode.org/versions/Unicode15.0.0/#Character_Additions
    Assert.assertEquals(Character.UnicodeBlock.KAWI, Character.UnicodeBlock.of(73472));
  }

  @Test
  public void testJavaRuntimeVersion() {
    Runtime.Version expected = Runtime.Version.parse("21.0.3");
    assertThat(Runtime.version()).isAtLeast(expected);
  }
}
