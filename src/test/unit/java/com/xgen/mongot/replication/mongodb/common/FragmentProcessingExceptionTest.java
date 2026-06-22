package com.xgen.mongot.replication.mongodb.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class FragmentProcessingExceptionTest {

  @Test
  public void testConstructor() {
    FragmentProcessingException exception =
        new FragmentProcessingException(
            "Out-of-order fragment: expected fragment " + "2 but received fragment 4");
    assertEquals(
        "Out-of-order fragment: expected fragment " + "2 but received fragment 4",
        exception.getMessage());
  }

  @Test
  public void testEmptyMessage() {
    FragmentProcessingException exception = new FragmentProcessingException("");
    assertEquals("", exception.getMessage());
  }

  @Test
  public void testNullMessage() {
    FragmentProcessingException exception = new FragmentProcessingException(null);
    assertNull(exception.getMessage());
  }
}
