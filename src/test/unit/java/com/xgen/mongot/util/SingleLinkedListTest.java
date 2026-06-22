package com.xgen.mongot.util;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.junit.Test;

public class SingleLinkedListTest {

  @Test
  public void testSize() {
    SingleLinkedList<Integer> list =
        SingleLinkedList.<Integer>empty().prepend(1).prepend(2).prepend(3);
    assertEquals(3, list.size());
    assertFalse(list.isEmpty());
  }

  @Test
  public void testEmptySize() {
    SingleLinkedList<Integer> list = SingleLinkedList.empty();
    assertEquals(0, list.size());
    assertTrue(list.isEmpty());
  }

  @Test
  @SuppressWarnings("SimplifyStreamApiCallChains")
  public void testStream() {
    SingleLinkedList<Integer> list =
        SingleLinkedList.<Integer>empty().prepend(1).prepend(2).prepend(3);
    assertEquals(List.of(3, 2, 1), list.stream().collect(toList()));
  }

  @Test
  @SuppressWarnings("SimplifyStreamApiCallChains")
  public void testEmptyStream() {
    SingleLinkedList<Integer> list = SingleLinkedList.empty();
    assertEquals(List.of(), list.stream().collect(toList()));
  }

  @Test
  public void testFoldLeft() {
    SingleLinkedList<Integer> list =
        SingleLinkedList.<Integer>empty().prepend(1).prepend(2).prepend(3);
    String result = list.foldLeft(":", (s, e) -> s + e);
    assertEquals(":321", result);
  }

  @Test
  public void testFoldRight() {
    SingleLinkedList<Integer> list =
        SingleLinkedList.<Integer>empty().prepend(1).prepend(2).prepend(3);
    String result = list.foldRight(":", (s, e) -> s + e);
    assertEquals(":123", result);
  }

  @Test
  public void testEmptyFoldLeft() {
    SingleLinkedList<Integer> list = SingleLinkedList.empty();
    String result = list.foldLeft("", (s, e) -> s + e);
    assertEquals("", result);
  }

  @Test
  public void testEmptyFoldRight() {
    SingleLinkedList<Integer> list = SingleLinkedList.empty();
    String result = list.foldRight("", (s, e) -> s + e);
    assertEquals("", result);
  }
}
