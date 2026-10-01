package com.xgen.mongot.util.bson.parser;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class BsonParseContextTest {

  @Test
  public void testRoot() {
    BsonParseContext root = BsonParseContext.root();

    BsonParseException ex =
        assertThrows(
            BsonParseException.class,
            () -> root.handleSemanticError("Field path should not be present"));

    assertThat(ex).hasMessageThat().isEqualTo("Field path should not be present");
  }

  @Test
  public void testChild() {
    BsonParseContext child = BsonParseContext.root().child("foo").child("bar").child("dotted.name");

    BsonParseException ex =
        assertThrows(BsonParseException.class, () -> child.handleOverflow("long"));

    assertThat(ex)
        .hasMessageThat()
        .isEqualTo("\"foo.bar.dotted.name\" overflowed, must fit in a long");
  }

  @Test
  public void testChildArray() {
    BsonParseContext childArray =
        BsonParseContext.root().child("arrayField").arrayElement(5).arrayElement(0).child("bar");

    BsonParseException ex =
        assertThrows(BsonParseException.class, () -> childArray.handleUnderflow("int"));

    assertThat(ex)
        .hasMessageThat()
        .isEqualTo("\"arrayField[5][0].bar\" underflowed, must fit in a int");
  }

  @Test
  public void testRootArray() {
    BsonParseContext childArray =
        BsonParseContext.root().arrayElement(5).arrayElement(0).child("bar");

    BsonParseException ex =
        assertThrows(BsonParseException.class, () -> childArray.handleUnderflow("int"));

    assertThat(ex).hasMessageThat().isEqualTo("\"[5][0].bar\" underflowed, must fit in a int");
  }
}
