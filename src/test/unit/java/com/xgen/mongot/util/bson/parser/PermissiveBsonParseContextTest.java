package com.xgen.mongot.util.bson.parser;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.DeploymentEnvironment;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;

public class PermissiveBsonParseContextTest {

  @Test
  public void testRootWithDeploymentEnvironment() {
    PermissiveBsonParseContext root =
        PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.ENTERPRISE));

    assertThat(root.getDeploymentEnvironment()).hasValue(DeploymentEnvironment.ENTERPRISE);
  }

  @Test
  public void testDeploymentEnvironmentPropagatesToChildren() {
    PermissiveBsonParseContext child =
        PermissiveBsonParseContext.root(Optional.of(DeploymentEnvironment.COMMUNITY))
            .child("foo")
            .arrayElement(0)
            .child("bar");

    assertThat(child.getDeploymentEnvironment()).hasValue(DeploymentEnvironment.COMMUNITY);
  }

  @Test
  public void testIgnoresUnknownFields() throws BsonParseException {
    BsonDocument bsonDocument = new BsonDocument("foo", new BsonString("bar"));

    PermissiveBsonParseContext permissiveContext = PermissiveBsonParseContext.root();
    BsonDocumentParser.withContext(permissiveContext, bsonDocument).build().close();

    assertThat(permissiveContext.getUnknownFieldExceptions()).hasSize(1);
    assertThat(permissiveContext.getUnknownFieldExceptions().getFirst().getMessage())
        .contains("foo");
  }

  @Test
  public void testRoot() {
    PermissiveBsonParseContext root = PermissiveBsonParseContext.root();

    BsonParseException ex =
        assertThrows(
            BsonParseException.class,
            () -> root.handleSemanticError("Field path should not be present"));

    assertThat(ex).hasMessageThat().isEqualTo("Field path should not be present");
  }

  @Test
  public void testChild() {
    PermissiveBsonParseContext child =
        PermissiveBsonParseContext.root().child("foo").child("bar").child("dotted.name");

    BsonParseException ex =
        assertThrows(BsonParseException.class, () -> child.handleOverflow("long"));

    assertThat(ex)
        .hasMessageThat()
        .isEqualTo("\"foo.bar.dotted.name\" overflowed, must fit in a long");
  }

  @Test
  public void testChildArray() {
    PermissiveBsonParseContext childArray =
        PermissiveBsonParseContext.root()
            .child("arrayField")
            .arrayElement(5)
            .arrayElement(0)
            .child("bar");

    BsonParseException ex =
        assertThrows(BsonParseException.class, () -> childArray.handleUnderflow("int"));

    assertThat(ex)
        .hasMessageThat()
        .isEqualTo("\"arrayField[5][0].bar\" underflowed, must fit in a int");
  }

  @Test
  public void testRootArray() {
    PermissiveBsonParseContext childArray =
        PermissiveBsonParseContext.root().arrayElement(5).arrayElement(0).child("bar");

    BsonParseException ex =
        assertThrows(BsonParseException.class, () -> childArray.handleUnderflow("int"));

    assertThat(ex).hasMessageThat().isEqualTo("\"[5][0].bar\" underflowed, must fit in a int");
  }
}
