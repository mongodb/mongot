package com.xgen.mongot.config.provider.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.xgen.mongot.util.bson.parser.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.json.JsonParseException;
import org.junit.Test;

public class CliOptionRegistryTest {

  private static final String JSON_ARRAY_VALUE =
      "[{\"name\": \"FOO\", \"value\": \"bar\"}, {\"name\": \"BAZ\", \"value\": \"qux\"}]";

  private static final Field.Optional<String> STRING_FIELD =
      Field.builder("string").stringField().optional().noDefault();
  private static final Field.Optional<String> ANOTHER_STRING_FIELD =
      Field.builder("anotherString").stringField().optional().noDefault();
  private static final Field.Optional<Path> PATH_FIELD =
      Field.builder("path")
          .classField(
              (context, value) -> Path.of(value.asString().getValue()),
              path -> new BsonString(path.toString()))
          .optional()
          .noDefault();
  private static final Field.Optional<Boolean> BOOLEAN_FIELD =
      Field.builder("boolean").booleanField().optional().noDefault();
  private static final Field.Optional<String> STRING_LIST_FIELD =
      Field.builder("stringList").stringField().optional().noDefault();
  private static final Field.Optional<String> JSON_ARRAY_FIELD =
      Field.builder("jsonArray").stringField().optional().noDefault();
  private static final Field.Optional<Integer> INTEGER_FIELD =
      Field.builder("integer").intField().optional().noDefault();
  private static final Field.Optional<Double> DOUBLE_FIELD =
      Field.builder("double").doubleField().optional().noDefault();

  /** The YAML block names of the child registries, used as the field for each child edge. */
  private static final Field.Optional<String> LEAF_VALUES_FIELD =
      Field.builder("leafValues").stringField().optional().noDefault();

  private static final Field.Optional<String> NESTED_FIELD =
      Field.builder("nested").stringField().optional().noDefault();
  private static final Field.Optional<String> NULL_OPTION_FIELD =
      Field.builder("nullOption").stringField().optional().noDefault();
  private static final Field.Optional<String> PRIMARY_FIELD =
      Field.builder("primary").stringField().optional().noDefault();
  private static final Field.Optional<String> SECONDARY_FIELD =
      Field.builder("secondary").stringField().optional().noDefault();

  /** Root registry: no leaves of its own, two child registries mounted on config fields. */
  private static class RootTestRegistry extends CliOptionRegistry {
    RootTestRegistry() {
      addChildRegistry(LEAF_VALUES_FIELD, new LeafOnlyTestRegistry());
      addChildRegistry(NESTED_FIELD, new NestedTestRegistry());
    }
  }

  /** Same leaf-only registry mounted under two distinct config fields. */
  private static class MultiMountedLeafTestRegistry extends CliOptionRegistry {

    MultiMountedLeafTestRegistry() {
      addChildRegistry(PRIMARY_FIELD, new LeafOnlyTestRegistry());
      addChildRegistry(SECONDARY_FIELD, new LeafOnlyTestRegistry());
    }
  }

  /** Registry with only leaf nodes, one for each {@code addXxxLeafValue} helper. */
  private static class LeafOnlyTestRegistry extends CliOptionRegistry {

    LeafOnlyTestRegistry() {
      addStringLeafValue(STRING_FIELD, () -> Optional.of("text"));
      addPathLeafValue(PATH_FIELD, () -> Optional.of(Path.of("/var/lib/mongot/data")));
      addBooleanLeafValue(BOOLEAN_FIELD, () -> Optional.of(true));
      addStringListLeafValue(STRING_LIST_FIELD, () -> List.of("alpha", "beta"));
      addJsonArrayLeafValue(JSON_ARRAY_FIELD, () -> Optional.of(JSON_ARRAY_VALUE));
      addIntegerLeafValue(INTEGER_FIELD, () -> Optional.of(42));
      addDoubleLeafValue(DOUBLE_FIELD, () -> Optional.of(3.14));
    }
  }

  /** Registry with both a child registry and leaf values of its own. */
  private static class NestedTestRegistry extends CliOptionRegistry {

    NestedTestRegistry() {
      addChildRegistry(NULL_OPTION_FIELD, new NullOptionTestRegistry());
      addStringLeafValue(STRING_FIELD, () -> Optional.of("nested-value"));
    }
  }

  /**
   * Leaf-only registry: one leaf option is empty (and so is omitted), and a second registered
   * string leaf makes a path three nodes deep visible in the output.
   */
  private static class NullOptionTestRegistry extends CliOptionRegistry {
    NullOptionTestRegistry() {
      addStringLeafValue(STRING_FIELD, () -> Optional.empty());
      addStringLeafValue(ANOTHER_STRING_FIELD, () -> Optional.of("deep-value"));
    }
  }

  /** Registry whose JSON array leaf is malformed, so traversal must fail. */
  private static class MalformedJsonArrayTestRegistry extends CliOptionRegistry {

    MalformedJsonArrayTestRegistry() {
      addJsonArrayLeafValue(JSON_ARRAY_FIELD, () -> Optional.of("not a valid JSON array: ["));
    }
  }

  private static Map<String, BsonValue> expectedCliValues() {
    return Map.of(
        "leafValues.string", new BsonString("text"),
        "leafValues.path", new BsonString("/var/lib/mongot/data"),
        "leafValues.boolean", BsonBoolean.TRUE,
        "leafValues.stringList",
            new BsonArray(List.of(new BsonString("alpha"), new BsonString("beta"))),
        "leafValues.jsonArray", BsonArray.parse(JSON_ARRAY_VALUE),
        "leafValues.integer", new BsonInt32(42),
        "leafValues.double", new BsonDouble(3.14),
        "nested.string", new BsonString("nested-value"),
        "nested.nullOption.anotherString", new BsonString("deep-value"));
  }

  @Test
  public void testTraverse_piecesTogetherFullYamlPaths() {
    Map<String, BsonValue> cliValues = new RootTestRegistry().traverse();

    assertEquals(expectedCliValues(), cliValues);
  }

  @Test
  public void testTraverse_omitsUnspecifiedLeafValues() {
    // nested.nullOption.string is registered with an empty option supplier, so it is omitted.
    Map<String, BsonValue> cliValues = new RootTestRegistry().traverse();

    assertEquals(expectedCliValues(), cliValues);
    assertNull(
        "nested.nullOption.string should be omitted since its option was not specified",
        cliValues.get("nested.nullOption.string"));
  }

  @Test
  public void testTraverse_isStableAcrossRepeatedCalls() {
    CliOptionRegistry registry = new RootTestRegistry();

    assertEquals(expectedCliValues(), registry.traverse());
    assertEquals(expectedCliValues(), registry.traverse());
  }

  @Test
  public void testTraverse_mountingSameRegistryUnderTwoFields_emitsEachFieldPath() {
    // The same leaf-only registry class is mounted under "primary" and "secondary", so every leaf
    // is emitted under each path. No node-internal path is involved: the emitted keys come
    // entirely from the field names of the child edges.
    Map<String, BsonValue> cliValues = new MultiMountedLeafTestRegistry().traverse();

    assertEquals(new BsonString("text"), cliValues.get("primary.string"));
    assertEquals(new BsonString("text"), cliValues.get("secondary.string"));
    assertEquals(new BsonInt32(42), cliValues.get("primary.integer"));
    assertEquals(new BsonInt32(42), cliValues.get("secondary.integer"));
    assertNull("leafValues.string should not be emitted", cliValues.get("leafValues.string"));
  }

  @Test
  public void testTraverse_nonRootRegistryEmitsRelativePaths() {
    // Traversing a subtree from its own root emits keys relative to that subtree, because the node
    // does not know where it is mounted. Only the root of the whole config contributes full paths.
    Map<String, BsonValue> cliValues = new LeafOnlyTestRegistry().traverse();

    assertEquals(new BsonString("text"), cliValues.get("string"));
    assertNull(
        "leafValues.string should not be emitted by a subtree traversal",
        cliValues.get("leafValues.string"));
  }

  @Test(expected = JsonParseException.class)
  public void testTraverse_illFormedJsonArray_throws() {
    new MalformedJsonArrayTestRegistry().traverse();
  }
}
