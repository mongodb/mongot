package com.xgen.mongot.index.definition;

import static com.xgen.mongot.index.definition.StoredSourceDefinition.Mode.EXCLUSION;
import static com.xgen.mongot.index.definition.StoredSourceDefinition.Mode.INCLUSION;
import static com.xgen.testing.BsonDeserializationTestSuite.fromValue;
import static com.xgen.testing.BsonSerializationTestSuite.load;

import com.google.common.testing.EqualsTester;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      StoredSourceDefinitionTest.DeserializationTest.class,
      StoredSourceDefinitionTest.SerializationTest.class,
      StoredSourceDefinitionTest.DefinitionTest.class
    })
public class StoredSourceDefinitionTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {

    private static final String SUITE_NAME = "storedSource-deserialization";
    private static final BsonDeserializationTestSuite<StoredSourceDefinition> TEST_SUITE =
        fromValue(DefinitionTests.RESOURCES_PATH, SUITE_NAME, StoredSourceDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<StoredSourceDefinition> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<StoredSourceDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<StoredSourceDefinition>>
        data() {
      return TEST_SUITE.withExamples(
          includeAll(), excludeAll(), includePaths(), excludePaths(), duplicatedPaths());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<StoredSourceDefinition> includeAll() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "include all", StoredSourceDefinition.createIncludeAll());
    }

    private static BsonDeserializationTestSuite.ValidSpec<StoredSourceDefinition> excludeAll() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "exclude all", StoredSourceDefinition.createExcludeAll());
    }

    private static BsonDeserializationTestSuite.ValidSpec<StoredSourceDefinition> includePaths() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "include paths",
          StoredSourceDefinition.create(INCLUSION, List.of("_id", "a", "b.c", "b.d", "x.y.z")));
    }

    private static BsonDeserializationTestSuite.ValidSpec<StoredSourceDefinition> excludePaths() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "exclude paths",
          StoredSourceDefinition.create(EXCLUSION, List.of("a", "b.c", "b.d", "x.y.z")));
    }

    private static BsonDeserializationTestSuite.ValidSpec<StoredSourceDefinition>
        duplicatedPaths() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "duplicated paths",
          StoredSourceDefinition.create(EXCLUSION, List.of("k", "k", "l", "m")));
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {

    private static final String SUITE_NAME = "storedSource-serialization";
    private static final BsonSerializationTestSuite<StoredSourceDefinition> TEST_SUITE =
        load(DefinitionTests.RESOURCES_PATH, SUITE_NAME, StoredSourceDefinition::toBson);

    private final BsonSerializationTestSuite.TestSpec<StoredSourceDefinition> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<StoredSourceDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<StoredSourceDefinition>> data() {
      return List.of(includeAll(), excludeAll(), includePaths(), excludePaths(), duplicatedPaths());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<StoredSourceDefinition> includeAll() {
      return BsonSerializationTestSuite.TestSpec.create(
          "include all", StoredSourceDefinition.createIncludeAll());
    }

    private static BsonSerializationTestSuite.TestSpec<StoredSourceDefinition> excludeAll() {
      return BsonSerializationTestSuite.TestSpec.create(
          "exclude all", StoredSourceDefinition.createExcludeAll());
    }

    private static BsonSerializationTestSuite.TestSpec<StoredSourceDefinition> includePaths() {
      return BsonSerializationTestSuite.TestSpec.create(
          "include paths",
          StoredSourceDefinition.create(INCLUSION, List.of("_id", "a", "b.c", "b.d", "x.y.z")));
    }

    private static BsonSerializationTestSuite.TestSpec<StoredSourceDefinition> excludePaths() {
      return BsonSerializationTestSuite.TestSpec.create(
          "exclude paths",
          StoredSourceDefinition.create(EXCLUSION, List.of("a", "b.c", "b.d", "x.y.z")));
    }

    private static BsonSerializationTestSuite.TestSpec<StoredSourceDefinition> duplicatedPaths() {
      return BsonSerializationTestSuite.TestSpec.create(
          "duplicated paths",
          StoredSourceDefinition.create(EXCLUSION, List.of("k", "l.m.n", "l.m.n")));
    }
  }

  public static class DefinitionTest {

    @Test
    public void testInclusionWithExclusionMode() {
      StoredSourceDefinition definition = StoredSourceDefinition.create(EXCLUSION, List.of("a"));
      Assert.assertThrows(IllegalArgumentException.class, definition::asInclusion);
    }

    @Test
    public void testIsExclusionWithInclusionMode() {
      StoredSourceDefinition definition = StoredSourceDefinition.create(INCLUSION, List.of("a"));
      Assert.assertThrows(IllegalArgumentException.class, definition::asExclusion);
    }

    @Test
    public void testInclusionWhenAllIncluded() {
      StoredSourceDefinition definition = StoredSourceDefinition.createIncludeAll();
      Assert.assertTrue(definition.isAllIncluded());

      Assert.assertTrue(definition.asInclusion().isIncluded(FieldPath.fromParts("_id")));
      Assert.assertTrue(definition.asInclusion().isIncluded(FieldPath.fromParts("a")));
      Assert.assertTrue(definition.asInclusion().isIncluded(FieldPath.fromParts("b", "c")));

      Assert.assertTrue(definition.asInclusion().isPathToIncluded(FieldPath.fromParts("_id")));
      Assert.assertTrue(definition.asInclusion().isPathToIncluded(FieldPath.fromParts("a")));
      Assert.assertTrue(definition.asInclusion().isPathToIncluded(FieldPath.fromParts("b", "c")));
    }

    @Test
    public void testInclusionWhenAllExcluded() {
      StoredSourceDefinition definition = StoredSourceDefinition.createExcludeAll();
      Assert.assertTrue(definition.isAllExcluded());

      Assert.assertFalse(definition.asInclusion().isIncluded(FieldPath.fromParts("_id")));
      Assert.assertFalse(definition.asInclusion().isIncluded(FieldPath.fromParts("a")));
      Assert.assertFalse(definition.asInclusion().isIncluded(FieldPath.fromParts("b", "c")));

      Assert.assertFalse(definition.asInclusion().isPathToIncluded(FieldPath.fromParts("_id")));
      Assert.assertFalse(definition.asInclusion().isPathToIncluded(FieldPath.fromParts("a")));
      Assert.assertFalse(definition.asInclusion().isPathToIncluded(FieldPath.fromParts("b", "c")));
    }

    @Test
    public void testExclusionWhenAllIncluded() {
      StoredSourceDefinition definition = StoredSourceDefinition.createIncludeAll();
      Assert.assertTrue(definition.isAllIncluded());

      Assert.assertFalse(definition.asExclusion().isExcluded(FieldPath.fromParts("_id")));
      Assert.assertFalse(definition.asExclusion().isExcluded(FieldPath.fromParts("x", "y", "z")));
    }

    @Test
    public void testExclusionWhenAllExcluded() {
      StoredSourceDefinition definition = StoredSourceDefinition.createExcludeAll();
      Assert.assertTrue(definition.isAllExcluded());

      Assert.assertTrue(definition.asExclusion().isExcluded(FieldPath.fromParts("_id")));
      Assert.assertTrue(definition.asExclusion().isExcluded(FieldPath.fromParts("x", "y", "z")));
    }

    @Test
    public void testIsIncluded() {
      StoredSourceDefinition definition =
          StoredSourceDefinition.create(INCLUSION, List.of("a", "b.c", "b.d"));
      Assert.assertTrue(definition.asInclusion().isIncluded(FieldPath.fromParts("_id")));
      Assert.assertTrue(definition.asInclusion().isIncluded(FieldPath.fromParts("a")));
      Assert.assertTrue(definition.asInclusion().isIncluded(FieldPath.fromParts("a", "d", "e")));
      Assert.assertFalse(definition.asInclusion().isIncluded(FieldPath.fromParts("b")));
      Assert.assertTrue(definition.asInclusion().isIncluded(FieldPath.fromParts("b", "c")));
      Assert.assertTrue(definition.asInclusion().isIncluded(FieldPath.fromParts("b", "d")));
      Assert.assertTrue(
          definition.asInclusion().isIncluded(FieldPath.fromParts("b", "d", "e", "f", "g")));
      Assert.assertFalse(definition.asInclusion().isIncluded(FieldPath.fromParts("b", "f")));
      Assert.assertFalse(definition.asInclusion().isIncluded(FieldPath.fromParts("c")));
    }

    @Test
    public void testIsPathToIncluded() {
      StoredSourceDefinition definition =
          StoredSourceDefinition.create(INCLUSION, List.of("a.b.c"));
      Assert.assertTrue(definition.asInclusion().isPathToIncluded(FieldPath.fromParts("_id")));
      Assert.assertTrue(definition.asInclusion().isPathToIncluded(FieldPath.fromParts("a")));
      Assert.assertTrue(definition.asInclusion().isPathToIncluded(FieldPath.fromParts("a", "b")));
      Assert.assertTrue(
          definition.asInclusion().isPathToIncluded(FieldPath.fromParts("a", "b", "c")));
      Assert.assertTrue(
          definition.asInclusion().isPathToIncluded(FieldPath.fromParts("a", "b", "c", "d")));
      Assert.assertFalse(
          definition.asInclusion().isPathToIncluded(FieldPath.fromParts("x", "y", "z")));
    }

    @Test
    public void testIsExcluded() {
      StoredSourceDefinition definition =
          StoredSourceDefinition.create(EXCLUSION, List.of("a", "b.c", "b.d"));
      Assert.assertFalse(definition.asExclusion().isExcluded(FieldPath.fromParts("_id")));
      Assert.assertTrue(definition.asExclusion().isExcluded(FieldPath.fromParts("a")));
      Assert.assertTrue(definition.asExclusion().isExcluded(FieldPath.fromParts("a", "d")));
      Assert.assertFalse(definition.asExclusion().isExcluded(FieldPath.fromParts("b")));
      Assert.assertTrue(definition.asExclusion().isExcluded(FieldPath.fromParts("b", "c")));
      Assert.assertTrue(definition.asExclusion().isExcluded(FieldPath.fromParts("b", "d")));
      Assert.assertTrue(definition.asExclusion().isExcluded(FieldPath.fromParts("b", "d", "x")));
    }

    @Test
    public void testEquals() {

      var equalityTester = new EqualsTester();

      equalityTester.addEqualityGroup(
          StoredSourceDefinition.create(INCLUSION, List.of("a", "b", "b", "c")),
          StoredSourceDefinition.create(INCLUSION, List.of("a", "b", "c")));

      equalityTester.addEqualityGroup(
          StoredSourceDefinition.create(INCLUSION, List.of("x", "y", "z")),
          StoredSourceDefinition.create(INCLUSION, List.of("z", "y", "x")));

      equalityTester.addEqualityGroup(
          StoredSourceDefinition.create(INCLUSION, List.of("m.n", "m.o", "m.p")),
          StoredSourceDefinition.create(INCLUSION, List.of("m.o", "m.n", "m.p")));

      equalityTester.addEqualityGroup(
          StoredSourceDefinition.defaultValue(), StoredSourceDefinition.createExcludeAll());

      equalityTester.testEquals();
    }
  }
}
