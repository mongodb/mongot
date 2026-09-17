package com.xgen.mongot.index.version;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.TestUtils;
import java.util.Arrays;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      GenerationTest.DeserializationTest.class,
      GenerationTest.SerializationTest.class,
      GenerationTest.ClassTest.class,
    })
public class GenerationTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {

    private static final String SUITE_NAME = "index-generation-deserialization";
    private static final BsonDeserializationTestSuite<Generation> TEST_SUITE =
        fromDocument("src/test/unit/resources/index/version/", SUITE_NAME, Generation::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<Generation> testSpec;

    public DeserializationTest(BsonDeserializationTestSuite.TestSpecWrapper<Generation> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<Generation>> data() {
      return TEST_SUITE.withExamples(simple(), noAttemptNum(), explicitLucene(), vectorlite());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<Generation> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid("simple", generation(1, 2, 1));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Generation> noAttemptNum() {
      return BsonDeserializationTestSuite.TestSpec.valid("no attempt number", generation(1, 2, 0));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Generation> explicitLucene() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit lucene engine", generation(1, 2, 0));
    }

    private static BsonDeserializationTestSuite.ValidSpec<Generation> vectorlite() {
      return BsonDeserializationTestSuite.TestSpec.valid("vectorlite", vectorliteGeneration());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {

    private static final String SUITE_NAME = "index-generation-serialization";
    private static final BsonSerializationTestSuite<Generation> TEST_SUITE =
        fromEncodable("src/test/unit/resources/index/version/", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<Generation> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<Generation> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<Generation>> data() {

      return Arrays.asList(simple(), noAttemptNum(), vectorlite());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<Generation> simple() {
      return BsonSerializationTestSuite.TestSpec.create("simple", generation(1, 2, 1));
    }

    private static BsonSerializationTestSuite.TestSpec<Generation> noAttemptNum() {
      return BsonSerializationTestSuite.TestSpec.create("no attempt number", generation(1, 2, 0));
    }

    private static BsonSerializationTestSuite.TestSpec<Generation> vectorlite() {
      return BsonSerializationTestSuite.TestSpec.create("vectorlite", vectorliteGeneration());
    }
  }

  public static class ClassTest {
    @Test
    public void testIncrement() {
      var value = generation(1, 2, 1);
      var expected = generation(2, 2, 0);
      Assert.assertEquals(expected, value.incrementUser());
    }

    @Test
    public void testGenerationId() {
      var gen = generation(3, 4, 1);
      ObjectId oid = new ObjectId();
      var actual = gen.generationId(oid);
      var expected = new GenerationId(oid, generation(3, 4, 1));
      Assert.assertEquals(expected, actual);
    }

    @Test
    public void testNextAttempt() {
      var value = generation(1, 2, 1);
      var expected = generation(1, 2, 2);
      Assert.assertEquals(expected, value.nextAttempt());
    }

    @Test
    public void vectorlite_keepsItsEngineThroughAttemptsAndUserVersions() {
      var vectorlite = new Generation(new UserIndexVersion(1), VectorliteFormatVersion.CURRENT);
      Assert.assertEquals(IndexEngine.VECTORLITE, vectorlite.formatVersion.engine());
      Assert.assertEquals(VectorliteFormatVersion.CURRENT, vectorlite.formatVersion.asVectorlite());
      Assert.assertEquals(IndexEngine.VECTORLITE, vectorlite.nextAttempt().formatVersion.engine());
      Assert.assertEquals(
          IndexEngine.VECTORLITE, vectorlite.incrementUser().formatVersion.engine());
      Assert.assertEquals(IndexEngine.LUCENE, Generation.CURRENT.formatVersion.engine());
    }

    @Test
    public void current_startsAtTheEnginesCurrentFormatVersion() {
      Assert.assertEquals(
          Generation.CURRENT, Generation.current(IndexEngine.LUCENE, UserIndexVersion.FIRST));
      Assert.assertEquals(
          new Generation(UserIndexVersion.FIRST, VectorliteFormatVersion.CURRENT),
          Generation.current(IndexEngine.VECTORLITE, UserIndexVersion.FIRST));
    }

    @Test
    public void formatVersion_isReadInTheEnginesLine() {
      var lucene = new Generation(UserIndexVersion.FIRST, IndexFormatVersion.MIN_SUPPORTED_VERSION);
      Assert.assertEquals(
          IndexFormatVersion.MIN_SUPPORTED_VERSION, lucene.formatVersion.asLucene());
      Assert.assertEquals(
          IndexFormatVersion.MIN_SUPPORTED_VERSION, lucene.capabilitiesFormatVersion());
      Assert.assertFalse(lucene.formatVersion.isCurrent());
      Assert.assertTrue(lucene.formatVersion.isSupported());
      Assert.assertThrows(UnsupportedOperationException.class, lucene.formatVersion::asVectorlite);

      var next = new VectorliteFormatVersion(VectorliteFormatVersion.CURRENT.versionNumber() + 1);
      var vectorlite = new Generation(UserIndexVersion.FIRST, next);
      Assert.assertEquals(next, vectorlite.formatVersion.asVectorlite());
      Assert.assertEquals(IndexFormatVersion.CURRENT, vectorlite.capabilitiesFormatVersion());
      Assert.assertFalse(vectorlite.formatVersion.isCurrent());
      Assert.assertFalse(vectorlite.formatVersion.isSupported());
      Assert.assertThrows(UnsupportedOperationException.class, vectorlite.formatVersion::asLucene);
    }

    @Test
    public void engine_isSerializedOnlyForVectorlite() {
      Assert.assertFalse(Generation.CURRENT.toBson().containsKey("engine"));
      var vectorlite = new Generation(UserIndexVersion.FIRST, new VectorliteFormatVersion(7));
      Assert.assertEquals("vectorlite", vectorlite.toBson().getString("engine").getValue());
      Assert.assertEquals(7, vectorlite.toBson().getInt32("formatVersion").getValue());
    }

    @Test
    public void testIndexGenerationEquals() {
      var u1 = new UserIndexVersion(1);
      var u2 = new UserIndexVersion(2);
      var b1 = IndexFormatVersion.create(1);
      var b2 = IndexFormatVersion.create(2);

      TestUtils.assertEqualityGroups(
          () -> new Generation(u1, b1, 0),
          () -> new Generation(u1, b2, 0),
          () -> new Generation(u1, b2, 1),
          () -> new Generation(u2, b1, 0),
          () -> new Generation(u2, b2, 0),
          () -> new Generation(u2, b2, 1),
          () -> new Generation(u1, new VectorliteFormatVersion(1)),
          () -> new Generation(u1, new VectorliteFormatVersion(2)),
          () -> new Generation(u1, new VectorliteFormatVersion(2)).nextAttempt());
    }
  }

  public static Generation generation(int user, int backend, int attempt) {
    return new Generation(new UserIndexVersion(user), IndexFormatVersion.create(backend), attempt);
  }

  /** A vectorlite generation independent of CURRENT. */
  static Generation vectorliteGeneration() {
    return new Generation(new UserIndexVersion(1), new VectorliteFormatVersion(3));
  }
}
