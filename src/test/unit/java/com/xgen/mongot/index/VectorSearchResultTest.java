package com.xgen.mongot.index;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.fromEncodable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.VectorSearchResultBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      VectorSearchResultTest.TestSerialization.class,
      VectorSearchResultTest.TestDeserialization.class,
      VectorSearchResultTest.TestComparatorIdTieBreaker.class
    })
public class VectorSearchResultTest {

  static final String RESOURCES_PATH = "src/test/unit/resources/index";

  @RunWith(Parameterized.class)
  public static class TestDeserialization {
    private static final String SUITE_NAME = "vector-search-result-deserialization";

    private static final BsonDeserializationTestSuite<VectorSearchResult> TEST_SUITE =
        fromDocument(RESOURCES_PATH, SUITE_NAME, VectorSearchResult::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<VectorSearchResult> testSpec;

    public TestDeserialization(
        BsonDeserializationTestSuite.TestSpecWrapper<VectorSearchResult> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<VectorSearchResult>>
        data() {
      return TEST_SUITE.withExamples(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<VectorSearchResult> simple() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "simple", VectorSearchResultBuilder.builder().id(new BsonInt32(0)).score(0.0f).build());
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {
    private static final String SUITE_NAME = "vector-search-result-serialization";
    private static final BsonSerializationTestSuite<VectorSearchResult> TEST_SUITE =
        fromEncodable(RESOURCES_PATH, SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<VectorSearchResult> testSpec;

    public TestSerialization(BsonSerializationTestSuite.TestSpec<VectorSearchResult> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<VectorSearchResult>> data() {
      return Arrays.asList(simple());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<VectorSearchResult> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple", VectorSearchResultBuilder.builder().id(new BsonInt32(0)).score(0.0f).build());
    }
  }

  public static class TestComparatorIdTieBreaker {
    @Test
    public void testComparatorWithInt32Ids() {
      VectorSearchResult result1 =
          VectorSearchResultBuilder.builder().id(new BsonInt32(1)).score(0.8f).build();
      VectorSearchResult result2 =
          VectorSearchResultBuilder.builder().id(new BsonInt32(2)).score(0.8f).build();
      VectorSearchResult result3 =
          VectorSearchResultBuilder.builder().id(new BsonInt32(1)).score(0.8f).build();

      assertTrue(VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result2) < 0);
      assertEquals(0, VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result3));
    }

    @Test
    public void testComparatorWithInt64Ids() {
      VectorSearchResult result1 =
          VectorSearchResultBuilder.builder().id(new BsonInt64(100L)).score(0.8f).build();
      VectorSearchResult result2 =
          VectorSearchResultBuilder.builder().id(new BsonInt64(200L)).score(0.8f).build();

      assertTrue(VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result2) < 0);
    }

    @Test
    public void testComparatorWithDecimal128Ids() {
      VectorSearchResult result1 =
          VectorSearchResultBuilder.builder()
              .id(new BsonDecimal128(Decimal128.parse("1.5")))
              .score(0.8f)
              .build();
      VectorSearchResult result2 =
          VectorSearchResultBuilder.builder()
              .id(new BsonDecimal128(Decimal128.parse("2.5")))
              .score(0.8f)
              .build();
      VectorSearchResult result3 =
          VectorSearchResultBuilder.builder()
              .id(new BsonDecimal128(Decimal128.parse("1.5")))
              .score(0.8f)
              .build();

      assertTrue(VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result2) < 0);
      assertEquals(0, VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result3));
    }

    @Test
    public void testComparatorWithDoubleIds() {
      VectorSearchResult result1 =
          VectorSearchResultBuilder.builder().id(new BsonDouble(1.5)).score(0.8f).build();
      VectorSearchResult result2 =
          VectorSearchResultBuilder.builder().id(new BsonDouble(2.5)).score(0.8f).build();
      VectorSearchResult result3 =
          VectorSearchResultBuilder.builder().id(new BsonDouble(1.5)).score(0.8f).build();

      assertTrue(VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result2) < 0);
      assertEquals(0, VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result3));
    }

    @Test
    public void testComparatorWithStringIds() {
      VectorSearchResult result1 =
          VectorSearchResultBuilder.builder().id(new BsonString("abc")).score(0.8f).build();
      VectorSearchResult result2 =
          VectorSearchResultBuilder.builder().id(new BsonString("xyz")).score(0.8f).build();

      assertTrue(VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result2) < 0);
    }

    @Test
    public void testComparatorWithBinaryIds() {
      VectorSearchResult result1 =
          VectorSearchResultBuilder.builder()
              .id(
                  new BsonBinary(
                      BsonBinarySubType.UUID_STANDARD,
                      new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}))
              .score(0.8f)
              .build();
      VectorSearchResult result2 =
          VectorSearchResultBuilder.builder()
              .id(
                  new BsonBinary(
                      BsonBinarySubType.UUID_STANDARD,
                      new byte[] {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}))
              .score(0.8f)
              .build();

      int comparison = VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result2);
      assertTrue(comparison < 0);
    }

    @Test
    public void testComparatorWithObjectIdIds() {
      ObjectId id1 = new ObjectId();
      ObjectId id2 = new ObjectId();
      VectorSearchResult result1 =
          VectorSearchResultBuilder.builder().id(new BsonObjectId(id1)).score(0.8f).build();
      VectorSearchResult result2 =
          VectorSearchResultBuilder.builder().id(new BsonObjectId(id2)).score(0.8f).build();

      int comparison = VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result2);
      assertEquals(
          Integer.signum(id1.toHexString().compareTo(id2.toHexString())),
          Integer.signum(comparison));
    }

    @Test
    public void testComparatorWithBooleanIds() {
      VectorSearchResult falseResult =
          VectorSearchResultBuilder.builder().id(BsonBoolean.FALSE).score(0.8f).build();
      VectorSearchResult trueResult =
          VectorSearchResultBuilder.builder().id(BsonBoolean.TRUE).score(0.8f).build();

      assertTrue(
          VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(falseResult, trueResult) < 0);
      assertEquals(
          0, VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(falseResult, falseResult));
    }

    @Test
    public void testComparatorWithDateTimeIds() {
      VectorSearchResult result1 =
          VectorSearchResultBuilder.builder().id(new BsonDateTime(1000L)).score(0.8f).build();
      VectorSearchResult result2 =
          VectorSearchResultBuilder.builder().id(new BsonDateTime(2000L)).score(0.8f).build();

      assertTrue(VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result2) < 0);
    }

    @Test
    public void testComparatorWithTimestampIds() {
      VectorSearchResult result1 =
          VectorSearchResultBuilder.builder().id(new BsonTimestamp(1, 0)).score(0.8f).build();
      VectorSearchResult result2 =
          VectorSearchResultBuilder.builder().id(new BsonTimestamp(2, 0)).score(0.8f).build();

      assertTrue(VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result2) < 0);
    }

    @Test
    public void testComparatorWithNullIds() {
      VectorSearchResult result1 =
          VectorSearchResultBuilder.builder().id(BsonNull.VALUE).score(0.9f).build();
      VectorSearchResult result2 =
          VectorSearchResultBuilder.builder().id(BsonNull.VALUE).score(0.8f).build();

      assertEquals(0, VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result1));
      assertTrue(VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result2) < 0);
    }

    @Test
    public void testComparatorWithJavaScriptIds() {
      VectorSearchResult result1 =
          VectorSearchResultBuilder.builder()
              .id(new BsonJavaScript("function a() {}"))
              .score(0.8f)
              .build();
      VectorSearchResult result2 =
          VectorSearchResultBuilder.builder()
              .id(new BsonJavaScript("function b() {}"))
              .score(0.8f)
              .build();

      assertTrue(VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result2) < 0);
    }

    @Test
    public void testComparatorWithDocumentIds() {
      VectorSearchResult result1 =
          VectorSearchResultBuilder.builder()
              .id(new BsonDocument("key", new BsonInt32(1)))
              .score(0.8f)
              .build();
      VectorSearchResult result2 =
          VectorSearchResultBuilder.builder()
              .id(new BsonDocument("key", new BsonInt32(2)))
              .score(0.8f)
              .build();
      VectorSearchResult result3 =
          VectorSearchResultBuilder.builder()
              .id(new BsonDocument("key", new BsonInt32(1)))
              .score(0.8f)
              .build();
      VectorSearchResult result4 =
          VectorSearchResultBuilder.builder()
              .id(new BsonDocument("key2", new BsonInt32(1)))
              .score(0.8f)
              .build();

      assertTrue(VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result2) < 0);
      assertTrue(VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result4) < 0);
      assertEquals(0, VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(result1, result3));
    }

    @Test
    public void testComparatorMinKey() {
      VectorSearchResult minKeyResult =
          VectorSearchResultBuilder.builder().id(new BsonMinKey()).score(0.8f).build();
      VectorSearchResult maxKeyResult =
          VectorSearchResultBuilder.builder().id(new BsonMaxKey()).score(0.8f).build();
      VectorSearchResult intResult =
          VectorSearchResultBuilder.builder().id(new BsonInt32(0)).score(0.8f).build();
      VectorSearchResult stringResult =
          VectorSearchResultBuilder.builder().id(new BsonString("a")).score(0.8f).build();

      assertTrue(
          VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(minKeyResult, intResult) < 0);
      assertTrue(
          VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(minKeyResult, maxKeyResult)
              < 0);
      assertTrue(
          VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(minKeyResult, stringResult)
              < 0);
    }

    @Test
    public void testComparatorMaxKey() {
      VectorSearchResult maxKeyResult =
          VectorSearchResultBuilder.builder().id(new BsonMaxKey()).score(0.8f).build();
      VectorSearchResult intResult =
          VectorSearchResultBuilder.builder().id(new BsonInt32(0)).score(0.8f).build();
      VectorSearchResult stringResult =
          VectorSearchResultBuilder.builder().id(new BsonString("z")).score(0.8f).build();

      assertTrue(
          VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(maxKeyResult, intResult) > 0);
      assertTrue(
          VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(maxKeyResult, stringResult)
              > 0);
    }

    @Test
    public void testComparatorStringVsSymbolIds() {
      VectorSearchResult stringResult =
          VectorSearchResultBuilder.builder().id(new BsonString("abc")).score(0.8f).build();
      VectorSearchResult symbolResult =
          VectorSearchResultBuilder.builder().id(new BsonSymbol("xyz")).score(0.8f).build();

      assertTrue(
          VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR.compare(stringResult, symbolResult)
              < 0);
    }

    @Test
    public void testComparatorWithMixedIdTypes() {
      List<VectorSearchResult> results = new ArrayList<VectorSearchResult>();
      VectorSearchResult result1 =
          VectorSearchResultBuilder.builder().id(new BsonDouble(1.0)).score(0.8f).build();
      VectorSearchResult result2 =
          VectorSearchResultBuilder.builder().id(new BsonString("abc")).score(0.8f).build();
      VectorSearchResult result3 =
          VectorSearchResultBuilder.builder().id(new BsonInt32(42)).score(0.8f).build();

      results.add(result1);
      results.add(result2);
      results.add(result3);
      results.sort(VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR);

      assertTrue(results.get(0) == result1);
      assertTrue(results.get(1) == result3);
      assertTrue(results.get(2) == result2);
    }
  }
}
