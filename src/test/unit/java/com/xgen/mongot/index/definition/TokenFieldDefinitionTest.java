package com.xgen.mongot.index.definition;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.load;

import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.TokenFieldDefinitionBuilder;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      TokenFieldDefinitionTest.DeserializationTest.class,
      TokenFieldDefinitionTest.SerializationTest.class,
      TokenFieldDefinitionTest.DefinitionTest.class
    })
public class TokenFieldDefinitionTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {

    private static final String SUITE_NAME = "token-deserialization";
    private static final BsonDeserializationTestSuite<TokenFieldDefinition> TEST_SUITE =
        fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, TokenFieldDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<TokenFieldDefinition> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<TokenFieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<TokenFieldDefinition>>
        data() {
      return TEST_SUITE.withExamples(empty(), withNormalizer());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFieldDefinition> empty() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "empty", TokenFieldDefinitionBuilder.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<TokenFieldDefinition> withNormalizer() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with normalizer",
          TokenFieldDefinitionBuilder.builder().normalizerName(StockNormalizerName.NONE).build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {

    private static final String SUITE_NAME = "token-serialization";
    private static final BsonSerializationTestSuite<TokenFieldDefinition> TEST_SUITE =
        load(DefinitionTests.RESOURCES_PATH, SUITE_NAME, TokenFieldDefinition::fieldTypeToBson);

    private final BsonSerializationTestSuite.TestSpec<TokenFieldDefinition> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<TokenFieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<TokenFieldDefinition>> data() {
      return Arrays.asList(empty(), withNormalizer());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFieldDefinition> empty() {
      return BsonSerializationTestSuite.TestSpec.create(
          "empty", TokenFieldDefinitionBuilder.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<TokenFieldDefinition> withNormalizer() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with normalizer",
          TokenFieldDefinitionBuilder.builder().normalizerName(StockNormalizerName.NONE).build());
    }
  }

  public static class DefinitionTest {
    @Test
    public void testGetType() {
      var definition = TokenFieldDefinitionBuilder.builder().build();
      Assert.assertEquals(FieldTypeDefinition.Type.TOKEN, definition.getType());
    }

    @Test
    public void testEquals() {
      TestUtils.assertEqualityGroups(
          () -> TokenFieldDefinitionBuilder.builder().build(),
          () ->
              TokenFieldDefinitionBuilder.builder()
                  .normalizerName(StockNormalizerName.LOWERCASE)
                  .build());
    }
  }
}
