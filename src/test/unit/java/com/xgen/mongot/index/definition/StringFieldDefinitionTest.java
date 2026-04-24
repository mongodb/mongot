package com.xgen.mongot.index.definition;

import static com.xgen.testing.BsonDeserializationTestSuite.fromDocument;
import static com.xgen.testing.BsonSerializationTestSuite.load;

import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      StringFieldDefinitionTest.DeserializationTest.class,
      StringFieldDefinitionTest.SerializationTest.class,
      StringFieldDefinitionTest.DefinitionTest.class,
    })
public class StringFieldDefinitionTest {

  @RunWith(Parameterized.class)
  public static class DeserializationTest {

    private static final String SUITE_NAME = "string-deserialization";
    private static final BsonDeserializationTestSuite<StringFieldDefinition> TEST_SUITE =
        fromDocument(DefinitionTests.RESOURCES_PATH, SUITE_NAME, StringFieldDefinition::fromBson);

    private final BsonDeserializationTestSuite.TestSpecWrapper<StringFieldDefinition> testSpec;

    public DeserializationTest(
        BsonDeserializationTestSuite.TestSpecWrapper<StringFieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<StringFieldDefinition>>
        data() {
      return TEST_SUITE.withExamples(
          empty(),
          withAnalyzer(),
          withSearchAnalyzer(),
          withIgnoreAbove(),
          explicitDocsIndexOptions(),
          explicitFreqsIndexOptions(),
          explicitPositionsIndexOptions(),
          explicitOffsetsIndexOptions(),
          explicitStore(),
          explicitNorms(),
          withMulti());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<StringFieldDefinition> empty() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "empty", StringFieldDefinitionBuilder.builder().build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<StringFieldDefinition> withAnalyzer() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with analyzer",
          StringFieldDefinitionBuilder.builder().analyzerName("my-analyzer").build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<StringFieldDefinition>
        withSearchAnalyzer() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with searchAnalyzer",
          StringFieldDefinitionBuilder.builder().searchAnalyzerName("my-search-analyzer").build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<StringFieldDefinition> withIgnoreAbove() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with ignoreAbove", StringFieldDefinitionBuilder.builder().ignoreAbove(13).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<StringFieldDefinition>
        explicitDocsIndexOptions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit docs indexOptions",
          StringFieldDefinitionBuilder.builder()
              .indexOptions(StringFieldDefinition.IndexOptions.DOCS)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<StringFieldDefinition>
        explicitFreqsIndexOptions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit freqs indexOptions",
          StringFieldDefinitionBuilder.builder()
              .indexOptions(StringFieldDefinition.IndexOptions.FREQS)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<StringFieldDefinition>
        explicitPositionsIndexOptions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit positions indexOptions",
          StringFieldDefinitionBuilder.builder()
              .indexOptions(StringFieldDefinition.IndexOptions.POSITIONS)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<StringFieldDefinition>
        explicitOffsetsIndexOptions() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit offsets indexOptions",
          StringFieldDefinitionBuilder.builder()
              .indexOptions(StringFieldDefinition.IndexOptions.OFFSETS)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<StringFieldDefinition> explicitStore() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit store", StringFieldDefinitionBuilder.builder().store(false).build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<StringFieldDefinition> explicitNorms() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "explicit norms",
          StringFieldDefinitionBuilder.builder()
              .norms(StringFieldDefinition.NormsOptions.OMIT)
              .build());
    }

    private static BsonDeserializationTestSuite.ValidSpec<StringFieldDefinition> withMulti() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "with multi",
          StringFieldDefinitionBuilder.builder()
              .multi(
                  "a",
                  StringFieldDefinitionBuilder.builder()
                      .analyzerName("my-multi-analyzer")
                      .similarity(SimilarityDefinition.STABLE_TFL)
                      .build())
              .multi(
                  "b",
                  StringFieldDefinitionBuilder.builder()
                      .analyzerName("my-other-multi-analyzer")
                      .similarity(SimilarityDefinition.BOOLEAN)
                      .build())
              .multi(
                  "c",
                  StringFieldDefinitionBuilder.builder()
                      .analyzerName("my-other-multi-analyzer")
                      .similarity(SimilarityDefinition.BM25)
                      .build())
              .build());
    }
  }

  @RunWith(Parameterized.class)
  public static class SerializationTest {

    private static final String SUITE_NAME = "string-serialization";
    private static final BsonSerializationTestSuite<StringFieldDefinition> TEST_SUITE =
        load(DefinitionTests.RESOURCES_PATH, SUITE_NAME, StringFieldDefinition::fieldTypeToBson);

    private final BsonSerializationTestSuite.TestSpec<StringFieldDefinition> testSpec;

    public SerializationTest(BsonSerializationTestSuite.TestSpec<StringFieldDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<StringFieldDefinition>> data() {
      return Arrays.asList(
          simple(),
          withAnalyzer(),
          withSearchAnalyzer(),
          withIgnoreAbove(),
          withNonDefaultIndexOption(),
          withMulti());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<StringFieldDefinition> simple() {
      return BsonSerializationTestSuite.TestSpec.create(
          "simple", StringFieldDefinitionBuilder.builder().build());
    }

    private static BsonSerializationTestSuite.TestSpec<StringFieldDefinition> withAnalyzer() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with analyzer",
          StringFieldDefinitionBuilder.builder().analyzerName("my-analyzer").build());
    }

    private static BsonSerializationTestSuite.TestSpec<StringFieldDefinition> withSearchAnalyzer() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with searchAnalyzer",
          StringFieldDefinitionBuilder.builder().searchAnalyzerName("my-search-analyzer").build());
    }

    private static BsonSerializationTestSuite.TestSpec<StringFieldDefinition> withIgnoreAbove() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with ignoreAbove", StringFieldDefinitionBuilder.builder().ignoreAbove(13).build());
    }

    private static BsonSerializationTestSuite.TestSpec<StringFieldDefinition>
        withNonDefaultIndexOption() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with non-default indexOptions",
          StringFieldDefinitionBuilder.builder()
              .indexOptions(StringFieldDefinition.IndexOptions.DOCS)
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<StringFieldDefinition> withMulti() {
      return BsonSerializationTestSuite.TestSpec.create(
          "with multi",
          StringFieldDefinitionBuilder.builder()
              .multi(
                  "a",
                  StringFieldDefinitionBuilder.builder()
                      .analyzerName("my-multi-analyzer")
                      .similarity(SimilarityDefinition.STABLE_TFL)
                      .build())
              .multi(
                  "b",
                  StringFieldDefinitionBuilder.builder()
                      .analyzerName("my-other-multi-analyzer")
                      .similarity(SimilarityDefinition.BOOLEAN)
                      .build())
              .multi(
                  "c",
                  StringFieldDefinitionBuilder.builder()
                      .analyzerName("my-other-multi-analyzer")
                      .similarity(SimilarityDefinition.BM25)
                      .build())
              .build());
    }
  }

  public static class DefinitionTest {

    @Test
    public void testGetType() {
      StringFieldDefinition definition = StringFieldDefinitionBuilder.builder().build();
      Assert.assertEquals(FieldTypeDefinition.Type.STRING, definition.getType());
    }

    @Test
    public void testGetAnalyzerName() {
      StringFieldDefinition definitionWithoutAnalyzer =
          StringFieldDefinitionBuilder.builder().build();
      Assert.assertEquals(Optional.empty(), definitionWithoutAnalyzer.analyzerName());

      StringFieldDefinition definitionWithAnalyzer =
          StringFieldDefinitionBuilder.builder().analyzerName("my-analyzer").build();
      Assert.assertEquals(Optional.of("my-analyzer"), definitionWithAnalyzer.analyzerName());
    }

    @Test
    public void testGetSearchAnalyzerName() {
      StringFieldDefinition definitionWithoutAnalyzer =
          StringFieldDefinitionBuilder.builder().build();
      Assert.assertEquals(Optional.empty(), definitionWithoutAnalyzer.searchAnalyzerName());

      StringFieldDefinition definitionWithAnalyzer =
          StringFieldDefinitionBuilder.builder().searchAnalyzerName("my-search-analyzer").build();
      Assert.assertEquals(
          Optional.of("my-search-analyzer"), definitionWithAnalyzer.searchAnalyzerName());
    }

    @Test
    public void testGetIgnoreAbove() {
      StringFieldDefinition definitionWithoutIgnoreAbove =
          StringFieldDefinitionBuilder.builder().build();
      Assert.assertEquals(Optional.empty(), definitionWithoutIgnoreAbove.ignoreAbove());

      StringFieldDefinition definitionWithIgnoreAbove =
          StringFieldDefinitionBuilder.builder().ignoreAbove(13).build();
      Assert.assertEquals(Optional.of(13), definitionWithIgnoreAbove.ignoreAbove());
    }

    @Test
    public void testGetIndexOptions() {
      StringFieldDefinition definition =
          StringFieldDefinitionBuilder.builder()
              .indexOptions(StringFieldDefinition.IndexOptions.FREQS)
              .build();
      Assert.assertEquals(StringFieldDefinition.IndexOptions.FREQS, definition.indexOptions());
    }

    @Test
    public void testGetMulti() {
      StringFieldDefinition definition =
          StringFieldDefinitionBuilder.builder()
              .multi("multi", StringFieldDefinitionBuilder.builder().build())
              .build();

      var multi = Map.ofEntries(Map.entry("multi", StringFieldDefinitionBuilder.builder().build()));
      Assert.assertEquals(multi, definition.multi());
    }

    @Test
    public void testGetStoreFlag() {
      StringFieldDefinition definition =
          StringFieldDefinitionBuilder.builder().store(false).build();

      Assert.assertFalse(definition.storeFlag());
    }

    @Test
    public void testGetNorms() {
      StringFieldDefinition definition =
          StringFieldDefinitionBuilder.builder()
              .norms(StringFieldDefinition.NormsOptions.OMIT)
              .build();

      Assert.assertTrue(definition.norms().equals(StringFieldDefinition.NormsOptions.OMIT));
    }

    @Test
    public void testEquals() {
      TestUtils.assertEqualityGroups(
          () -> StringFieldDefinitionBuilder.builder().analyzerName("analyzer").build(),
          () -> StringFieldDefinitionBuilder.builder().analyzerName("other-analyzer").build(),
          () -> StringFieldDefinitionBuilder.builder().ignoreAbove(13).build(),
          () -> StringFieldDefinitionBuilder.builder().ignoreAbove(14).build(),
          () ->
              StringFieldDefinitionBuilder.builder()
                  .indexOptions(StringFieldDefinition.IndexOptions.DOCS)
                  .build(),
          () ->
              StringFieldDefinitionBuilder.builder()
                  .indexOptions(StringFieldDefinition.IndexOptions.FREQS)
                  .build(),
          () ->
              StringFieldDefinitionBuilder.builder()
                  .multi(
                      "multi", StringFieldDefinitionBuilder.builder().analyzerName("multi").build())
                  .build(),
          () ->
              StringFieldDefinitionBuilder.builder()
                  .multi(
                      "multi",
                      StringFieldDefinitionBuilder.builder().analyzerName("other-multi").build())
                  .build(),
          () -> StringFieldDefinitionBuilder.builder().searchAnalyzerName("searchAnalyzer").build(),
          () ->
              StringFieldDefinitionBuilder.builder()
                  .searchAnalyzerName("other-searchAnalyzer")
                  .build(),
          () ->
              StringFieldDefinitionBuilder.builder()
                  .store(true)
                  .norms(StringFieldDefinition.NormsOptions.INCLUDE)
                  .build(),
          () ->
              StringFieldDefinitionBuilder.builder()
                  .store(false)
                  .norms(StringFieldDefinition.NormsOptions.INCLUDE)
                  .build(),
          () ->
              StringFieldDefinitionBuilder.builder()
                  .store(true)
                  .norms(StringFieldDefinition.NormsOptions.OMIT)
                  .build());
    }
  }
}
