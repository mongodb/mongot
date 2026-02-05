package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamType;
import com.xgen.mongot.index.analyzer.custom.LimitTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.SnowballStemmingTokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.TokenFilterDefinition;
import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.testing.BsonDeserializationTestSuite;
import com.xgen.testing.BsonSerializationTestSuite;
import com.xgen.testing.mongot.index.analyzer.custom.CustomCharFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.custom.TokenFilterDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.custom.TokenizerDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.definition.CustomAnalyzerDefinitionBuilder;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(
    value = {
      CustomAnalyzerDefinitionTest.TestDeserialization.class,
      CustomAnalyzerDefinitionTest.TestSerialization.class,
      CustomAnalyzerDefinitionTest.TestDefinition.class
    })
public class CustomAnalyzerDefinitionTest {

  @RunWith(Parameterized.class)
  public static class TestDeserialization {
    private static final String SUITE_NAME = "custom-analyzer-deserialization";
    private static final BsonDeserializationTestSuite<CustomAnalyzerDefinition> TEST_SUITE =
        BsonDeserializationTestSuite.fromDocument(
            "src/test/unit/resources/index/analyzer/",
            SUITE_NAME,
            CustomAnalyzerDefinition::fromBson);

    @Parameterized.Parameter
    public BsonDeserializationTestSuite.TestSpecWrapper<CustomAnalyzerDefinition> testSpec;

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonDeserializationTestSuite.TestSpecWrapper<CustomAnalyzerDefinition>>
        data() {
      return TEST_SUITE.withExamples(fullDefinition(), limitTokenCount());
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonDeserializationTestSuite.ValidSpec<CustomAnalyzerDefinition>
        fullDefinition() {
      return BsonDeserializationTestSuite.TestSpec.valid(
          "full definition",
          CustomAnalyzerDefinitionBuilder.builder(
                  "full definition", TokenizerDefinitionBuilder.StandardTokenizer.builder().build())
              .tokenFilter(
                  TokenFilterDefinitionBuilder.LengthTokenFilter.builder().min(2).max(12).build())
              .charFilter(CustomCharFilterDefinitionBuilder.HtmlStripCharFilter.builder().build())
              .build());
    }
  }

  private static BsonDeserializationTestSuite.ValidSpec<CustomAnalyzerDefinition>
      limitTokenCount() {
    return BsonDeserializationTestSuite.TestSpec.valid(
        "definition with limitTokenCount",
        CustomAnalyzerDefinitionBuilder.builder(
                "limit test", TokenizerDefinitionBuilder.StandardTokenizer.builder().build())
            .tokenFilter(
                TokenFilterDefinitionBuilder
                    .LimitTokenCountTokenFilter
                    .builder()
                    .maxTokenCount(5)
                    .build())
            .build());
  }

  @RunWith(Parameterized.class)
  public static class TestSerialization {
    private static final String SUITE_NAME = "custom-analyzer-serialization";
    private static final BsonSerializationTestSuite<CustomAnalyzerDefinition> TEST_SUITE =
        BsonSerializationTestSuite.fromEncodable(
            "src/test/unit/resources/index/analyzer/", SUITE_NAME);

    private final BsonSerializationTestSuite.TestSpec<CustomAnalyzerDefinition> testSpec;

    public TestSerialization(
        BsonSerializationTestSuite.TestSpec<CustomAnalyzerDefinition> testSpec) {
      this.testSpec = testSpec;
    }

    /** Test data. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<BsonSerializationTestSuite.TestSpec<CustomAnalyzerDefinition>> data() {
      return List.of(fullDefinition(), limitTokenCount()); // Add limitTokenCount() here
    }

    @Test
    public void runTest() throws Exception {
      TEST_SUITE.runTest(this.testSpec);
    }

    private static BsonSerializationTestSuite.TestSpec<CustomAnalyzerDefinition> fullDefinition() {
      return BsonSerializationTestSuite.TestSpec.create(
          "full definition",
          CustomAnalyzerDefinitionBuilder.builder(
                  "full definition", TokenizerDefinitionBuilder.StandardTokenizer.builder().build())
              .tokenFilter(
                  TokenFilterDefinitionBuilder.LengthTokenFilter.builder().min(2).max(12).build())
              .charFilter(CustomCharFilterDefinitionBuilder.HtmlStripCharFilter.builder().build())
              .build());
    }

    private static BsonSerializationTestSuite.TestSpec<CustomAnalyzerDefinition> limitTokenCount() {
      return BsonSerializationTestSuite.TestSpec.create(
          "limit token filter serialization",
          CustomAnalyzerDefinitionBuilder.builder(
                  "limit test", TokenizerDefinitionBuilder.StandardTokenizer.builder().build())
              .tokenFilter(
                  TokenFilterDefinitionBuilder
                      .LimitTokenCountTokenFilter
                      .builder()
                      .maxTokenCount(5)
                      .build())
              .build());
    }
  }

  public static class TestDefinition {
    @Test
    public void testOnlyStreamTokenizerProducesStream() {
      CustomAnalyzerDefinition definition =
          CustomAnalyzerDefinitionBuilder.builder(
                  "myCustomAz", TokenizerDefinitionBuilder.StandardTokenizer.builder().build())
              .build();

      Assert.assertSame(
          "should produce stream token stream",
          TokenStreamType.STREAM,
          definition.getTokenStreamType());
    }

    @Test
    public void testStreamTokenizerStreamTokenFilterProducesStream() {
      CustomAnalyzerDefinition definition =
          CustomAnalyzerDefinitionBuilder.builder(
                  "myCustomAz", TokenizerDefinitionBuilder.StandardTokenizer.builder().build())
              .tokenFilter(TokenFilterDefinitionBuilder.LowercaseTokenFilter.builder().build())
              .build();

      Assert.assertSame(
          "should produce stream token stream",
          TokenStreamType.STREAM,
          definition.getTokenStreamType());
    }

    @Test
    public void testStreamTokenizerGraphTokenFilterProducesGraph() {
      CustomAnalyzerDefinition definition =
          CustomAnalyzerDefinitionBuilder.builder(
                  "myCustomAz", TokenizerDefinitionBuilder.StandardTokenizer.builder().build())
              .tokenFilter(
                  TokenFilterDefinitionBuilder.NGramTokenFilter.builder()
                      .minGram(2)
                      .maxGram(4)
                      .build())
              .build();

      Assert.assertSame(
          "should produce graph token stream",
          TokenStreamType.GRAPH,
          definition.getTokenStreamType());
    }

    @Test
    public void testStreamTokenizerManyFiltersOneGraphProducesGraph() {
      CustomAnalyzerDefinition definition =
          CustomAnalyzerDefinitionBuilder.builder(
                  "myCustomAz", TokenizerDefinitionBuilder.StandardTokenizer.builder().build())
              .tokenFilter(
                  TokenFilterDefinitionBuilder.SnowballStemmingTokenFilter.builder()
                      .stemmerName(SnowballStemmingTokenFilterDefinition.StemmerName.SWEDISH)
                      .build())
              .tokenFilter(
                  TokenFilterDefinitionBuilder.NGramTokenFilter.builder()
                      .minGram(2)
                      .maxGram(4)
                      .build())
              .tokenFilter(TokenFilterDefinitionBuilder.LengthTokenFilter.builder().max(25).build())
              .tokenFilter(TokenFilterDefinitionBuilder.IcuFoldingTokenFilter.builder().build())
              .build();

      Assert.assertSame(
          "should produce graph token stream",
          TokenStreamType.GRAPH,
          definition.getTokenStreamType());
    }

    @Test
    public void testOnlyGraphTokenizerProducesGraph() {
      CustomAnalyzerDefinition definition =
          CustomAnalyzerDefinitionBuilder.builder(
                  "myCustomAz",
                  TokenizerDefinitionBuilder.NGramTokenizer.builder().minGram(2).maxGram(5).build())
              .build();

      Assert.assertSame(
          "should produce graph token stream",
          TokenStreamType.GRAPH,
          definition.getTokenStreamType());
    }

    @Test
    public void testGraphTokenizerStreamTokenFilterProducesGraph() {
      CustomAnalyzerDefinition definition =
          CustomAnalyzerDefinitionBuilder.builder(
                  "myCustomAz",
                  TokenizerDefinitionBuilder.NGramTokenizer.builder().minGram(2).maxGram(5).build())
              .tokenFilter(TokenFilterDefinitionBuilder.LowercaseTokenFilter.builder().build())
              .build();

      Assert.assertSame(
          "should produce graph token stream",
          TokenStreamType.GRAPH,
          definition.getTokenStreamType());
    }

    @Test
    public void testLimitTokenCountAddsLimitFilter() {
      CustomAnalyzerDefinition def =
          CustomAnalyzerDefinitionBuilder.builder(
                  "myAnalyzer", TokenizerDefinitionBuilder.StandardTokenizer.builder().build())
              .tokenFilter(
                  TokenFilterDefinitionBuilder
                      .LimitTokenCountTokenFilter
                      .builder()
                      .maxTokenCount(5)
                      .build())
              .build();

      Optional<List<TokenFilterDefinition>> filtersOpt = def.tokenFilters();
      Assert.assertTrue(filtersOpt.isPresent());
      Assert.assertEquals(1, filtersOpt.get().size());
      Assert.assertTrue(filtersOpt.get().get(0) instanceof LimitTokenFilterDefinition);
    }
  }
}
