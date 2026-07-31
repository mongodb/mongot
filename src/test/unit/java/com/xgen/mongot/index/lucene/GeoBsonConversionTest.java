package com.xgen.mongot.index.lucene;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.mongot.index.ingestion.BsonDocumentProcessor;
import com.xgen.mongot.index.lucene.document.DefaultIndexingPolicy;
import com.xgen.mongot.index.lucene.document.builder.DocumentBuilder;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.testing.BsonTestSuite;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.GeoFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.document.Document;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonValue;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Indexing geometry should either:
 *
 * <p>Have some fields indexed for them (valid cases).
 *
 * <p>Have nothing indexed for them, and no exceptions are thrown (invalid cases).
 */
@RunWith(Parameterized.class)
public class GeoBsonConversionTest {
  private final TestValidity testValidity;

  public GeoBsonConversionTest(TestValidity testValidity) {
    this.testValidity = testValidity;
  }

  @Test
  public void testGeometryConverted() throws IOException {
    var message = this.testValidity.testCase.getDescription();
    var luceneDoc = convertDocument(this.testValidity.testCase.getValue());

    var geoFields =
        luceneDoc.getFields().stream()
            .filter(e -> e.name().contains("geo"))
            .collect(Collectors.toList());

    if (this.testValidity.valid) {
      Assert.assertTrue(message, geoFields.size() > 0);
    } else {
      Assert.assertEquals(message, 0, geoFields.size());
    }
  }

  /** loads all test cases' documents. */
  @Parameterized.Parameters(name = "{0}")
  public static List<TestValidity> data() {
    BsonTestSuite testSuite =
        BsonTestSuite.load("src/test/unit/resources/index/lucene", "geoDocuments");

    return Stream.of(
            testSuite.invalid.stream().map(t -> new TestValidity(t, false)),
            testSuite.valid.stream().map(t -> new TestValidity(t, true)))
        .flatMap(s -> s)
        .collect(Collectors.toUnmodifiableList());
  }

  private Document convertDocument(BsonValue value) throws IOException {
    // field definition where "geo" field indexes geo shapes
    var indexDefinition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        "geo",
                        FieldDefinitionBuilder.builder()
                            .geo(GeoFieldDefinitionBuilder.builder().indexShapes(true).build())
                            .build())
                    .build())
            .indexFeatureVersion(2)
            .build();

    DocumentBuilder builder =
        DefaultIndexingPolicy.RootDocumentIndexingPolicy.create(
                AnalyzerRegistryBuilder.empty().getNormalizer(StockNormalizerName.NONE),
                indexDefinition.createFieldDefinitionResolver(IndexFormatVersion.CURRENT),
                new IndexMetricsUpdater.IndexingMetricsUpdater(
                    SearchIndex.mockMetricsFactory(), indexDefinition.getType()),
                FeatureFlags.getDefault())
            .createBuilder(new byte[] {14, 0, 0, 0, 16, 95, 105, 100, 0, 13, 0, 0, 0, 0});
    var bsonDocument = new BsonDocument().append("_id", new BsonDouble(0)).append("geo", value);
    BsonDocumentProcessor.process(BsonUtils.documentToRaw(bsonDocument), builder);

    return builder.build();
  }

  private static class TestValidity {
    private final BsonTestSuite.TestCase testCase;
    private final boolean valid;

    private TestValidity(BsonTestSuite.TestCase testCase, boolean valid) {
      this.testCase = testCase;
      this.valid = valid;
    }

    @Override
    public String toString() {
      return String.format("Test(desc=%s, valid=%s)", this.testCase.getDescription(), this.valid);
    }
  }
}
