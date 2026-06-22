package com.xgen.testing.mongot.integration.index.serialization;

import static com.xgen.testing.mongot.integration.index.serialization.ExpectedResultUtil.hydrateResultsIfPossible;

import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import com.xgen.testing.mongot.integration.index.serialization.variations.TestRunVariant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class ValidResult extends Result {
  public static class Fields {
    public static final Field.Optional<Map<String, ShardZoneConfig.ExplainOutput>> SHARDED_EXPLAIN =
        Field.builder("shardedExplain")
            .mapOf(
                Value.builder()
                    .classValue(ShardZoneConfig.ExplainOutput::fromBson)
                    .disallowUnknownFields()
                    .required())
            .optional()
            .noDefault();

    static final Field.Optional<SearchExplainInformation> EXPLAIN =
        Field.builder("explain")
            .classField(SearchExplainInformation::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<List<SearchExplainInformation>> INDEX_PARTITION_EXPLAIN =
        Field.builder("indexPartitionExplain")
            .classField(SearchExplainInformation::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();

    static final Field.Optional<List<ExpectedResultItem>> RESULTS =
        Field.builder("results")
            .classField(ExpectedResultItem::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();

    static final Field.Optional<List<ResultVariation>> RESULT_VARIATIONS =
        Field.builder("resultVariations")
            .classField(ResultVariation::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();

    static final Field.Optional<List<BsonDocument>> AGGREGATION_RESULTS =
        Field.builder("aggregationResults").documentField().asList().optional().noDefault();

    static final Field.WithDefault<Boolean> UNORDERED_AGGREGATION_RESULTS =
        Field.builder("unorderedAggregationResults").booleanField().optional().withDefault(false);

    static final Field.Optional<MetaResults> META =
        Field.builder("meta")
            .classField(MetaResults::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<BsonValue> INTERMEDIATE_META =
        Field.builder("intermediateMeta").unparsedValueField().optional().noDefault();
  }

  private final Optional<List<ExpectedResultItem>> searchResults;
  private final Optional<List<ResultVariation>> searchResultVariations;
  public final Optional<SearchExplainInformation> explainResult;
  public final Optional<Map<String, ShardZoneConfig.ExplainOutput>> shardedExplainResult;
  private final Optional<List<SearchExplainInformation>> indexPartitionExplainResult;
  private final Optional<MetaResults> metaResult;
  private final Optional<BsonValue> intermediateMetaResult;
  private final Optional<List<BsonDocument>> aggregationResults;
  private final boolean unorderedAggregationResults;

  public ValidResult(
      Optional<List<ExpectedResultItem>> searchResults,
      Optional<List<ResultVariation>> searchResultVariations,
      Optional<SearchExplainInformation> explainResult,
      Optional<Map<String, ShardZoneConfig.ExplainOutput>> shardedExplainResult,
      Optional<List<SearchExplainInformation>> indexPartitionExplainResult,
      Optional<MetaResults> metaResult,
      Optional<BsonValue> intermediateMetaResult,
      Optional<List<BsonDocument>> aggregationResults,
      boolean unorderedAggregationResults) {
    this.searchResults = searchResults;
    this.searchResultVariations = searchResultVariations;
    this.explainResult = explainResult;
    this.shardedExplainResult = shardedExplainResult;
    this.indexPartitionExplainResult = indexPartitionExplainResult;
    this.metaResult = metaResult;
    this.intermediateMetaResult = intermediateMetaResult;
    this.aggregationResults = aggregationResults;
    this.unorderedAggregationResults = unorderedAggregationResults;
  }

  /** Deserialize a ValidResult into results and/or explain. */
  public static ValidResult fromBson(DocumentParser parser) throws BsonParseException {
    var searchResults = parser.getField(Fields.RESULTS);
    var searchResultVariations = parser.getField(Fields.RESULT_VARIATIONS);
    var explainResult = parser.getField(Fields.EXPLAIN);
    var shardedExplainResult = parser.getField(Fields.SHARDED_EXPLAIN);
    var indexPartitionExplainResult = parser.getField(Fields.INDEX_PARTITION_EXPLAIN);
    var metaResult = parser.getField(Fields.META);
    var intermediateMetaResult = parser.getField(Fields.INTERMEDIATE_META);
    var aggregationResult = parser.getField(Fields.AGGREGATION_RESULTS);
    var unorderedAggregationResults = parser.getField(Fields.UNORDERED_AGGREGATION_RESULTS);

    // Must have at least search, explain, meta, or aggregation result.
    parser
        .getGroup()
        .atLeastOneOf(
            searchResults,
            explainResult,
            shardedExplainResult,
            indexPartitionExplainResult,
            metaResult,
            aggregationResult);

    parser.getGroup().atMostOneOf(explainResult, shardedExplainResult);

    return new ValidResult(
        searchResults.unwrap(),
        searchResultVariations.unwrap(),
        explainResult.unwrap(),
        shardedExplainResult.unwrap(),
        indexPartitionExplainResult.unwrap(),
        metaResult.unwrap(),
        intermediateMetaResult.unwrap(),
        aggregationResult.unwrap(),
        unorderedAggregationResults.unwrap());
  }

  @Override
  public Type getType() {
    return Type.VALID;
  }

  @Override
  public BsonDocument toBson() {
    BsonDocumentBuilder builder = BsonDocumentBuilder.builder().field(Result.Fields.VALID, true);

    return builder
        .field(Fields.RESULTS, this.searchResults)
        .field(Fields.EXPLAIN, this.explainResult)
        .field(Fields.SHARDED_EXPLAIN, this.shardedExplainResult)
        .field(Fields.INDEX_PARTITION_EXPLAIN, this.indexPartitionExplainResult)
        .build();
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public List<ExpectedResult> getSearchResultsFor(TestRunVariant testRunVariant) {
    if (this.searchResultVariations.isEmpty()) {
      return new ArrayList<>(getDefaultSearchResults());
    }
    Optional<ResultVariation> atMost1Match =
        this.searchResultVariations.get().stream()
            .filter(variation -> variation.getApplicableVariants().contains(testRunVariant))
            .reduce(
                (a, b) -> {
                  throw new IllegalStateException(
                      String.format(
                          "Found multiple matching variations %s and %s for %s",
                          a, b, testRunVariant));
                });

    Optional<List<ExpectedResult>> variationResultsOutlineOptional =
        atMost1Match.map(ResultVariation::getExpectedResultsOutline);
    if (variationResultsOutlineOptional.isEmpty()) {
      return new ArrayList<>(getDefaultSearchResults());
    }
    List<ExpectedResult> variationResultsOutline = variationResultsOutlineOptional.get();
    return hydrateResultsIfPossible(variationResultsOutline, getDefaultSearchResults());
  }

  public List<ExpectedResultItem> getDefaultSearchResults() {
    return Check.isPresent(this.searchResults, "searchResults");
  }

  public Optional<List<ResultVariation>> getSearchResultVariations() {
    return this.searchResultVariations;
  }

  public Optional<List<SearchExplainInformation>> getIndexPartitionExplainResult() {
    return this.indexPartitionExplainResult;
  }

  public Optional<MetaResults> getMetaResults() {
    return this.metaResult;
  }

  public Optional<BsonValue> getIntermediateMetaResults() {
    return this.intermediateMetaResult;
  }

  public List<BsonDocument> getAggregationResults() {
    Check.isPresent(this.aggregationResults, "aggregationResults");
    return this.aggregationResults.get();
  }

  public boolean getUnorderedAggregationResults() {
    return this.unorderedAggregationResults;
  }
}
