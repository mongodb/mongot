package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.JsonCodec;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;

public record SearchExplainInformation(
    Optional<List<QueryExplainInformation>> query,
    Optional<CollectorExplainInformation> collectStats,
    Optional<HighlightStats> highlightStats,
    Optional<ResultMaterializationStats> resultMaterialization,
    Optional<AggregationStats> aggregation,
    Optional<MetadataExplainInformation> metadata,
    Optional<ResourceUsageOutput> resourceUsage,
    Optional<List<VectorSearchTracingSpec>> vectorSearchTracingInfo,
    Optional<List<VectorSearchSegmentStatsSpec>> vectorSearchSegmentStats,
    Optional<List<SearchExplainInformation>> indexPartitionExplainInformation,
    Optional<List<FeatureFlagEvaluationSpec>> dynamicFeatureFlags,
    Optional<List<String>> optimizerRulesFired)
    implements DocumentEncodable {

  static class Fields {
    static final Field.Optional<List<QueryExplainInformation>> QUERY =
        Field.builder("query")
            .classField(QueryExplainInformation::fromBson)
            .allowUnknownFields()
            .asSingleValueOrList()
            .optional()
            .noDefault();

    static final Field.Optional<CollectorExplainInformation> COLLECT_STATS =
        Field.builder("collectors")
            .classField(CollectorExplainInformation::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<MetadataExplainInformation> METADATA =
        Field.builder("metadata")
            .classField(MetadataExplainInformation::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<ResourceUsageOutput> RESOURCE_USAGE =
        Field.builder("resourceUsage")
            .classField(ResourceUsageOutput::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<HighlightStats> HIGHLIGHT =
        Field.builder("highlight")
            .classField(HighlightStats::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<ResultMaterializationStats> RESULT_MATERIALIZATION =
        Field.builder("resultMaterialization")
            .classField(ResultMaterializationStats::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<AggregationStats> AGGREGATION =
        Field.builder("aggregation")
            .classField(AggregationStats::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<List<VectorSearchSegmentStatsSpec>>
        VECTOR_SEARCH_LUCENE_SEGMENT_STATS =
            Field.builder("luceneVectorSegmentStats")
                .classField(VectorSearchSegmentStatsSpec::fromBson)
                .disallowUnknownFields()
                .asList()
                .mustNotBeEmpty()
                .optional()
                .noDefault();

    static final Field.Optional<List<VectorSearchTracingSpec>> VECTOR_TRACING =
        Field.builder("vectorTracing")
            .classField(VectorSearchTracingSpec::fromBson)
            .disallowUnknownFields()
            .asList()
            .mustNotBeEmpty()
            .sizeMustBeWithinBounds(Range.of(1, 100))
            .optional()
            .noDefault();

    static final Field.Optional<List<SearchExplainInformation>>
        INDEX_PARTITION_EXPLAIN_INFORMATION =
            Field.builder("indexPartitionExplain")
                .classField(SearchExplainInformation::fromBson)
                .allowUnknownFields()
                .asList()
                .mustNotBeEmpty()
                .optional()
                .noDefault();

    static final Field.Optional<List<FeatureFlagEvaluationSpec>> DYNAMIC_FEATURE_FLAGS =
        Field.builder("featureFlags")
            .classField(FeatureFlagEvaluationSpec::fromBson)
            .allowUnknownFields()
            .asList()
            .optional()
            .noDefault();

    static final Field.Optional<List<String>> OPTIMIZER_RULES_FIRED =
        Field.builder("optimizerRulesFired").stringField().asList().optional().noDefault();
  }

  public static SearchExplainInformation fromBson(DocumentParser parser) throws BsonParseException {
    return new SearchExplainInformation(
        parser.getField(Fields.QUERY).unwrap(),
        parser.getField(Fields.COLLECT_STATS).unwrap(),
        parser.getField(Fields.HIGHLIGHT).unwrap(),
        parser.getField(Fields.RESULT_MATERIALIZATION).unwrap(),
        parser.getField(Fields.AGGREGATION).unwrap(),
        parser.getField(Fields.METADATA).unwrap(),
        parser.getField(Fields.RESOURCE_USAGE).unwrap(),
        parser.getField(Fields.VECTOR_TRACING).unwrap(),
        parser.getField(Fields.VECTOR_SEARCH_LUCENE_SEGMENT_STATS).unwrap(),
        parser.getField(Fields.INDEX_PARTITION_EXPLAIN_INFORMATION).unwrap(),
        parser.getField(Fields.DYNAMIC_FEATURE_FLAGS).unwrap(),
        parser.getField(Fields.OPTIMIZER_RULES_FIRED).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    var cmp = Comparator.comparing((QueryExplainInformation sub) -> JsonCodec.toJson(sub.toBson()));

    Optional<List<QueryExplainInformation>> list =
        this.query.map(lst -> lst.stream().map(q -> q.sortedArgs(cmp)).toList());

    return BsonDocumentBuilder.builder()
        .field(Fields.QUERY, list)
        .field(Fields.COLLECT_STATS, this.collectStats)
        .field(Fields.HIGHLIGHT, this.highlightStats)
        .field(Fields.RESULT_MATERIALIZATION, this.resultMaterialization)
        .field(Fields.AGGREGATION, this.aggregation)
        .field(Fields.METADATA, this.metadata)
        .field(Fields.RESOURCE_USAGE, this.resourceUsage)
        .field(Fields.VECTOR_TRACING, this.vectorSearchTracingInfo)
        .field(Fields.VECTOR_SEARCH_LUCENE_SEGMENT_STATS, this.vectorSearchSegmentStats)
        .field(Fields.INDEX_PARTITION_EXPLAIN_INFORMATION, this.indexPartitionExplainInformation)
        .field(Fields.DYNAMIC_FEATURE_FLAGS, this.dynamicFeatureFlags)
        .field(Fields.OPTIMIZER_RULES_FIRED, this.optimizerRulesFired)
        .build();
  }

  @Override
  public String toString() {
    return JsonCodec.toJson(this);
  }
}
