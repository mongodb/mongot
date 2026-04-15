package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnFloatQuery;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.VectorSearchFilter;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.mongot.index.definition.VectorDataFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ClauseBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.MqlFilterOperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.mql.ValueBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.bson.types.ObjectId;
import org.junit.Test;

public class DateRangeVectorQueryFactoryTest {

  private static final IndexMetricsUpdater.QueryingMetricsUpdater metrics =
      new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory());

  private static final String FILTER_PATH = "start";
  private static final String VECTOR_FIELD_NAME = "vector";

  private static final DatePoint DATE_FIRST =
      new DatePoint(
          new Calendar.Builder()
              .setDate(2019, Calendar.SEPTEMBER, 21)
              .setTimeOfDay(22, 15, 27, 37)
              .setTimeZone(TimeZone.getTimeZone("GMT"))
              .build()
              .getTime());

  private static final DatePoint DATE_SECOND =
      new DatePoint(
          new Calendar.Builder()
              .setDate(2019, Calendar.DECEMBER, 1)
              .setTimeOfDay(0, 0, 0, 1)
              .setTimeZone(TimeZone.getTimeZone("GMT"))
              .build()
              .getTime());

  @Test
  public void testDateRangeQueryLte() throws Exception {
    Query dateRange = createDateRangeQueryForBounds(Long.MIN_VALUE, DATE_FIRST.value().getTime());
    Query expected = createDateRangeVectorSearchQuery(List.of(dateRange));

    Clause filter =
        ClauseBuilder.simpleClause()
            .addOperator(
                MqlFilterOperatorBuilder.lte().value(ValueBuilder.date(DATE_FIRST)).build())
            .path(FieldPath.parse(FILTER_PATH))
            .build();

    var result = createVectorQuery(filter);
    getLuceneVectorTranslation().assertTranslatedTo(result, expected);
  }

  @Test
  public void testDateRangeQueryLt() throws Exception {

    Query dateRange =
        createDateRangeQueryForBounds(Long.MIN_VALUE, DATE_FIRST.value().getTime() - 1L);
    Query expected = createDateRangeVectorSearchQuery(List.of(dateRange));

    Clause filter =
        ClauseBuilder.simpleClause()
            .addOperator(MqlFilterOperatorBuilder.lt().value(ValueBuilder.date(DATE_FIRST)).build())
            .path(FieldPath.parse(FILTER_PATH))
            .build();

    var result = createVectorQuery(filter);
    getLuceneVectorTranslation().assertTranslatedTo(result, expected);
  }

  @Test
  public void testDateRangeQueryGte() throws Exception {
    Query dateRange = createDateRangeQueryForBounds(DATE_FIRST.value().getTime(), Long.MAX_VALUE);
    Query expected = createDateRangeVectorSearchQuery(List.of(dateRange));

    Clause filter =
        ClauseBuilder.simpleClause()
            .addOperator(
                MqlFilterOperatorBuilder.gte().value(ValueBuilder.date(DATE_FIRST)).build())
            .path(FieldPath.parse(FILTER_PATH))
            .build();

    var result = createVectorQuery(filter);
    getLuceneVectorTranslation().assertTranslatedTo(result, expected);
  }

  @Test
  public void testDateRangeQueryGt() throws Exception {

    Query dateRange =
        createDateRangeQueryForBounds(DATE_FIRST.value().getTime() + 1L, Long.MAX_VALUE);
    Query expected = createDateRangeVectorSearchQuery(List.of(dateRange));

    Clause filter =
        ClauseBuilder.simpleClause()
            .addOperator(MqlFilterOperatorBuilder.gt().value(ValueBuilder.date(DATE_FIRST)).build())
            .path(FieldPath.parse(FILTER_PATH))
            .build();

    var result = createVectorQuery(filter);
    getLuceneVectorTranslation().assertTranslatedTo(result, expected);
  }

  @Test
  public void testDateRangeQueryLtGte() throws Exception {

    Query dateRangeGte =
        createDateRangeQueryForBounds(DATE_FIRST.value().getTime(), Long.MAX_VALUE);
    Query dateRangeLt =
        createDateRangeQueryForBounds(Long.MIN_VALUE, DATE_SECOND.value().getTime() - 1L);

    Query expected = createDateRangeVectorSearchQuery(List.of(dateRangeGte, dateRangeLt));

    Clause filter =
        ClauseBuilder.simpleClause()
            .addOperator(
                MqlFilterOperatorBuilder.gte().value(ValueBuilder.date(DATE_FIRST)).build())
            .addOperator(
                MqlFilterOperatorBuilder.lt().value(ValueBuilder.date(DATE_SECOND)).build())
            .path(FieldPath.parse(FILTER_PATH))
            .build();

    var result = createVectorQuery(filter);
    getLuceneVectorTranslation().assertTranslatedTo(result, expected);
  }

  private static Query createDateRangeVectorSearchQuery(List<Query> queries) {
    var builder = new BooleanQuery.Builder();
    for (var query : queries) {
      builder.add(query, BooleanClause.Occur.MUST);
    }
    Query dateQuery = builder.build();
    return new MongotKnnFloatQuery(
        metrics,
        FeatureFlags.getDefault(),
        FieldName.TypeField.KNN_VECTOR.getLuceneFieldName(
            FieldPath.parse(VECTOR_FIELD_NAME), Optional.empty()),
        new float[] {1.0f, 2.0f},
        10,
        dateQuery);
  }

  private static Query createDateRangeQueryForBounds(long lowerBound, long upperBound) {
    return new IndexOrDocValuesQuery(
        LongPoint.newRangeQuery("$type:dateV2/" + FILTER_PATH, lowerBound, upperBound),
        SortedNumericDocValuesField.newSlowRangeQuery(
            "$type:dateV2/" + FILTER_PATH, lowerBound, upperBound));
  }

  private static VectorSearchQuery createVectorQuery(Clause filter) {
    return VectorQueryBuilder.builder()
        .index("a")
        .criteria(
            ApproximateVectorQueryCriteriaBuilder.builder()
                .path(FieldPath.parse(VECTOR_FIELD_NAME))
                .limit(10)
                .numCandidates(10)
                .queryVector(Vector.fromFloats(new float[] {1f, 2f}, NATIVE))
                .filter(new VectorSearchFilter.ClauseFilter(filter))
                .build())
        .build();
  }

  private static DynamicFeatureFlagRegistry createNumericV2EnabledRegistry() {
    ObjectId clusterId = new ObjectId();
    DynamicFeatureFlagConfig enabledConfig =
        new DynamicFeatureFlagConfig(
            DynamicFeatureFlags.NUMERIC_V2_SEMANTICS.getName(),
            DynamicFeatureFlagConfig.Phase.ENABLED,
            List.of(),
            List.of(),
            0,
            DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER);
    return new DynamicFeatureFlagRegistry(
        Optional.of(List.of(enabledConfig)),
        Optional.empty(),
        Optional.empty(),
        Optional.of(clusterId));
  }

  private LuceneVectorTranslation getLuceneVectorTranslation() {
    return new LuceneVectorTranslation(
        List.of(
            VectorDataFieldDefinitionBuilder.builder()
                .path(FieldPath.parse(VECTOR_FIELD_NAME))
                .numDimensions(2)
                .similarity(VectorSimilarity.EUCLIDEAN)
                .quantization(VectorQuantization.NONE)
                .build(),
            VectorIndexFilterFieldDefinition.create(FieldPath.parse(FILTER_PATH))),
        FeatureFlags.getDefault(),
        createNumericV2EnabledRegistry());
  }
}
