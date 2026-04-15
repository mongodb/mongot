package com.xgen.mongot.index.lucene.query;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.mockito.Mockito.mock;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DateRangeQueryFactoryTest {

  private static final String PATH_START = "start";

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

  private static Directory directory;
  private static IndexWriter writer;

  /** set up an index. */
  @Before
  public void setUp() throws IOException {
    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    writer = new IndexWriter(directory, new IndexWriterConfig());
    writer.commit();
  }

  @After
  public void tearDown() throws IOException {
    writer.close();
    directory.close();
  }

  @Test
  public void testDateRangeQueryUpperInclusive() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .dateBounds(Optional.empty(), Optional.of(DATE_FIRST), false, true)
            .build();

    Query expected = createDateRangeQuery("start", Long.MIN_VALUE, DATE_FIRST.value().getTime());

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("date range upper inclusive", expected, result);
  }

  @Test
  public void testDateRangeQueryUpperExclusive() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .dateBounds(Optional.empty(), Optional.of(DATE_FIRST), false, false)
            .build();

    Query expected =
        createDateRangeQuery("start", Long.MIN_VALUE, DATE_FIRST.value().getTime() - 1L);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("date range upper exclusive", expected, result);
  }

  @Test
  public void testDateRangeQueryLowerInclusive() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .dateBounds(Optional.of(DATE_FIRST), Optional.empty(), true, false)
            .build();

    Query expected = createDateRangeQuery("start", DATE_FIRST.value().getTime(), Long.MAX_VALUE);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("date range lower inclusive", expected, result);
  }

  @Test
  public void testDateRangeQueryLowerExclusive() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .dateBounds(Optional.of(DATE_FIRST), Optional.empty(), false, false)
            .build();

    Query expected =
        createDateRangeQuery("start", DATE_FIRST.value().getTime() + 1L, Long.MAX_VALUE);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("date range lower exclusive", expected, result);
  }

  @Test
  public void testDateRangeQueryEmpty() {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .dateBounds(Optional.empty(), Optional.empty(), false, false)
            .build();

    Assert.assertThrows(
        IllegalStateException.class,
        () ->
            createFactory()
                .createQuery(
                    definition,
                    DirectoryReader.open(directory),
                    QueryOptimizationFlags.DEFAULT_OPTIONS));
  }

  @Test
  public void testDateRangeQueryLtGte() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .dateBounds(Optional.of(DATE_FIRST), Optional.of(DATE_SECOND), true, false)
            .build();

    Query expected =
        createDateRangeQuery(
            "start", DATE_FIRST.value().getTime(), DATE_SECOND.value().getTime() - 1L);

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("date range lower, upper, and near", expected, result);
  }

  @Test
  public void testDateRangeQueryEqualExclusiveBounds() {
    IllegalArgumentException e =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () ->
                OperatorBuilder.range()
                    .path(PATH_START)
                    .dateBounds(Optional.of(DATE_FIRST), Optional.of(DATE_FIRST), false, false)
                    .build());

    assertThat(e)
        .hasCauseThat()
        .hasMessageThat()
        .contains("bounds must both be inclusive if they are equal");
  }

  @Test
  public void testDateRangeOverflow() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .dateBounds(
                Optional.of(new DatePoint(new Date(Long.MAX_VALUE))),
                Optional.empty(),
                false,
                false)
            .build();

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);

    assertWithMessage("date range gt highest possible should return a MatchNoDocsQuery")
        .that(result)
        .isInstanceOf(MatchNoDocsQuery.class);
  }

  @Test
  public void testDateRangeUnderflow() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .dateBounds(
                Optional.empty(),
                Optional.of(new DatePoint(new Date(Long.MIN_VALUE))),
                false,
                false)
            .build();

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);

    assertWithMessage("date range lt lowest possible should return a MatchNoDocsQuery")
        .that(result)
        .isInstanceOf(MatchNoDocsQuery.class);
  }

  @Test
  public void testDateRangeQueryMultiplePathLtGte() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path("start")
            .path("end")
            .path("somethingElse")
            .dateBounds(Optional.of(DATE_FIRST), Optional.of(DATE_SECOND), true, false)
            .build();

    Query expected =
        new BooleanQuery.Builder()
            .add(
                new BooleanClause(
                    createDateRangeQuery(
                        "start", DATE_FIRST.value().getTime(), DATE_SECOND.value().getTime() - 1L),
                    BooleanClause.Occur.SHOULD))
            .add(
                new BooleanClause(
                    createDateRangeQuery(
                        "end", DATE_FIRST.value().getTime(), DATE_SECOND.value().getTime() - 1L),
                    BooleanClause.Occur.SHOULD))
            .add(
                new BooleanClause(
                    createDateRangeQuery(
                        "somethingElse",
                        DATE_FIRST.value().getTime(),
                        DATE_SECOND.value().getTime() - 1L),
                    BooleanClause.Occur.SHOULD))
            .build();

    LuceneSearchQueryFactoryDistributor factory = createFactory();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("multiple path date range lower, upper, and near", expected, result);
  }

  @Test
  public void testDateRangeQueryV1WhenFlagDisabled() throws Exception {
    RangeOperator definition =
        OperatorBuilder.range()
            .path(PATH_START)
            .dateBounds(Optional.of(DATE_FIRST), Optional.of(DATE_SECOND), true, true)
            .build();

    Query expected =
        createLegacyDateRangeQuery(
            "start", DATE_FIRST.value().getTime(), DATE_SECOND.value().getTime());

    LuceneSearchQueryFactoryDistributor factory = createFactoryWithFlagDisabled();
    Query result =
        factory.createQuery(
            definition, DirectoryReader.open(directory), QueryOptimizationFlags.DEFAULT_OPTIONS);
    Assert.assertEquals("legacy date range when NumericV2 DFF disabled", expected, result);
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

  private static LuceneSearchQueryFactoryDistributor createFactory() {
    return LuceneSearchQueryFactoryDistributor.create(
        SearchIndexDefinitionBuilder.VALID_INDEX,
        IndexFormatVersion.CURRENT,
        mock(AnalyzerRegistry.class),
        mock(SynonymRegistry.class),
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
        false,
        FeatureFlags.withDefaults().build(),
        createNumericV2EnabledRegistry());
  }

  private static LuceneSearchQueryFactoryDistributor createFactoryWithFlagDisabled() {
    return LuceneSearchQueryFactoryDistributor.create(
        SearchIndexDefinitionBuilder.VALID_INDEX,
        IndexFormatVersion.CURRENT,
        mock(AnalyzerRegistry.class),
        mock(SynonymRegistry.class),
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
        false,
        FeatureFlags.withDefaults().build(),
        DynamicFeatureFlagRegistry.empty());
  }

  private static Query createDateRangeQuery(String field, long lowerBound, long upperBound) {
    return new IndexOrDocValuesQuery(
        LongPoint.newRangeQuery("$type:dateV2/" + field, lowerBound, upperBound),
        SortedNumericDocValuesField.newSlowRangeQuery(
            "$type:dateV2/" + field, lowerBound, upperBound));
  }

  private static Query createLegacyDateRangeQuery(String field, long lowerBound, long upperBound) {
    return new ConstantScoreQuery(
        BooleanComposer.should(
            new IndexOrDocValuesQuery(
                LongPoint.newRangeQuery("$type:date/" + field, lowerBound, upperBound),
                NumericDocValuesField.newSlowRangeQuery(
                    "$type:date/" + field, lowerBound, upperBound)),
            LongPoint.newRangeQuery(
                "$type:dateMultiple/" + field, lowerBound, upperBound)));
  }
}
