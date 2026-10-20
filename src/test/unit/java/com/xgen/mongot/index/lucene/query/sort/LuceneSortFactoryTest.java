package com.xgen.mongot.index.lucene.query.sort;

import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.truth.Truth;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.explainers.SortFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.sort.common.ExplainSortField;
import com.xgen.mongot.index.lucene.query.sort.comparator.ScopeFilters;
import com.xgen.mongot.index.lucene.query.sort.mixed.MqlMixedSort;
import com.xgen.mongot.index.lucene.searcher.FieldToSortableTypesMapping;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.sort.EmbeddedSortScope;
import com.xgen.mongot.index.query.sort.MetaSortField;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.SortSelector;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.lucene.synonym.SynonymRegistryBuilder;
import com.xgen.testing.mongot.index.query.sort.SortFieldBuilder;
import com.xgen.testing.mongot.index.query.sort.SortOptionsBuilder;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.Test;

public class LuceneSortFactoryTest {

  @Test
  public void explain_scopedAndRootTypesCollide_reportsOnlyScopedTypes()
      throws InvalidQueryException {
    FieldPath price = FieldPath.parse("variants.price");
    FieldPath label = FieldPath.parse("label");
    SortSpec spec =
        SortSpecBuilder.builder()
            .sortField(scopedField("variants.price", "variants"))
            .sortField(new MongotSortField(label, UserFieldSortOptions.DEFAULT_ASC))
            .buildSort();
    var mapping =
        new FieldToSortableTypesMapping(
            ImmutableSetMultimap.of(
                price,
                FieldName.TypeField.TOKEN,
                label,
                FieldName.TypeField.TOKEN,
                FieldPath.parse("unrelated"),
                FieldName.TypeField.BOOLEAN),
            ImmutableMap.of(
                FieldPath.parse("variants"),
                ImmutableSetMultimap.of(price, FieldName.TypeField.NUMBER_INT64_V2)));

    var fieldInfos =
        explainFieldInfos(spec, mapping, Optional.empty(), Explain.Verbosity.EXECUTION_STATS);

    Truth.assertThat(fieldInfos)
        .isEqualTo(
            Map.of(
                "variants.price", List.of(FieldName.TypeField.NUMBER_INT64_V2),
                "label", List.of(FieldName.TypeField.TOKEN)));
  }

  @Test
  public void explain_emptyScopeWithRootValues_doesNotReportRootTypes()
      throws InvalidQueryException {
    SortSpec spec =
        SortSpecBuilder.builder().sortField(scopedField("variants.price", "variants")).buildSort();
    var mapping =
        new FieldToSortableTypesMapping(
            ImmutableSetMultimap.of(FieldPath.parse("variants.price"), FieldName.TypeField.TOKEN),
            ImmutableMap.of());

    var fieldInfos =
        explainFieldInfos(spec, mapping, Optional.empty(), Explain.Verbosity.QUERY_PLANNER);

    Truth.assertThat(fieldInfos).isEmpty();
  }

  @Test
  public void explain_multipleScopes_resolvesEachKeyIndependently() throws InvalidQueryException {
    FieldPath price = FieldPath.parse("variants.price");
    FieldPath nestedPrice = FieldPath.parse("variants.options.price");
    SortSpec spec =
        SortSpecBuilder.builder()
            .sortField(scopedField("variants.price", "variants"))
            .sortField(scopedField("variants.options.price", "variants.options"))
            .buildSort();
    var mapping =
        new FieldToSortableTypesMapping(
            ImmutableSetMultimap.of(nestedPrice, FieldName.TypeField.BOOLEAN),
            ImmutableMap.of(
                FieldPath.parse("variants"),
                    ImmutableSetMultimap.of(
                        price,
                        FieldName.TypeField.NUMBER_INT64_V2,
                        nestedPrice,
                        FieldName.TypeField.DATE_V2),
                FieldPath.parse("variants.options"),
                    ImmutableSetMultimap.of(
                        nestedPrice,
                        FieldName.TypeField.TOKEN,
                        nestedPrice,
                        FieldName.TypeField.NUMBER_DOUBLE_V2)));

    var fieldInfos =
        explainFieldInfos(spec, mapping, Optional.empty(), Explain.Verbosity.QUERY_PLANNER);

    Truth.assertThat(fieldInfos)
        .isEqualTo(
            Map.of(
                "variants.price", List.of(FieldName.TypeField.NUMBER_INT64_V2),
                "variants.options.price",
                    List.of(FieldName.TypeField.NUMBER_DOUBLE_V2, FieldName.TypeField.TOKEN)));
  }

  @Test
  public void explain_unscopedKeyWithReturnScope_usesQueryScope() throws InvalidQueryException {
    FieldPath price = FieldPath.parse("variants.price");
    SortSpec spec =
        SortSpecBuilder.builder()
            .sortField(new MongotSortField(price, UserFieldSortOptions.DEFAULT_ASC))
            .buildSort();
    var mapping =
        new FieldToSortableTypesMapping(
            ImmutableSetMultimap.of(price, FieldName.TypeField.TOKEN),
            ImmutableMap.of(
                FieldPath.parse("variants"),
                ImmutableSetMultimap.of(price, FieldName.TypeField.NUMBER_INT64_V2)));

    var fieldInfos =
        explainFieldInfos(
            spec,
            mapping,
            Optional.of(FieldPath.parse("variants")),
            Explain.Verbosity.EXECUTION_STATS);

    Truth.assertThat(fieldInfos)
        .isEqualTo(Map.of("variants.price", List.of(FieldName.TypeField.NUMBER_INT64_V2)));
  }

  private static MongotSortField scopedField(String path, String scope) {
    return new MongotSortField(
        FieldPath.parse(path),
        new UserFieldSortOptions(
            SortOrder.ASC,
            SortSelector.MIN,
            NullEmptySortPosition.LOWEST,
            Optional.of(
                new EmbeddedSortScope(
                    FieldPath.parse(scope),
                    new BsonDocument("exists", new BsonDocument("path", new BsonString(path)))))));
  }

  private Map<String, List<FieldName.TypeField>> explainFieldInfos(
      SortSpec spec,
      FieldToSortableTypesMapping mapping,
      Optional<FieldPath> embeddedRoot,
      Explain.Verbosity verbosity)
      throws InvalidQueryException {
    Map<FieldPath, SortField> overrides = new HashMap<>();
    var parents = new QueryBitSetProducer(new TermQuery(new Term("docType", "root")));
    var children = new QueryBitSetProducer(new TermQuery(new Term("docType", "child")));
    for (MongotSortField field : spec.getSortFields()) {
      if (field.options() instanceof UserFieldSortOptions options) {
        options
            .embeddedScope()
            .ifPresent(
                scope ->
                    overrides.put(
                        field.field(),
                        MqlMixedSort.scoped(
                            field, scope.path(), new ScopeFilters(parents, children))));
      }
    }
    try (var unused = Explain.setup(Optional.of(verbosity), Optional.of(1))) {
      new LuceneSortFactory(newQueryFactoryContext())
          .createLuceneSort(
              spec, Optional.empty(), mapping, embeddedRoot, Optional.empty(), overrides);
      var builder = SearchExplainInformationBuilder.newBuilder();
      Explain.getQueryInfo()
          .orElseThrow()
          .getFeatureExplainer(SortFeatureExplainer.class)
          .orElseThrow()
          .emitExplanation(verbosity, builder);
      return builder.build().collectStats().orElseThrow().sortStats().orElseThrow().fieldInfos();
    }
  }

  @Test
  public void testExplain() throws InvalidQueryException {
    try (var unused =
        Explain.setup(
            Optional.of(Explain.Verbosity.EXECUTION_STATS),
            Optional.of(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()))) {
      SortSpec sortSpec =
          SortSpecBuilder.builder()
              .sortField(
                  SortFieldBuilder.builder()
                      .path("foo")
                      .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                      .build())
              .sortField(
                  SortFieldBuilder.builder()
                      .path("bar")
                      .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                      .build())
              .buildSort();

      Sort sort =
          new LuceneSortFactory(newQueryFactoryContext())
              .createLuceneSort(
                  sortSpec,
                  Optional.empty(),
                  new FieldToSortableTypesMapping(
                      ImmutableSetMultimap.of(
                          FieldPath.newRoot("foo"),
                          FieldName.TypeField.NUMBER_INT64_V2,
                          FieldPath.newRoot("bar"),
                          FieldName.TypeField.TOKEN),
                      ImmutableMap.of()),
                  Optional.empty(),
                  Optional.empty());

      Arrays.stream(sort.getSort())
          .forEach(sortField -> Truth.assertThat(sortField).isInstanceOf(ExplainSortField.class));
    }
  }

  @Test
  public void testSimple() throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("foo"), FieldName.TypeField.NUMBER_INT64_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.empty());

    Truth.assertThat(sort.getSort().length).isEqualTo(1);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlLongSort.class);
  }

  @Test
  public void testMultipleDataTypesSameField() throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("foo"),
                        FieldName.TypeField.NUMBER_INT64_V2,
                        FieldPath.newRoot("foo"),
                        FieldName.TypeField.DATE_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.empty());

    Truth.assertThat(sort.getSort().length).isEqualTo(1);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlMixedSort.class);
  }

  @Test
  public void testFieldMissingValue() throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(ImmutableSetMultimap.of(), ImmutableMap.of()),
                Optional.empty(),
                Optional.empty());

    Truth.assertThat(sort.getSort().length).isEqualTo(1);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlMixedSort.class);
  }

  @Test
  public void testSortByScore() throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(
                        SortOptionsBuilder.meta()
                            .meta(MetaSortField.SEARCH_SCORE)
                            .sortOrder(SortOrder.DESC)
                            .build())
                    .build())
            .buildSort();

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("foo"),
                        FieldName.TypeField.NUMBER_INT64_V2,
                        FieldPath.newRoot("foo"),
                        FieldName.TypeField.DATE_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.empty());

    Truth.assertThat(sort.getSort().length).isEqualTo(1);
    Truth.assertThat(sort.getSort()[0].getType()).isEqualTo(SortField.Type.SCORE);
  }

  @Test
  public void testSortWithSameAfter() throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                    .build())
            .buildSort();
    BsonValue id = new BsonString("test");
    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.of(
                    SequenceToken.of(
                        id, new FieldDoc(0, 0.5f, new Object[] {new BsonString("a")}))),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(FieldPath.newRoot("foo"), FieldName.TypeField.TOKEN),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.empty());

    Truth.assertThat(sort.getSort().length).isEqualTo(1);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlSortedSetSortField.class);
  }

  @Test
  public void testSortWithDiffAfter() throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                    .build())
            .buildSort();
    BsonValue id = new BsonString("test");
    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.of(
                    SequenceToken.of(
                        id, new FieldDoc(0, 0.5f, new Object[] {new BsonString("a")}))),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("foo"), FieldName.TypeField.NUMBER_INT64_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.empty());

    Truth.assertThat(sort.getSort().length).isEqualTo(1);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlMixedSort.class);
  }

  @Test
  public void testMultiSortSpecWithMultiAfter() throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .sortField(
                SortFieldBuilder.builder()
                    .path("bar")
                    .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                    .build())
            .buildSort();
    BsonValue id = new BsonString("test");
    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.of(
                    SequenceToken.of(
                        id,
                        new FieldDoc(
                            0, 0.5f, new Object[] {new BsonInt64(5), new BsonString("a")}))),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("foo"),
                        FieldName.TypeField.NUMBER_INT64_V2,
                        FieldPath.newRoot("bar"),
                        FieldName.TypeField.TOKEN),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.empty());

    Truth.assertThat(sort.getSort().length).isEqualTo(2);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlLongSort.class);
    Truth.assertThat(sort.getSort()[1]).isInstanceOf(MqlSortedSetSortField.class);
  }

  @Test
  public void testMultiSortSpecWithFewerFieldsInSequenceToken() {
    // 2 sort fields in spec
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .sortField(
                SortFieldBuilder.builder()
                    .path("bar")
                    .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                    .build())
            .buildSort();

    // Only 1 field in SequenceToken (mismatched)
    BsonValue id = new BsonString("test");
    SequenceToken token =
        SequenceToken.of(id, new FieldDoc(0, 0.5f, new Object[] {new BsonInt64(5)}));

    // Should throw InvalidQueryException instead of ArrayIndexOutOfBoundsException
    assertThrows(
        InvalidQueryException.class,
        () ->
            new LuceneSortFactory(newQueryFactoryContext())
                .createLuceneSort(
                    sortSpec,
                    Optional.of(token),
                    new FieldToSortableTypesMapping(
                        ImmutableSetMultimap.of(
                            FieldPath.newRoot("foo"),
                            FieldName.TypeField.NUMBER_INT64_V2,
                            FieldPath.newRoot("bar"),
                            FieldName.TypeField.TOKEN),
                        ImmutableMap.of()),
                    Optional.empty(),
                    Optional.empty()));
  }

  @Test
  public void testMultiSortSpecWithMoreFieldsInSequenceToken() {
    // 1 sort field in spec
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    // 2 fields in SequenceToken (mismatched)
    BsonValue id = new BsonString("test");
    SequenceToken token =
        SequenceToken.of(
            id, new FieldDoc(0, 0.5f, new Object[] {new BsonInt64(5), new BsonString("a")}));

    assertThrows(
        InvalidQueryException.class,
        () ->
            new LuceneSortFactory(newQueryFactoryContext())
                .createLuceneSort(
                    sortSpec,
                    Optional.of(token),
                    new FieldToSortableTypesMapping(
                        ImmutableSetMultimap.of(
                            FieldPath.newRoot("foo"), FieldName.TypeField.NUMBER_INT64_V2),
                        ImmutableMap.of()),
                    Optional.empty(),
                    Optional.empty()));
  }

  @Test
  public void testEmbeddedDocumentSort() throws Exception {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo.bar")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    BsonValue id = new BsonString("test");
    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.of(
                    SequenceToken.of(id, new FieldDoc(0, 0.5f, new Object[] {new BsonInt64(5)}))),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(),
                    ImmutableMap.of(
                        FieldPath.newRoot("foo"),
                        ImmutableSetMultimap.of(
                            FieldPath.newRoot("foo.bar"), FieldName.TypeField.NUMBER_INT64_V2))),
                Optional.of(FieldPath.newRoot("foo")),
                Optional.empty());

    Truth.assertThat(sort.getSort().length).isEqualTo(1);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlLongSort.class);
  }

  @Test
  public void createLuceneSort_int64WithIndexSort_injectsNullnessField()
      throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    Sort indexSort =
        new Sort(
            new SortedNumericSortField(
                "$meta/nullness/foo", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN),
            new SortedNumericSortField(
                "$type:int64V2/foo", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN));

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("foo"), FieldName.TypeField.NUMBER_INT64_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.of(indexSort));

    Truth.assertThat(sort.getSort().length).isEqualTo(2);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlNullnessSortField.class);
    Truth.assertThat(sort.getSort()[0].getField()).isEqualTo("$meta/nullness/foo");
    Truth.assertThat(sort.getSort()[0].getMissingValue()).isEqualTo(Long.MIN_VALUE);
    Truth.assertThat(sort.getSort()[1]).isInstanceOf(MqlLongSort.class);
  }

  @Test
  public void createLuceneSort_int64WithIndexSortAndSequenceToken_expandsCorrectly()
      throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    Sort indexSort =
        new Sort(
            new SortedNumericSortField(
                "$meta/nullness/foo", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN),
            new SortedNumericSortField(
                "$type:int64V2/foo", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN));

    BsonValue id = new BsonString("test");
    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.of(
                    SequenceToken.of(
                        id,
                        new FieldDoc(0, 0.5f, new Object[] {new BsonInt64(1), new BsonInt64(42)}))),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("foo"), FieldName.TypeField.NUMBER_INT64_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.of(indexSort));

    Truth.assertThat(sort.getSort().length).isEqualTo(2);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlNullnessSortField.class);
    Truth.assertThat(sort.getSort()[0].getField()).isEqualTo("$meta/nullness/foo");
    Truth.assertThat(sort.getSort()[1]).isInstanceOf(MqlLongSort.class);
  }

  @Test
  public void createLuceneSort_int64WithIndexSortAndMismatchedSequenceToken_throws() {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    Sort indexSort =
        new Sort(
            new SortedNumericSortField(
                "$meta/nullness/foo", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN),
            new SortedNumericSortField(
                "$type:int64V2/foo", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN));

    BsonValue id = new BsonString("test");
    SequenceToken token =
        SequenceToken.of(id, new FieldDoc(0, 0.5f, new Object[] {new BsonInt64(42)}));

    assertThrows(
        InvalidQueryException.class,
        () ->
            new LuceneSortFactory(newQueryFactoryContext())
                .createLuceneSort(
                    sortSpec,
                    Optional.of(token),
                    new FieldToSortableTypesMapping(
                        ImmutableSetMultimap.of(
                            FieldPath.newRoot("foo"), FieldName.TypeField.NUMBER_INT64_V2),
                        ImmutableMap.of()),
                    Optional.empty(),
                    Optional.of(indexSort)));
  }

  @Test
  public void createLuceneSort_dateWithIndexSort_injectsNullnessField()
      throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("ts")
                    .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                    .build())
            .buildSort();

    Sort indexSort =
        new Sort(
            new SortedNumericSortField(
                "$meta/nullness/ts", SortField.Type.LONG, true, SortedNumericSelector.Type.MIN),
            new SortedNumericSortField(
                "$type:dateV2/ts", SortField.Type.LONG, true, SortedNumericSelector.Type.MIN));

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(FieldPath.newRoot("ts"), FieldName.TypeField.DATE_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.of(indexSort));

    Truth.assertThat(sort.getSort().length).isEqualTo(2);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlNullnessSortField.class);
    Truth.assertThat(sort.getSort()[0].getField()).isEqualTo("$meta/nullness/ts");
    Truth.assertThat(sort.getSort()[0].getMissingValue()).isEqualTo(Long.MIN_VALUE);
    Truth.assertThat(sort.getSort()[1]).isInstanceOf(MqlDateSort.class);
  }

  @Test
  public void createLuceneSort_int64WithoutIndexSort_noNullnessInjected()
      throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("foo"), FieldName.TypeField.NUMBER_INT64_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.empty());

    Truth.assertThat(sort.getSort().length).isEqualTo(1);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlLongSort.class);
  }

  @Test
  public void createLuceneSort_tokenWithIndexSort_noNullnessInjected()
      throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("name")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    Sort indexSort = new Sort(new SortField("$type:token/name", SortField.Type.STRING));

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(FieldPath.newRoot("name"), FieldName.TypeField.TOKEN),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.of(indexSort));

    Truth.assertThat(sort.getSort().length).isEqualTo(1);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlSortedSetSortField.class);
  }

  @Test
  public void createLuceneSort_highestNullPosition_usesMaxValueForMissing()
      throws InvalidQueryException {
    UserFieldSortOptions ascNullsHighest =
        new UserFieldSortOptions(SortOrder.ASC, NullEmptySortPosition.HIGHEST);
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(SortFieldBuilder.builder().path("foo").sortOption(ascNullsHighest).build())
            .buildSort();

    Sort indexSort =
        new Sort(
            new SortedNumericSortField(
                "$meta/nullness/foo", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN),
            new SortedNumericSortField(
                "$type:int64V2/foo", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN));

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("foo"), FieldName.TypeField.NUMBER_INT64_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.of(indexSort));

    Truth.assertThat(sort.getSort().length).isEqualTo(2);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlNullnessSortField.class);
    Truth.assertThat(sort.getSort()[0].getMissingValue()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void createLuceneSort_sequenceTokenWithNullnessExpansion_acceptsExpandedToken()
      throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    Sort indexSort =
        new Sort(
            new SortedNumericSortField(
                "$meta/nullness/foo", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN),
            new SortedNumericSortField(
                "$type:int64V2/foo", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN));

    BsonValue id = new BsonString("test");
    SequenceToken token =
        SequenceToken.of(
            id, new FieldDoc(0, 0.5f, new Object[] {new BsonInt64(0), new BsonInt64(42)}));

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.of(token),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("foo"), FieldName.TypeField.NUMBER_INT64_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.of(indexSort));

    Truth.assertThat(sort.getSort().length).isEqualTo(2);
  }

  @Test
  public void createLuceneSort_sequenceTokenMismatchWithNullness_throwsInvalidQuery() {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    Sort indexSort =
        new Sort(
            new SortedNumericSortField(
                "$meta/nullness/foo", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN),
            new SortedNumericSortField(
                "$type:int64V2/foo", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN));

    BsonValue id = new BsonString("test");
    SequenceToken token =
        SequenceToken.of(id, new FieldDoc(0, 0.5f, new Object[] {new BsonInt64(42)}));

    assertThrows(
        InvalidQueryException.class,
        () ->
            new LuceneSortFactory(newQueryFactoryContext())
                .createLuceneSort(
                    sortSpec,
                    Optional.of(token),
                    new FieldToSortableTypesMapping(
                        ImmutableSetMultimap.of(
                            FieldPath.newRoot("foo"), FieldName.TypeField.NUMBER_INT64_V2),
                        ImmutableMap.of()),
                    Optional.empty(),
                    Optional.of(indexSort)));
  }

  @Test
  public void createLuceneSort_multiFieldMixedTypes_injectsNullnessOnlyForInt64()
      throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("num")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .sortField(
                SortFieldBuilder.builder()
                    .path("name")
                    .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                    .build())
            .buildSort();

    Sort indexSort =
        new Sort(
            new SortedNumericSortField(
                "$meta/nullness/num", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN),
            new SortedNumericSortField(
                "$type:int64V2/num", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN),
            new SortField("$type:token/name", SortField.Type.STRING, true));

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("num"),
                        FieldName.TypeField.NUMBER_INT64_V2,
                        FieldPath.newRoot("name"),
                        FieldName.TypeField.TOKEN),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.of(indexSort));

    // INT64 field "num" gets $nullness + main = 2 fields, TOKEN field "name" gets 1 = total 3
    Truth.assertThat(sort.getSort().length).isEqualTo(3);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlNullnessSortField.class);
    Truth.assertThat(sort.getSort()[0].getField()).isEqualTo("$meta/nullness/num");
    Truth.assertThat(sort.getSort()[1]).isInstanceOf(MqlLongSort.class);
    Truth.assertThat(sort.getSort()[2]).isInstanceOf(MqlSortedSetSortField.class);
  }

  @Test
  public void createLuceneSort_int64WithMisalignedIndexSort_skipsNullnessExpansion()
      throws InvalidQueryException {
    // Query sort is [b], index sort starts with [$meta/nullness/a, a_typed, ...].
    // Even though the index sort also contains $meta/nullness/b further down, the
    // positional alignment fails so expansion must be skipped to avoid unnecessary
    // Pruning.NONE overhead.
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("b")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    Sort indexSort =
        new Sort(
            new SortedNumericSortField(
                "$meta/nullness/a", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN),
            new SortedNumericSortField(
                "$type:int64V2/a", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN),
            new SortedNumericSortField(
                "$meta/nullness/b", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN),
            new SortedNumericSortField(
                "$type:int64V2/b", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN));

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("b"), FieldName.TypeField.NUMBER_INT64_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.of(indexSort));

    // No nullness expansion: query sort [b] does not align with index sort prefix
    Truth.assertThat(sort.getSort().length).isEqualTo(1);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlLongSort.class);
  }

  @Test
  public void createLuceneSort_doubleWithNullTypeAndUnrelatedIndexSort_fallsBackToMixedSort()
      throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("revenue")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    Sort indexSort =
        new Sort(
            new SortedNumericSortField(
                "$type:doubleV2/otherField",
                SortField.Type.LONG,
                false,
                SortedNumericSelector.Type.MIN));

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("revenue"),
                        FieldName.TypeField.NULL,
                        FieldPath.newRoot("revenue"),
                        FieldName.TypeField.NUMBER_DOUBLE_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.of(indexSort));

    Truth.assertThat(sort.getSort().length).isEqualTo(1);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlMixedSort.class);
  }

  @Test
  public void createLuceneSort_doubleWithNullTypeNoIndexSort_fallsBackToMixedSort()
      throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("a")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("a"),
                        FieldName.TypeField.NULL,
                        FieldPath.newRoot("a"),
                        FieldName.TypeField.NUMBER_DOUBLE_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.empty());

    Truth.assertThat(sort.getSort().length).isEqualTo(1);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlMixedSort.class);
  }

  @Test
  public void createLuceneSort_int64WithNullTypeNoNullnessField_fallsBackToMixedSort()
      throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("count")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("count"),
                        FieldName.TypeField.NULL,
                        FieldPath.newRoot("count"),
                        FieldName.TypeField.NUMBER_INT64_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.empty());

    Truth.assertThat(sort.getSort().length).isEqualTo(1);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlMixedSort.class);
  }

  @Test
  public void createLuceneSort_doubleOnlyNoNull_usesOptimizedSort() throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("price")
                    .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                    .build())
            .buildSort();

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("price"), FieldName.TypeField.NUMBER_DOUBLE_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.empty());

    Truth.assertThat(sort.getSort().length).isEqualTo(1);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlDoubleSort.class);
  }

  @Test
  public void createLuceneSort_int64WithNullTypeAndNullnessField_fallsBackToMixedSort()
      throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("foo")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    Sort indexSort =
        new Sort(
            new SortedNumericSortField(
                "$meta/nullness/foo", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN),
            new SortedNumericSortField(
                "$type:int64V2/foo", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN));

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(
                        FieldPath.newRoot("foo"),
                        FieldName.TypeField.NULL,
                        FieldPath.newRoot("foo"),
                        FieldName.TypeField.NUMBER_INT64_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.of(indexSort));

    Truth.assertThat(sort.getSort().length).isEqualTo(2);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlNullnessSortField.class);
    Truth.assertThat(sort.getSort()[0].getField()).isEqualTo("$meta/nullness/foo");
    Truth.assertThat(sort.getSort()[1]).isInstanceOf(MqlMixedSort.class);
  }

  @Test
  public void createLuceneSort_dateWithIndexSort_usesOptimizedSort() throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("ts")
                    .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                    .build())
            .buildSort();

    Sort indexSort =
        new Sort(
            new SortedNumericSortField(
                "$meta/nullness/ts", SortField.Type.LONG, true, SortedNumericSelector.Type.MAX),
            new SortedNumericSortField(
                "$type:dateV2/ts", SortField.Type.LONG, true, SortedNumericSelector.Type.MAX));

    Sort sort =
        new LuceneSortFactory(newQueryFactoryContext())
            .createLuceneSort(
                sortSpec,
                Optional.empty(),
                new FieldToSortableTypesMapping(
                    ImmutableSetMultimap.of(FieldPath.newRoot("ts"), FieldName.TypeField.DATE_V2),
                    ImmutableMap.of()),
                Optional.empty(),
                Optional.of(indexSort));

    Truth.assertThat(sort.getSort().length).isEqualTo(2);
    Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlNullnessSortField.class);
    Truth.assertThat(sort.getSort()[0].getField()).isEqualTo("$meta/nullness/ts");
    Truth.assertThat(sort.getSort()[1]).isInstanceOf(MqlDateSort.class);
  }

  @Test
  public void createLuceneSort_consistentExpansionAcrossShards_withDifferentTypes()
      throws InvalidQueryException {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("age")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .buildSort();

    Sort indexSort =
        new Sort(
            new SortedNumericSortField(
                "$meta/nullness/age", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN),
            new SortedNumericSortField(
                "$type:int64V2/age", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN));

    FieldToSortableTypesMapping shardA =
        new FieldToSortableTypesMapping(
            ImmutableSetMultimap.of(FieldPath.newRoot("age"), FieldName.TypeField.NUMBER_INT64_V2),
            ImmutableMap.of());

    FieldToSortableTypesMapping shardB =
        new FieldToSortableTypesMapping(
            ImmutableSetMultimap.of(FieldPath.newRoot("age"), FieldName.TypeField.TOKEN),
            ImmutableMap.of());

    FieldToSortableTypesMapping shardC =
        new FieldToSortableTypesMapping(ImmutableSetMultimap.of(), ImmutableMap.of());

    FieldToSortableTypesMapping shardD =
        new FieldToSortableTypesMapping(
            ImmutableSetMultimap.of(FieldPath.newRoot("age"), FieldName.TypeField.NULL),
            ImmutableMap.of());

    LuceneSortFactory factory = new LuceneSortFactory(newQueryFactoryContext());

    for (FieldToSortableTypesMapping mapping : List.of(shardA, shardB, shardC, shardD)) {
      Sort sort =
          factory.createLuceneSort(
              sortSpec, Optional.empty(), mapping, Optional.empty(), Optional.of(indexSort));
      Truth.assertThat(sort.getSort().length).isEqualTo(2);
      Truth.assertThat(sort.getSort()[0]).isInstanceOf(MqlNullnessSortField.class);
      Truth.assertThat(sort.getSort()[0].getField()).isEqualTo("$meta/nullness/age");
    }
  }

  private SearchQueryFactoryContext newQueryFactoryContext() {
    var fieldDefinitionResolver =
        SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
            SearchIndex.MOCK_INDEX_DEFINITION_GENERATION.generation().formatVersion.asLucene());
    return new SearchQueryFactoryContext(
        AnalyzerRegistryBuilder.empty(),
        LuceneAnalyzer.queryAnalyzer(
            SearchIndex.MOCK_INDEX_DEFINITION, AnalyzerRegistryBuilder.empty()),
        fieldDefinitionResolver,
        SynonymRegistryBuilder.empty(),
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
        FeatureFlags.getDefault());
  }
}
