package com.xgen.mongot.index.lucene.query.sort;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.truth.Truth;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.sort.common.ExplainSortField;
import com.xgen.mongot.index.lucene.query.sort.mixed.MqlMixedSort;
import com.xgen.mongot.index.lucene.searcher.FieldToSortableTypesMapping;
import com.xgen.mongot.index.query.sort.MetaSortField;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.index.query.sort.SortOrder;
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
import java.util.Optional;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.Test;

public class LuceneSortFactoryTest {

  @Test
  public void testExplain() {
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
  public void testSimple() {
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
  public void testMultipleDataTypesSameField() {
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
  public void testFieldMissingValue() {
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
  public void testSortByScore() {
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
  public void testSortWithSameAfter() {
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
  public void testSortWithDiffAfter() {
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
  public void testMultiSortSpecWithMultiAfter() {
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
                    SequenceToken.of(
                        id,
                        new FieldDoc(
                            0, 0.5f, new Object[] {new BsonInt64(5), new BsonString("a")}))),
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

  private SearchQueryFactoryContext newQueryFactoryContext() {
    var fieldDefinitionResolver =
        SearchIndex.MOCK_INDEX_DEFINITION.createFieldDefinitionResolver(
            SearchIndex.MOCK_INDEX_DEFINITION_GENERATION.generation().indexFormatVersion);
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
