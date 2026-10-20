package com.xgen.mongot.index.lucene.query;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.explain.explainers.SortFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.custom.WrappedQuery;
import com.xgen.mongot.index.lucene.searcher.FieldToSortableTypesMapping;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.sort.EmbeddedSortScope;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.SortSelector;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.synonym.SynonymRegistryBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ScopedSortExplainTest {
  private static final FieldPath SCOPE = FieldPath.parse("variants");
  private final Explain.Verbosity verbosity;

  public ScopedSortExplainTest(Explain.Verbosity verbosity) {
    this.verbosity = verbosity;
  }

  @Parameterized.Parameters(name = "verbosity={0}")
  public static Collection<Explain.Verbosity> data() {
    return Arrays.asList(Explain.Verbosity.values());
  }

  @Test
  public void createSort_independentScopes_emitsCompiledPlansAndOperatorPaths() throws Exception {
    SortSpec spec =
        SortSpecBuilder.builder()
            .sortField(
                scopedField(
                    "variants.price",
                    """
                    {"compound":{"filter":[
                      {"exists":{"path":"variants.inStock"}},
                      {"exists":{"path":"variants.price"}}
                    ]}}
                    """))
            .sortField(
                scopedField(
                    "variants.cost",
                    """
                    {"equals":{"path":"variants.inStock","value":true}}
                    """))
            .sortField(
                new MongotSortField(FieldPath.parse("rating"), UserFieldSortOptions.DEFAULT_ASC))
            .buildSort();

    BsonDocument embedded = explainSort(spec).getDocument("embeddedDocuments");

    assertThat(embedded.keySet()).containsExactly("variants.price", "variants.cost");
    BsonDocument price = embedded.getDocument("variants.price");
    assertThat(price.getString("path").getValue()).isEqualTo("variants");
    assertThat(price.containsKey("operator")).isFalse();
    BsonDocument query = price.getDocument("query");
    assertThat(query.getString("type").getValue()).isEqualTo("BooleanQuery");
    var filters = query.getDocument("args").getArray("filter");
    assertThat(filters).hasSize(2);
    assertThat(
            filters.stream()
                .map(filter -> filter.asDocument().getString("path").getValue())
                .toList())
        .containsExactly("compound.filter[0]", "compound.filter[1]");
    assertThat(query.containsKey("stats")).isFalse();
    BsonDocument costQuery = embedded.getDocument("variants.cost").getDocument("query");
    assertThat(costQuery.getString("type").getValue()).isEqualTo("ConstantScoreQuery");
    assertThat(costQuery.getDocument("args").getDocument("query").getDocument("args"))
        .isEqualTo(BsonDocument.parse("{\"path\":\"variants.inStock\",\"value\":\"T\"}"));
  }

  @Test
  public void createSort_unscoped_omitsChildPlans() throws Exception {
    SortSpec spec =
        SortSpecBuilder.builder()
            .sortField(
                new MongotSortField(FieldPath.parse("rating"), UserFieldSortOptions.DEFAULT_ASC))
            .buildSort();

    BsonDocument stats = explainSort(spec);

    assertThat(stats.containsKey("embeddedDocuments")).isFalse();
  }

  @Test
  public void childExplainQuery_usesEmbeddedRootAndMatchesOrdinaryChildQuery() throws Exception {
    var factory = factory();
    var operator = OperatorBuilder.exists().path("variants.price").build();
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      writer.addDocuments(List.of(documentAt("variants"), documentAt("other")));
      try (var reader = DirectoryReader.open(writer)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        Query ordinary = factory.createEmbeddedChildQuery(operator, SCOPE, reader);
        Query explained = factory.createEmbeddedChildExplainQuery(operator, SCOPE, reader);

        assertThat(explained).isInstanceOf(WrappedQuery.class);
        assertThat(searcher.count(ordinary)).isEqualTo(1);
        assertThat(searcher.count(explained)).isEqualTo(1);
        assertThat(searcher.search(explained, 2).scoreDocs[0].doc).isEqualTo(0);
      }
    }
  }

  private BsonDocument explainSort(SortSpec spec) throws Exception {
    try (var unused = Explain.setup(Optional.of(this.verbosity), Optional.of(1));
        var reader = new MultiReader()) {
      factory()
          .createSort(
              Optional.of(spec),
              Optional.empty(),
              new FieldToSortableTypesMapping(ImmutableSetMultimap.of(), ImmutableMap.of()),
              Optional.empty(),
              QueryOptimizationFlags.DEFAULT_OPTIONS,
              Optional.empty(),
              false,
              reader,
              true);
      var builder = SearchExplainInformationBuilder.newBuilder();
      Explain.getQueryInfo()
          .orElseThrow()
          .getFeatureExplainer(SortFeatureExplainer.class)
          .orElseThrow()
          .emitExplanation(this.verbosity, builder);
      return builder.build().collectStats().orElseThrow().sortStats().orElseThrow().toBson();
    }
  }

  private static MongotSortField scopedField(String path, String operator) {
    return new MongotSortField(
        FieldPath.parse(path),
        new UserFieldSortOptions(
            SortOrder.ASC,
            SortSelector.MIN,
            NullEmptySortPosition.LOWEST,
            Optional.of(new EmbeddedSortScope(SCOPE, BsonDocument.parse(operator)))));
  }

  private static Document documentAt(String path) {
    Document document = new Document();
    document.add(
        new StringField(
            FieldName.MetaField.EMBEDDED_PATH.getLuceneFieldName(), path, Field.Store.NO));
    document.add(
        new StringField(
            FieldName.MetaField.FIELD_NAMES.getLuceneFieldName(),
            "variants.price",
            Field.Store.NO));
    return document;
  }

  private static LuceneSearchQueryFactoryDistributor factory() {
    var index =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexFeatureVersion(4)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(true)
                    .field(
                        "variants",
                        FieldDefinitionBuilder.builder()
                            .embeddedDocuments(
                                EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                    .dynamic(true)
                                    .build())
                            .build())
                    .build())
            .build();
    return LuceneSearchQueryFactoryDistributor.create(
        index,
        IndexFormatVersion.CURRENT,
        AnalyzerRegistryBuilder.empty(),
        SynonymRegistryBuilder.empty(),
        new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
        false,
        FeatureFlags.getDefault());
  }
}
