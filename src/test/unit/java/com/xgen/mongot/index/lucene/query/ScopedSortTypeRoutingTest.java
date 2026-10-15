package com.xgen.mongot.index.lucene.query;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.BooleanFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.ObjectIdFieldDefinition;
import com.xgen.mongot.index.definition.UuidFieldDefinition;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.query.sort.mixed.MqlMixedSort;
import com.xgen.mongot.index.lucene.searcher.FieldToSortableTypesMapping;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.sort.EmbeddedSortScope;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.SortSelector;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.synonym.SynonymRegistryBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.Sort;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ScopedSortTypeRoutingTest {
  private static final FieldPath PATH = FieldPath.parse("variants.val");
  private static final FieldPath SCOPE = FieldPath.parse("variants");

  private final FieldDefinition definition;
  private final List<TypeField> observedTypes;

  public ScopedSortTypeRoutingTest(
      String name, FieldDefinition definition, List<TypeField> observedTypes) {
    this.definition = definition;
    this.observedTypes = observedTypes;
  }

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> data() {
    FieldDefinition numberBoolean =
        FieldDefinitionBuilder.builder()
            .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
            .bool(new BooleanFieldDefinition())
            .build();
    FieldDefinition booleanUuid =
        FieldDefinitionBuilder.builder()
            .bool(new BooleanFieldDefinition())
            .uuid(new UuidFieldDefinition())
            .build();
    FieldDefinition uuidObjectId =
        FieldDefinitionBuilder.builder()
            .uuid(new UuidFieldDefinition())
            .objectid(new ObjectIdFieldDefinition())
            .build();
    return List.of(
        new Object[] {
          "number + boolean", numberBoolean, List.of(TypeField.NUMBER_DOUBLE_V2, TypeField.BOOLEAN)
        },
        new Object[] {"boolean + uuid", booleanUuid, List.of(TypeField.BOOLEAN, TypeField.UUID)},
        new Object[] {
          "uuid + objectId", uuidObjectId, List.of(TypeField.UUID, TypeField.OBJECT_ID)
        },
        new Object[] {"empty number + boolean", numberBoolean, List.of()},
        new Object[] {"empty boolean + uuid", booleanUuid, List.of()},
        new Object[] {
          "boolean only",
          FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build(),
          List.of(TypeField.BOOLEAN)
        },
        new Object[] {
          "uuid only",
          FieldDefinitionBuilder.builder().uuid(new UuidFieldDefinition()).build(),
          List.of(TypeField.UUID)
        },
        new Object[] {
          "objectId only",
          FieldDefinitionBuilder.builder().objectid(new ObjectIdFieldDefinition()).build(),
          List.of(TypeField.OBJECT_ID)
        });
  }

  @Test
  public void createSort_supportedSchema_routesToMixedEngine() throws Exception {
    var index =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexFeatureVersion(4)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .field(
                        "variants",
                        FieldDefinitionBuilder.builder()
                            .embeddedDocuments(
                                EmbeddedDocumentsFieldDefinitionBuilder.builder()
                                    .field("val", this.definition)
                                    .build())
                            .build())
                    .build())
            .build();
    var factory =
        LuceneSearchQueryFactoryDistributor.create(
            index,
            IndexFormatVersion.CURRENT,
            AnalyzerRegistryBuilder.empty(),
            SynonymRegistryBuilder.empty(),
            new IndexMetricsUpdater.QueryingMetricsUpdater(SearchIndex.mockMetricsFactory()),
            false,
            FeatureFlags.getDefault());
    var mapping =
        new FieldToSortableTypesMapping(
            ImmutableSetMultimap.of(),
            ImmutableMap.of(
                SCOPE,
                this.observedTypes.stream()
                    .collect(
                        ImmutableSetMultimap.toImmutableSetMultimap(type -> PATH, type -> type))));
    var options =
        new UserFieldSortOptions(
            SortOrder.ASC,
            SortSelector.MIN,
            NullEmptySortPosition.LOWEST,
            Optional.of(
                new EmbeddedSortScope(
                    SCOPE, BsonDocument.parse("{\"exists\": {\"path\": \"variants.val\"}}"))));
    var spec =
        new com.xgen.mongot.index.query.sort.Sort(
            ImmutableList.of(new MongotSortField(PATH, options)));

    try (var reader = new MultiReader()) {
      Sort sort =
          factory
              .createSort(
                  Optional.of(spec),
                  Optional.empty(),
                  mapping,
                  Optional.empty(),
                  QueryOptimizationFlags.DEFAULT_OPTIONS,
                  Optional.empty(),
                  false,
                  reader,
                  true)
              .orElseThrow();

      assertThat(sort.getSort()[0]).isInstanceOf(MqlMixedSort.class);
    }
  }
}
