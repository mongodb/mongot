package com.xgen.mongot.embedding.utils;

import static com.xgen.mongot.index.mongodb.MaterializedViewWriter.MV_DATABASE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.mongot.index.definition.BooleanFieldDefinition;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.DynamicDefinition;
import com.xgen.mongot.index.definition.EmbeddedDocumentsFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.IllegalEmbeddedFieldException;
import com.xgen.mongot.index.definition.ObjectIdFieldDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexCapabilities;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.TokenFieldDefinition;
import com.xgen.mongot.index.definition.UuidFieldDefinition;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.analyzer.custom.TokenizerDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.definition.CustomAnalyzerDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.AutocompleteFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DateFacetFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DateFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.GeoFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.KnnVectorFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.NumericFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SortableDateBetaV1FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SortableNumberBetaV1FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SortableStringBetaV1FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFacetFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SynonymMappingDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TypeSetDefinitionBuilder;
import com.xgen.testing.mongot.index.query.sort.SortFieldBuilder;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/** Contract tests for Search auto-embed feature passthrough and MV hoisting. */
public class SearchAutoEmbedPassthroughDefinitionTest {
  private static final MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata
      SCHEMA_METADATA =
          new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
              1L,
              Map.of(FieldPath.parse("description"), FieldPath.parse("_autoEmbed.description")));
  private static final CustomAnalyzerDefinition CUSTOM_ANALYZER =
      CustomAnalyzerDefinitionBuilder.builder(
              "titleEdgeNgram",
              TokenizerDefinitionBuilder.NGramTokenizer.builder().minGram(3).maxGram(8).build())
          .build();

  @BeforeClass
  public static void registerTestModels() {
    EmbeddingModelCatalog.clear();
    EmbeddingModelCatalog.registerModelConfig(
        "voyage-3-large",
        EmbeddingModelConfig.create(
            "voyage-3-large",
            EmbeddingServiceConfig.EmbeddingProvider.VOYAGE,
            new EmbeddingServiceConfig.EmbeddingConfig(
                Optional.of("us-east-1"),
                new EmbeddingServiceConfig.VoyageModelConfig(
                    Optional.of(512),
                    Optional.of(EmbeddingServiceConfig.TruncationOption.START),
                    Optional.of(100),
                    Optional.of(1000),
                    Optional.of("text"),
                    Optional.of(VectorAutoEmbedQuantization.FLOAT),
                    Optional.empty()),
                new EmbeddingServiceConfig.ErrorHandlingConfig(50, 50L, 10L, 0.1),
                new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                    "token123", "2024-10-15T22:32:20.925Z"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                false,
                Optional.empty())));
  }

  @Test
  public void derivedDefinition_preservesAllNonAutoEmbedFeatures() throws Exception {
    SearchIndexDefinition raw = fullDefinition();
    SearchIndexDefinition derived =
        AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition(
            raw, MV_DATABASE_NAME, UUID.randomUUID(), SCHEMA_METADATA);

    assertEquals(raw.getNumPartitions(), derived.getNumPartitions());
    assertEquals(raw.getAnalyzerName(), derived.getAnalyzerName());
    assertEquals(raw.getSearchAnalyzerName(), derived.getSearchAnalyzerName());
    assertEquals(raw.getAnalyzers(), derived.getAnalyzers());
    assertEquals(raw.getSynonyms(), derived.getSynonyms());
    assertEquals(raw.getTypeSets(), derived.getTypeSets());
    assertEquals(raw.getSort(), derived.getSort());
    assertEquals(raw.getParsedIndexFeatureVersion(), derived.getParsedIndexFeatureVersion());
    assertEquals(MV_DATABASE_NAME, derived.getDatabase());

    Map<String, FieldDefinition> fields = derived.getMappings().fields();
    assertEquals(
        Optional.of("titleEdgeNgram"),
        fields.get("title").stringFieldDefinition().orElseThrow().analyzerName());
    assertNotNull(fields.get("title").autocompleteFieldDefinition().orElse(null));
    assertNotNull(fields.get("category").tokenFieldDefinition().orElse(null));
    assertNotNull(fields.get("category").stringFacetFieldDefinition().orElse(null));
    assertNotNull(fields.get("price").numberFieldDefinition().orElse(null));
    assertNotNull(fields.get("price").numberFacetFieldDefinition().orElse(null));
    assertNotNull(fields.get("price").sortableNumberBetaV1FieldDefinition().orElse(null));
    assertNotNull(fields.get("launched").dateFieldDefinition().orElse(null));
    assertNotNull(fields.get("launched").dateFacetFieldDefinition().orElse(null));
    assertNotNull(fields.get("launched").sortableDateBetaV1FieldDefinition().orElse(null));
    assertNotNull(fields.get("inStock").booleanFieldDefinition().orElse(null));
    assertNotNull(fields.get("refId").objectIdFieldDefinition().orElse(null));
    assertNotNull(fields.get("extId").uuidFieldDefinition().orElse(null));
    assertNotNull(fields.get("location").geoFieldDefinition().orElse(null));
    assertNotNull(fields.get("tags").stringFieldDefinition().orElse(null));
    assertNotNull(fields.get("specs").documentFieldDefinition().orElse(null));
    assertNotNull(fields.get("reviews").embeddedDocumentsFieldDefinition().orElse(null));
    assertNotNull(fields.get("embedding2d").knnVectorFieldDefinition().orElse(null));
    assertNotNull(fields.get("_autoEmbed").documentFieldDefinition().orElse(null));
    assertNotNull(
        fields
            .get("_autoEmbed")
            .documentFieldDefinition()
            .orElseThrow()
            .fields()
            .get("description")
            .searchIndexVectorFieldDefinition()
            .orElse(null));
  }

  @Test
  public void mappings_preservePassThroughPathsAndHoistAutoEmbed() throws Exception {
    SearchIndexDefinition raw = fullDefinition();
    AutoEmbedFieldMapping sourceMapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(raw);

    assertEquals(
        ImmutableSet.of(FieldPath.parse("description")), sourceMapping.embedFields().keySet());
    for (String path :
        new String[] {
          "title",
          "category",
          "price",
          "launched",
          "inStock",
          "refId",
          "extId",
          "location",
          "tags",
          "specs.color",
          "reviews",
          "embedding2d"
        }) {
      assertTrue(
          "missing source passthrough " + path, sourceMapping.isPassthrough(FieldPath.parse(path)));
      assertFalse(sourceMapping.isEmbed(FieldPath.parse(path)));
    }

    AutoEmbedFieldMapping matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(raw, SCHEMA_METADATA);
    assertEquals(
        ImmutableSet.of(FieldPath.parse("_autoEmbed.description")),
        matViewMapping.embedFields().keySet());
    for (String path :
        new String[] {
          "title",
          "category",
          "price",
          "launched",
          "inStock",
          "refId",
          "extId",
          "location",
          "tags",
          "specs.color",
          "reviews",
          "embedding2d"
        }) {
      assertTrue(
          "missing MV passthrough " + path, matViewMapping.isPassthrough(FieldPath.parse(path)));
    }
    assertTrue(matViewMapping.isPassthrough(FieldPath.parse("_autoEmbed._hash.description")));
    assertFalse(matViewMapping.isPassthrough(FieldPath.parse("description")));
  }

  @Test
  public void autoEmbedInsideEmbeddedDocuments_isRejectedOnUserDefinitionRoundTrip()
      throws Exception {
    var nestedAutoEmbed =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("reviews.text"));
    FieldDefinition nestedField =
        FieldDefinitionBuilder.builder().searchAutoEmbed(nestedAutoEmbed).build();
    assertThrows(
        IllegalEmbeddedFieldException.class,
        () ->
            EmbeddedDocumentsFieldDefinition.create(
                new DynamicDefinition.Boolean(false),
                Map.of("text", nestedField),
                Optional.empty()));
  }

  @Test
  public void embeddingCallContexts_exposeRequestDimensionsForCallCounting() {
    var context =
        new EmbeddingRequestContext(
            "db", "index", "collection", 1024, VectorAutoEmbedQuantization.FLOAT, false);
    assertEquals("db", context.database());
    assertEquals("index", context.indexName());
    assertEquals("collection", context.collectionName());
    assertEquals(Integer.valueOf(1024), context.outputDimension());
    assertFalse(context.isRebatchAttempt());
  }

  /**
   * Correct user-visible behavior is that the source {@code storedSource} survives derivation.
   * Disabled because {@link AutoEmbeddingIndexDefinitionUtils} currently drops it; see
   * CLOUDP-441770.
   */
  @Ignore("CLOUDP-441770: derived Search definitions drop storedSource")
  @Test
  public void derivedDefinition_preservesStoredSource() throws Exception {
    SearchIndexDefinition derived =
        AutoEmbeddingIndexDefinitionUtils.getDerivedSearchIndexDefinition(
            fullDefinition(), MV_DATABASE_NAME, UUID.randomUUID(), SCHEMA_METADATA);
    assertEquals(fullDefinition().getStoredSource(), derived.getStoredSource());
  }

  private static SearchIndexDefinition fullDefinition() throws Exception {
    var title =
        FieldDefinitionBuilder.builder()
            .string(StringFieldDefinitionBuilder.builder().analyzerName("titleEdgeNgram").build())
            .autocomplete(AutocompleteFieldDefinitionBuilder.builder().build())
            .build();
    var subtitle =
        FieldDefinitionBuilder.builder()
            .string(StringFieldDefinitionBuilder.builder().analyzerName("lucene.english").build())
            .sortableStringBetaV1(SortableStringBetaV1FieldDefinitionBuilder.builder().build())
            .build();
    var category =
        FieldDefinitionBuilder.builder()
            .token(new TokenFieldDefinition(Optional.empty()))
            .stringFacet(StringFacetFieldDefinitionBuilder.builder().build())
            .build();
    var price =
        FieldDefinitionBuilder.builder()
            .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
            .numberFacet(NumericFieldDefinitionBuilder.builder().buildNumberFacetField())
            .sortableNumberBetaV1(SortableNumberBetaV1FieldDefinitionBuilder.builder().build())
            .build();
    var launched =
        FieldDefinitionBuilder.builder()
            .date(DateFieldDefinitionBuilder.builder().build())
            .dateFacet(DateFacetFieldDefinitionBuilder.builder().build())
            .sortableDateBetaV1(SortableDateBetaV1FieldDefinitionBuilder.builder().build())
            .build();
    var specs =
        FieldDefinitionBuilder.builder()
            .document(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "color",
                        FieldDefinitionBuilder.builder()
                            .string(StringFieldDefinitionBuilder.builder().build())
                            .build())
                    .build())
            .build();
    var reviews =
        FieldDefinitionBuilder.builder()
            .embeddedDocuments(
                EmbeddedDocumentsFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "stars",
                        FieldDefinitionBuilder.builder()
                            .number(NumericFieldDefinitionBuilder.builder().buildNumberField())
                            .build())
                    .field(
                        "comment",
                        FieldDefinitionBuilder.builder()
                            .string(StringFieldDefinitionBuilder.builder().build())
                            .build())
                    .build())
            .build();
    var autoEmbed =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("description"));
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("title", title)
            .field("subtitle", subtitle)
            .field(
                "desc_embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbed).build())
            .field("category", category)
            .field("price", price)
            .field("launched", launched)
            .field(
                "inStock",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .field(
                "refId",
                FieldDefinitionBuilder.builder().objectid(new ObjectIdFieldDefinition()).build())
            .field(
                "extId", FieldDefinitionBuilder.builder().uuid(new UuidFieldDefinition()).build())
            .field(
                "location",
                FieldDefinitionBuilder.builder()
                    .geo(GeoFieldDefinitionBuilder.builder().build())
                    .build())
            .field(
                "tags",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .field("specs", specs)
            .field("reviews", reviews)
            .field(
                "embedding2d",
                FieldDefinitionBuilder.builder()
                    .knnVector(
                        KnnVectorFieldDefinitionBuilder.builder()
                            .dimensions(2)
                            .similarity(VectorSimilarity.EUCLIDEAN)
                            .build())
                    .build())
            .build();

    var sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                SortFieldBuilder.builder()
                    .path("category")
                    .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                    .build())
            .sortField(
                SortFieldBuilder.builder()
                    .path("price")
                    .sortOption(UserFieldSortOptions.DEFAULT_DESC)
                    .build())
            .buildSort();
    var typeSet =
        TypeSetDefinitionBuilder.builder()
            .name("englishStrings")
            .addType(StringFieldDefinitionBuilder.builder().analyzerName("lucene.english").build())
            .build();

    return SearchIndexDefinitionBuilder.builder()
        .defaultMetadata()
        .numPartitions(4)
        .mappings(mappings)
        .analyzerName("titleEdgeNgram")
        .searchAnalyzerName("lucene.english")
        .analyzers(java.util.List.of(CUSTOM_ANALYZER))
        .synonyms(
            SynonymMappingDefinitionBuilder.builder()
                .name("deviceSyn")
                .synonymSourceDefinition("syn_devices")
                .analyzer("lucene.english")
                .buildAsList())
        .indexFeatureVersion(SearchIndexCapabilities.CURRENT_FEATURE_VERSION)
        .storedSource(
            StoredSourceDefinition.create(
                StoredSourceDefinition.Mode.INCLUSION, java.util.List.of("title", "subtitle")))
        .typeSets(typeSet)
        .sort(sortSpec)
        .autoEmbeddingDefinitionVersion(0L)
        .build();
  }
}
