package com.xgen.mongot.index.autoembedding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddingModelCatalogTestUtils;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.TypeSetDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.sort.SortFieldBuilder;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Test;

/** Unit tests for {@link MaterializedViewIndexGenerationUtil#skipInitialSync}. */
public class MaterializedViewIndexGenerationUtilTest {

  static {
    EmbeddingModelCatalogTestUtils.registerAutoEmbedModel("voyage-3-large");
    EmbeddingModelCatalogTestUtils.registerAutoEmbedModel("voyage-3-small");
  }

  private static final ObjectId INDEX_ID = new ObjectId();

  @Test
  public void skipInitialSync_sameDefinitionDifferentOrder_returnsTrue() {
    VectorIndexFieldDefinition autoEmbedFieldDefinition =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("desc"),
            512,
            VectorSimilarity.DOT_PRODUCT,
            VectorAutoEmbedQuantization.FLOAT);
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    autoEmbedFieldDefinition,
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category")),
                    autoEmbedFieldDefinition))
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_onlyIncreaseDefinitionVersion_returnsFalse() {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(
                "voyage-3-large",
                "text",
                FieldPath.parse("desc"),
                512,
                VectorSimilarity.DOT_PRODUCT,
                VectorAutoEmbedQuantization.FLOAT),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("category")));
    VectorIndexDefinition defVersion1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(fields)
            .build();
    VectorIndexDefinition defVersion2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(2L))
            .setFields(fields)
            .build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(defVersion1, defVersion2));
  }

  @Test
  public void skipInitialSync_redefineHnswOptions_returnsTrue() {
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        512,
                        VectorSimilarity.DOT_PRODUCT,
                        VectorAutoEmbedQuantization.FLOAT),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(2L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        512,
                        VectorSimilarity.DOT_PRODUCT,
                        VectorAutoEmbedQuantization.FLOAT,
                        new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                            new VectorFieldSpecification.HnswOptions(32, 200))),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_onlyIndexingMethodDiffer_returnsTrue() {
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        512,
                        VectorSimilarity.DOT_PRODUCT,
                        VectorAutoEmbedQuantization.FLOAT),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(2L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        512,
                        VectorSimilarity.DOT_PRODUCT,
                        VectorAutoEmbedQuantization.FLOAT,
                        new VectorIndexingAlgorithm.FlatIndexingAlgorithm()),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_onlySimilarityDiffer_returnsTrue() {
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        512,
                        VectorSimilarity.DOT_PRODUCT,
                        VectorAutoEmbedQuantization.FLOAT),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(2L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        512,
                        VectorSimilarity.COSINE,
                        VectorAutoEmbedQuantization.FLOAT),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_redefineQuantization_returnsFalse() {
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        512,
                        VectorSimilarity.DOT_PRODUCT,
                        VectorAutoEmbedQuantization.FLOAT),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(2L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        512,
                        VectorSimilarity.DOT_PRODUCT,
                        VectorAutoEmbedQuantization.SCALAR),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_redefinePath_returnsFalse() {
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        512,
                        VectorSimilarity.DOT_PRODUCT,
                        VectorAutoEmbedQuantization.FLOAT),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("title"),
                        512,
                        VectorSimilarity.DOT_PRODUCT,
                        VectorAutoEmbedQuantization.FLOAT),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_redefineFilterField_returnsFalse() {
    VectorIndexFieldDefinition autoEmbedFieldDefinition =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("desc"),
            512,
            VectorSimilarity.DOT_PRODUCT,
            VectorAutoEmbedQuantization.FLOAT);
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    autoEmbedFieldDefinition,
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    autoEmbedFieldDefinition,
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("genre"))))
            .build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_onlyIncreaseNumPartitions_returnsTrue() {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(
                "voyage-3-large",
                "text",
                FieldPath.parse("desc"),
                512,
                VectorSimilarity.DOT_PRODUCT,
                VectorAutoEmbedQuantization.FLOAT),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("category")));
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .numPartitions(2)
            .setFields(fields)
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(2L))
            .numPartitions(3)
            .setFields(fields)
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_firstAutoEmbedLuceneOnlySecondQuantization_returnsFalse() {
    VectorIndexFieldDefinition filter =
        new VectorIndexFilterFieldDefinition(FieldPath.parse("category"));
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        512,
                        VectorSimilarity.DOT_PRODUCT,
                        VectorAutoEmbedQuantization.FLOAT),
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("title"),
                        512,
                        VectorSimilarity.DOT_PRODUCT,
                        VectorAutoEmbedQuantization.FLOAT),
                    filter))
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(2L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        512,
                        VectorSimilarity.DOT_PRODUCT,
                        VectorAutoEmbedQuantization.FLOAT,
                        new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                            new VectorFieldSpecification.HnswOptions(32, 200))),
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("title"),
                        512,
                        VectorSimilarity.DOT_PRODUCT,
                        VectorAutoEmbedQuantization.SCALAR),
                    filter))
            .build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_onlyIncreaseIndexFeatureVersion_returnsTrue() {
    List<VectorIndexFieldDefinition> fields =
        List.of(
            new VectorAutoEmbedFieldDefinition(
                "voyage-3-large",
                "text",
                FieldPath.parse("desc"),
                512,
                VectorSimilarity.DOT_PRODUCT,
                VectorAutoEmbedQuantization.FLOAT),
            new VectorIndexFilterFieldDefinition(FieldPath.parse("category")));
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .indexFeatureVersion(2)
            .setFields(fields)
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(2L))
            .indexFeatureVersion(3)
            .setFields(fields)
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_redefineNumDimensions_returnsFalse() {
    // Using different models with different dimensions (voyage-3-large=512, voyage-3-small=256)
    VectorIndexDefinition def1 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        512,
                        VectorSimilarity.DOT_PRODUCT,
                        VectorAutoEmbedQuantization.FLOAT),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    VectorIndexDefinition def2 =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(2L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-small",
                        "text",
                        FieldPath.parse("desc"),
                        256,
                        VectorSimilarity.DOT_PRODUCT,
                        VectorAutoEmbedQuantization.FLOAT),
                    new VectorIndexFilterFieldDefinition(FieldPath.parse("category"))))
            .build();
    // Different numDimensions (via different model) should require resync
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  // ---- Search index ----

  private static final SearchAutoEmbedFieldDefinition SEARCH_AUTO_EMBED_FIELD =
      new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("content"));

  private static SearchIndexDefinitionBuilder baseSearchBuilder(long defVersion) {
    return SearchIndexDefinitionBuilder.builder()
        .defaultMetadata()
        .indexId(INDEX_ID)
        .name("mock")
        .database("mock")
        .lastObservedCollectionName("mock")
        .collectionUuid(UUID.randomUUID())
        .definitionVersion(defVersion);
  }

  private static SearchIndexDefinitionBuilder autoEmbedSearchBuilder(
      long defVersion, boolean dynamic) {
    return baseSearchBuilder(defVersion)
        .mappings(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(dynamic)
                .field(
                    "content_embed",
                    FieldDefinitionBuilder.builder()
                        .searchAutoEmbed(SEARCH_AUTO_EMBED_FIELD)
                        .build())
                .build());
  }

  private static SearchIndexDefinition searchDefWithAutoEmbed(long defVersion, boolean dynamic) {
    return autoEmbedSearchBuilder(defVersion, dynamic).build();
  }

  private static SearchIndexDefinition searchDefWithAutoEmbedAndStringField(
      long defVersion, String analyzer) {
    return baseSearchBuilder(defVersion)
        .mappings(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    "content_embed",
                    FieldDefinitionBuilder.builder()
                        .searchAutoEmbed(SEARCH_AUTO_EMBED_FIELD)
                        .build())
                .field(
                    "title",
                    FieldDefinitionBuilder.builder()
                        .string(
                            StringFieldDefinition.create(
                                Optional.of(analyzer),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                StringFieldDefinition.IndexOptions.OFFSETS,
                                true,
                                StringFieldDefinition.NormsOptions.INCLUDE,
                                Map.of()))
                        .build())
                .build())
        .build();
  }

  private static SearchIndexDefinition searchDefWithAutoEmbedField(
      long defVersion, String mappingsKey, SearchAutoEmbedFieldDefinition autoEmbed) {
    return baseSearchBuilder(defVersion)
        .mappings(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    mappingsKey,
                    FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbed).build())
                .build())
        .build();
  }

  private static SearchAutoEmbedFieldDefinition searchAutoEmbedField(
      int numDimensions,
      VectorSimilarity similarity,
      VectorAutoEmbedQuantization quantization,
      VectorIndexingAlgorithm indexingAlgorithm) {
    return searchAutoEmbedField(
        "voyage-3-large", numDimensions, similarity, quantization, indexingAlgorithm);
  }

  private static SearchAutoEmbedFieldDefinition searchAutoEmbedField(
      String modelName,
      int numDimensions,
      VectorSimilarity similarity,
      VectorAutoEmbedQuantization quantization,
      VectorIndexingAlgorithm indexingAlgorithm) {
    return new SearchAutoEmbedFieldDefinition(
        FieldPath.parse("content"),
        new VectorAutoEmbedFieldSpecification(
            numDimensions, similarity, quantization, indexingAlgorithm, modelName, "text"));
  }

  private static SearchIndexDefinition searchDefWithNestedPassthroughFields(boolean includeWeight) {
    var specs =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "color",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build());
    if (includeWeight) {
      specs.field(
          "weight",
          FieldDefinitionBuilder.builder()
              .string(StringFieldDefinitionBuilder.builder().build())
              .build());
    }
    return baseSearchBuilder(includeWeight ? 2L : 1L)
        .mappings(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    "content_embed",
                    FieldDefinitionBuilder.builder()
                        .searchAutoEmbed(SEARCH_AUTO_EMBED_FIELD)
                        .build())
                .field("specs", FieldDefinitionBuilder.builder().document(specs.build()).build())
                .build())
        .build();
  }

  private static SearchIndexDefinition searchDefWithEmbeddedDocumentsFields(boolean includeStars) {
    var reviewsBuilder =
        EmbeddedDocumentsFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "comment",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build());
    if (includeStars) {
      reviewsBuilder.field(
          "stars",
          FieldDefinitionBuilder.builder()
              .string(StringFieldDefinitionBuilder.builder().build())
              .build());
    }
    return baseSearchBuilder(includeStars ? 2L : 1L)
        .mappings(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    "content_embed",
                    FieldDefinitionBuilder.builder()
                        .searchAutoEmbed(SEARCH_AUTO_EMBED_FIELD)
                        .build())
                .field(
                    "reviews",
                    FieldDefinitionBuilder.builder()
                        .embeddedDocuments(reviewsBuilder.build())
                        .build())
                .build())
        .build();
  }

  private static SearchIndexDefinition searchDefWithMultiTypeDocumentFields(boolean includeWeight) {
    var nestedBuilder =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "color",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build());
    if (includeWeight) {
      nestedBuilder.field(
          "weight",
          FieldDefinitionBuilder.builder()
              .string(StringFieldDefinitionBuilder.builder().build())
              .build());
    }
    return baseSearchBuilder(includeWeight ? 2L : 1L)
        .mappings(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    "content_embed",
                    FieldDefinitionBuilder.builder()
                        .searchAutoEmbed(SEARCH_AUTO_EMBED_FIELD)
                        .build())
                .field(
                    "parent",
                    FieldDefinitionBuilder.builder()
                        .document(nestedBuilder.build())
                        .string(StringFieldDefinitionBuilder.builder().build())
                        .build())
                .build())
        .build();
  }

  private static SearchIndexDefinition searchDefWithNestedDynamicDocumentFields(
      boolean includeWeight) {
    var nestedBuilder =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "color",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build());
    if (includeWeight) {
      nestedBuilder.field(
          "weight",
          FieldDefinitionBuilder.builder()
              .string(StringFieldDefinitionBuilder.builder().build())
              .build());
    }
    return baseSearchBuilder(includeWeight ? 2L : 1L)
        .mappings(
            DocumentFieldDefinitionBuilder.builder()
                .dynamic(false)
                .field(
                    "content_embed",
                    FieldDefinitionBuilder.builder()
                        .searchAutoEmbed(SEARCH_AUTO_EMBED_FIELD)
                        .build())
                .field(
                    "parent",
                    FieldDefinitionBuilder.builder().document(nestedBuilder.build()).build())
                .build())
        .build();
  }

  @Test
  public void skipInitialSync_search_sameDefinition_returnsTrue() {
    SearchIndexDefinition def1 = searchDefWithAutoEmbed(1L, false);
    SearchIndexDefinition def2 = searchDefWithAutoEmbed(1L, false);
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_autoEmbedModelChanged_returnsFalse() {
    SearchIndexDefinition def1 = searchDefWithAutoEmbed(1L, false);
    SearchIndexDefinition def2 =
        baseSearchBuilder(2L)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "content_embed",
                        FieldDefinitionBuilder.builder()
                            .searchAutoEmbed(
                                new SearchAutoEmbedFieldDefinition(
                                    "voyage-3-small", FieldPath.parse("content")))
                            .build())
                    .build())
            .build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_onlySimilarityDiffer_returnsTrue() {
    SearchIndexDefinition def1 =
        searchDefWithAutoEmbedField(
            1L,
            "content_embed",
            searchAutoEmbedField(
                512,
                VectorSimilarity.DOT_PRODUCT,
                VectorAutoEmbedQuantization.FLOAT,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));
    SearchIndexDefinition def2 =
        searchDefWithAutoEmbedField(
            2L,
            "content_embed",
            searchAutoEmbedField(
                512,
                VectorSimilarity.COSINE,
                VectorAutoEmbedQuantization.FLOAT,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_search_onlyHnswOptionsDiffer_returnsTrue() {
    SearchIndexDefinition def1 =
        searchDefWithAutoEmbedField(
            1L,
            "content_embed",
            searchAutoEmbedField(
                512,
                VectorSimilarity.DOT_PRODUCT,
                VectorAutoEmbedQuantization.FLOAT,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));
    SearchIndexDefinition def2 =
        searchDefWithAutoEmbedField(
            2L,
            "content_embed",
            searchAutoEmbedField(
                512,
                VectorSimilarity.DOT_PRODUCT,
                VectorAutoEmbedQuantization.FLOAT,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                    new VectorFieldSpecification.HnswOptions(32, 200))));

    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_search_onlyIndexingMethodDiffer_returnsTrue() {
    SearchIndexDefinition def1 =
        searchDefWithAutoEmbedField(
            1L,
            "content_embed",
            searchAutoEmbedField(
                512,
                VectorSimilarity.DOT_PRODUCT,
                VectorAutoEmbedQuantization.FLOAT,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));
    SearchIndexDefinition def2 =
        searchDefWithAutoEmbedField(
            2L,
            "content_embed",
            searchAutoEmbedField(
                512,
                VectorSimilarity.DOT_PRODUCT,
                VectorAutoEmbedQuantization.FLOAT,
                new VectorIndexingAlgorithm.FlatIndexingAlgorithm()));

    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_search_quantizationChanged_returnsFalse() {
    SearchIndexDefinition def1 =
        searchDefWithAutoEmbedField(
            1L,
            "content_embed",
            searchAutoEmbedField(
                512,
                VectorSimilarity.DOT_PRODUCT,
                VectorAutoEmbedQuantization.FLOAT,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));
    SearchIndexDefinition def2 =
        searchDefWithAutoEmbedField(
            2L,
            "content_embed",
            searchAutoEmbedField(
                512,
                VectorSimilarity.DOT_PRODUCT,
                VectorAutoEmbedQuantization.SCALAR,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_search_numDimensionsChanged_returnsFalse() {
    SearchIndexDefinition def1 =
        searchDefWithAutoEmbedField(
            1L,
            "content_embed",
            searchAutoEmbedField(
                512,
                VectorSimilarity.DOT_PRODUCT,
                VectorAutoEmbedQuantization.FLOAT,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));
    SearchIndexDefinition def2 =
        searchDefWithAutoEmbedField(
            2L,
            "content_embed",
            searchAutoEmbedField(
                256,
                VectorSimilarity.DOT_PRODUCT,
                VectorAutoEmbedQuantization.FLOAT,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_search_modelChangedWithSameDimensions_returnsFalse() {
    SearchIndexDefinition def1 =
        searchDefWithAutoEmbedField(
            1L,
            "content_embed",
            searchAutoEmbedField(
                "voyage-3-large",
                512,
                VectorSimilarity.DOT_PRODUCT,
                VectorAutoEmbedQuantization.FLOAT,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));
    SearchIndexDefinition def2 =
        searchDefWithAutoEmbedField(
            2L,
            "content_embed",
            searchAutoEmbedField(
                "voyage-3-small",
                512,
                VectorSimilarity.DOT_PRODUCT,
                VectorAutoEmbedQuantization.FLOAT,
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));

    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_search_nonAutoEmbedFieldsChanged_returnsFalse() {
    SearchIndexDefinition def1 = searchDefWithAutoEmbed(1L, false);
    SearchIndexDefinition def2 =
        baseSearchBuilder(2L)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "content_embed",
                        FieldDefinitionBuilder.builder()
                            .searchAutoEmbed(SEARCH_AUTO_EMBED_FIELD)
                            .build())
                    .field("title", FieldDefinitionBuilder.builder().build())
                    .build())
            .build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_dynamicFlipped_returnsFalse() {
    // Regression: dynamic flip with no other change must trigger resync because the MV was
    // built with the old dynamic mode and won't carry the fields the new mode expects.
    SearchIndexDefinition def1 = searchDefWithAutoEmbed(1L, false);
    SearchIndexDefinition def2 = searchDefWithAutoEmbed(2L, true);
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_dynamicDeclaredFieldChange_returnsTrue() {
    // In dynamic mode every non-embed path is passthrough, so changing only declared
    // non-auto-embed fields leaves the MV content unchanged and the generation is reused.
    SearchIndexDefinition def1 = searchDefWithAutoEmbed(1L, true);
    SearchIndexDefinition def2 =
        baseSearchBuilder(2L)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(true)
                    .field(
                        "content_embed",
                        FieldDefinitionBuilder.builder()
                            .searchAutoEmbed(SEARCH_AUTO_EMBED_FIELD)
                            .build())
                    .field("title", FieldDefinitionBuilder.builder().build())
                    .build())
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_search_dynamicAutoEmbedModelChanged_returnsFalse() {
    // The relaxed dynamic-mode comparison still resyncs when the embed fields themselves change.
    SearchIndexDefinition def1 = searchDefWithAutoEmbed(1L, true);
    SearchIndexDefinition def2 =
        baseSearchBuilder(2L)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(true)
                    .field(
                        "content_embed",
                        FieldDefinitionBuilder.builder()
                            .searchAutoEmbed(
                                new SearchAutoEmbedFieldDefinition(
                                    "voyage-3-small", FieldPath.parse("content")))
                            .build())
                    .build())
            .build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_numPartitionsOnly_returnsTrue() {
    SearchIndexDefinition def1 = searchDefWithAutoEmbed(1L, false);
    SearchIndexDefinition def2 =
        baseSearchBuilder(2L)
            .numPartitions(4)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "content_embed",
                        FieldDefinitionBuilder.builder()
                            .searchAutoEmbed(SEARCH_AUTO_EMBED_FIELD)
                            .build())
                    .build())
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_indexFeatureVersionOnly_returnsTrue() {
    SearchIndexDefinition def1 = searchDefWithAutoEmbed(1L, false);
    SearchIndexDefinition def2 =
        baseSearchBuilder(2L)
            .indexFeatureVersion(2)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "content_embed",
                        FieldDefinitionBuilder.builder()
                            .searchAutoEmbed(SEARCH_AUTO_EMBED_FIELD)
                            .build())
                    .build())
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_typeSetsOnly_returnsTrue() {
    SearchIndexDefinition def1 = searchDefWithAutoEmbed(1L, false);
    SearchIndexDefinition def2 =
        autoEmbedSearchBuilder(2L, false)
            .typeSets(
                TypeSetDefinitionBuilder.builder()
                    .name("englishStrings")
                    .addType(
                        StringFieldDefinition.create(
                            Optional.of("lucene.english"),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            StringFieldDefinition.IndexOptions.OFFSETS,
                            true,
                            StringFieldDefinition.NormsOptions.INCLUDE,
                            Map.of()))
                    .build())
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_sortOnly_returnsTrue() {
    SearchIndexDefinition def1 = searchDefWithAutoEmbed(1L, false);
    SearchIndexDefinition def2 =
        autoEmbedSearchBuilder(2L, false)
            .sort(
                SortSpecBuilder.builder()
                    .sortField(
                        SortFieldBuilder.builder()
                            .path("price")
                            .sortOption(UserFieldSortOptions.DEFAULT_ASC)
                            .build())
                    .buildSort())
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_analyzerOnly_returnsTrue() {
    SearchIndexDefinition def1 = searchDefWithAutoEmbed(1L, false);
    SearchIndexDefinition def2 =
        baseSearchBuilder(2L)
            .analyzerName("lucene.standard")
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "content_embed",
                        FieldDefinitionBuilder.builder()
                            .searchAutoEmbed(SEARCH_AUTO_EMBED_FIELD)
                            .build())
                    .build())
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_nonEmbedFieldAnalyzerOnly_returnsTrue() {
    // A non-auto-embed field's analyzer is a Lucene-only knob: it changes how the field is
    // tokenized at index time, not what the MV materializes, so the embeddings stay valid.
    SearchIndexDefinition def1 = searchDefWithAutoEmbedAndStringField(1L, "lucene.standard");
    SearchIndexDefinition def2 = searchDefWithAutoEmbedAndStringField(2L, "lucene.keyword");
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_synonymsOnly_returnsTrue() {
    // Synonyms are a query-time Lucene feature that does not change what the MV materializes,
    // so the embeddings stay valid and the generation is reused.
    SearchIndexDefinition def1 = autoEmbedSearchBuilder(1L, false).build();
    SearchIndexDefinition def2 =
        autoEmbedSearchBuilder(2L, false)
            .synonyms(SearchIndex.MOCK_SYNONYM_MAPPING_DEFINITIONS)
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_versionOnlyDiffers_returnsFalse() {
    // A definitionVersion bump with no other change forces a resync: the operator escape hatch
    // for recovering from transient errors, mirroring the vector arm. The definitions share the
    // collection UUID, as they do on the replaceGenerator path.
    UUID collectionUuid = UUID.randomUUID();
    SearchIndexDefinition def1 =
        autoEmbedSearchBuilder(1L, false).collectionUuid(collectionUuid).build();
    SearchIndexDefinition def2 =
        autoEmbedSearchBuilder(2L, false).collectionUuid(collectionUuid).build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_viewPipelineChanged_returnsFalse() {
    // The MV materializes the view's output, so a pipeline change invalidates it even when the
    // mappings are identical; an unchanged view with a Lucene-only change still reuses.
    ViewDefinition oldView =
        ViewDefinition.existing("v", List.of(BsonDocument.parse("{'$match': {'a': 1}}")));
    ViewDefinition newView =
        ViewDefinition.existing("v", List.of(BsonDocument.parse("{'$match': {'a': 2}}")));
    SearchIndexDefinition def1 = autoEmbedSearchBuilder(1L, false).view(oldView).build();
    SearchIndexDefinition def2 = autoEmbedSearchBuilder(2L, false).view(newView).build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));

    SearchIndexDefinition sameViewLuceneChange =
        autoEmbedSearchBuilder(2L, false).view(oldView).analyzerName("lucene.standard").build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, sameViewLuceneChange));
  }

  @Test
  public void skipInitialSync_search_searchAnalyzerOnly_returnsTrue() {
    SearchIndexDefinition def1 = searchDefWithAutoEmbed(1L, false);
    SearchIndexDefinition def2 =
        autoEmbedSearchBuilder(2L, false).searchAnalyzerName("lucene.standard").build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_storedSourceOnly_returnsTrue() {
    // storedSource controls which source fields are stored for retrieval, not what the MV
    // materializes, so the embeddings stay valid and the generation is reused.
    SearchIndexDefinition def1 = searchDefWithAutoEmbed(1L, false);
    SearchIndexDefinition def2 =
        autoEmbedSearchBuilder(2L, false)
            .storedSource(StoredSourceDefinition.createExcludeAll())
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_multipleLuceneParamsChanged_returnsTrue() {
    // Several Lucene-only params change at once, but the materialized-view content is unchanged.
    SearchIndexDefinition def1 = searchDefWithAutoEmbed(1L, false);
    SearchIndexDefinition def2 =
        autoEmbedSearchBuilder(2L, false)
            .numPartitions(4)
            .indexFeatureVersion(2)
            .analyzerName("lucene.standard")
            .searchAnalyzerName("lucene.keyword")
            .storedSource(StoredSourceDefinition.createExcludeAll())
            .build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_autoEmbedDestinationKeyRenamed_returnsTrue() {
    // The embed mapping is keyed by path, not the destination field name, so renaming the
    // destination field (same source and model) leaves the materialized embeddings valid.
    SearchIndexDefinition def1 =
        searchDefWithAutoEmbedField(1L, "content_embed", SEARCH_AUTO_EMBED_FIELD);
    SearchIndexDefinition def2 =
        searchDefWithAutoEmbedField(2L, "embedding", SEARCH_AUTO_EMBED_FIELD);
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_autoEmbedPathChanged_returnsFalse() {
    // Embedding a different source field changes the materialized embeddings.
    SearchIndexDefinition def1 =
        searchDefWithAutoEmbedField(1L, "content_embed", SEARCH_AUTO_EMBED_FIELD);
    SearchIndexDefinition def2 =
        searchDefWithAutoEmbedField(
            2L,
            "content_embed",
            new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("body")));
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_autoEmbedFieldAdded_returnsFalse() {
    // A second auto-embed field adds a new embedding to the materialized view.
    SearchIndexDefinition def1 = searchDefWithAutoEmbed(1L, false);
    SearchIndexDefinition def2 =
        baseSearchBuilder(2L)
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "content_embed",
                        FieldDefinitionBuilder.builder()
                            .searchAutoEmbed(SEARCH_AUTO_EMBED_FIELD)
                            .build())
                    .field(
                        "title_embed",
                        FieldDefinitionBuilder.builder()
                            .searchAutoEmbed(
                                new SearchAutoEmbedFieldDefinition(
                                    "voyage-3-large", FieldPath.parse("title")))
                            .build())
                    .build())
            .build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
  }

  @Test
  public void skipInitialSync_search_nonEmbedFieldRemoved_returnsFalse() {
    // Symmetric to nonAutoEmbedFieldsChanged: dropping a passthrough field changes the set of
    // values the materialized view carries.
    SearchIndexDefinition withField = searchDefWithAutoEmbedAndStringField(1L, "lucene.standard");
    SearchIndexDefinition withoutField = searchDefWithAutoEmbed(2L, false);
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(withField, withoutField));
  }

  @Test
  public void skipInitialSync_search_passthroughFieldAdded_sameMatViewVersion_resyncs() {
    // C1/Update-1: A new non-auto-embed field is new MV content, but it does not bump
    // autoEmbeddingDefinitionVersion. This is a resync-without-provider-re-embed, not MV reuse.
    SearchIndexDefinition def1 = searchDefWithAutoEmbed(1L, false);
    SearchIndexDefinition def2 = searchDefWithAutoEmbedAndStringField(2L, "lucene.standard");
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertEquals(
        def1.getAutoEmbeddingDefinitionVersion(), def2.getAutoEmbeddingDefinitionVersion());
  }

  @Test
  public void skipInitialSync_search_nestedPassthroughFieldAdded_returnsFalse() {
    // CLOUDP-445102: static document children must remain enumerable so adding a child changes the
    // MV-content comparison and triggers a resync.
    SearchIndexDefinition def1 = searchDefWithNestedPassthroughFields(false);
    SearchIndexDefinition def2 = searchDefWithNestedPassthroughFields(true);
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_search_embeddedDocumentsChildAdded_reusesMatView() {
    // The embeddedDocuments root is copied wholesale, so the existing MV already has the raw child.
    SearchIndexDefinition def1 = searchDefWithEmbeddedDocumentsFields(false);
    SearchIndexDefinition def2 = searchDefWithEmbeddedDocumentsFields(true);
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_search_multiTypeDocumentChildAdded_reusesMatView() {
    // A document alternative beside a scalar makes the whole field wholesale passthrough.
    SearchIndexDefinition def1 = searchDefWithMultiTypeDocumentFields(false);
    SearchIndexDefinition def2 = searchDefWithMultiTypeDocumentFields(true);
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_search_deepDeclaredChildUnderEmbedSource_resyncs() {
    // A declared child beneath an embed source is new MV content even under a dynamic root: the
    // embed-source carve-out means the value is only copied once explicitly declared.
    DocumentFieldDefinition details =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "text",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .build();
    DocumentFieldDefinition description =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("details", FieldDefinitionBuilder.builder().document(details).build())
            .build();
    DocumentFieldDefinitionBuilder mappingsBuilder =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "desc_embed",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("description")))
                    .build());
    SearchIndexDefinition def1 = baseSearchBuilder(1L).mappings(mappingsBuilder.build()).build();
    SearchIndexDefinition def2 =
        baseSearchBuilder(2L)
            .mappings(
                mappingsBuilder
                    .field(
                        "description",
                        FieldDefinitionBuilder.builder().document(description).build())
                    .build())
            .build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_search_literalDottedChildUnderEmbedSource_resyncs() {
    // A root literal dotted key ("description.text") resolves to a nested path under the embed
    // source; declaring it is new MV content and requires a resync even in dynamic mode.
    DocumentFieldDefinitionBuilder mappingsBuilder =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "description_embed",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("description")))
                    .build());
    SearchIndexDefinition def1 = baseSearchBuilder(1L).mappings(mappingsBuilder.build()).build();
    SearchIndexDefinition def2 =
        baseSearchBuilder(2L)
            .mappings(
                mappingsBuilder
                    .field(
                        "description.text",
                        FieldDefinitionBuilder.builder()
                            .string(StringFieldDefinitionBuilder.builder().build())
                            .build())
                    .build())
            .build();
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_search_nestedDynamicDocumentChildAdded_reusesMatView() {
    // Dynamic documents are copied wholesale, including undeclared children.
    SearchIndexDefinition def1 = searchDefWithNestedDynamicDocumentFields(false);
    SearchIndexDefinition def2 = searchDefWithNestedDynamicDocumentFields(true);
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def1, def2));
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(def2, def1));
  }

  @Test
  public void skipInitialSync_search_isSymmetric() {
    // The reuse/resync decision is order-independent.
    SearchIndexDefinition reuseA = searchDefWithAutoEmbed(1L, false);
    SearchIndexDefinition reuseB =
        autoEmbedSearchBuilder(2L, false).analyzerName("lucene.standard").build();
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(reuseA, reuseB));
    assertTrue(MaterializedViewIndexGenerationUtil.skipInitialSync(reuseB, reuseA));

    SearchIndexDefinition resyncA = searchDefWithAutoEmbed(1L, false);
    SearchIndexDefinition resyncB = searchDefWithAutoEmbedAndStringField(2L, "lucene.standard");
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(resyncA, resyncB));
    assertFalse(MaterializedViewIndexGenerationUtil.skipInitialSync(resyncB, resyncA));
  }

  @Test(expected = IllegalArgumentException.class)
  public void skipInitialSync_mixedTypes_throws() {
    SearchIndexDefinition searchDef = searchDefWithAutoEmbed(1L, false);
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder()
            .indexId(INDEX_ID)
            .withDefinitionVersion(Optional.of(1L))
            .setFields(
                List.of(
                    new VectorAutoEmbedFieldDefinition(
                        "voyage-3-large",
                        "text",
                        FieldPath.parse("desc"),
                        512,
                        VectorSimilarity.DOT_PRODUCT,
                        VectorAutoEmbedQuantization.FLOAT)))
            .build();
    MaterializedViewIndexGenerationUtil.skipInitialSync(searchDef, vectorDef);
  }
}
