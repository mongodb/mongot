package com.xgen.mongot.embedding.utils;

import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.HASH_FIELD_SUFFIX;

import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.embedding.SearchIndexAutoEmbedFieldMapping;
import com.xgen.mongot.embedding.VectorAutoEmbedFieldMapping;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.index.definition.BooleanFieldDefinition;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification;
import com.xgen.mongot.index.definition.VectorDataFieldDefinition;
import com.xgen.mongot.index.definition.VectorEmbeddedDocumentsFieldDefinition;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexingAlgorithm;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.EmbeddedDocumentsFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.StringFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AutoEmbedFieldMappingCreatorTest {

  private static final MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata
      MAT_VIEW_SCHEMA_METADATA =
          new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of());

  @Test
  public void hasSourcePassthroughFields_reflectsMapping() {
    AutoEmbedFieldMapping vectorEmbedOnly =
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(
            VectorIndexDefinitionBuilder.builder()
                .setFields(
                    List.of(
                        new VectorAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("text"))))
                .build());
    Assert.assertFalse(vectorEmbedOnly.hasSourcePassthroughFields());

    AutoEmbedFieldMapping vectorWithFilter =
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(
            VectorIndexDefinitionBuilder.builder()
                .setFields(
                    List.of(
                        new VectorAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("text")),
                        new VectorIndexFilterFieldDefinition(FieldPath.parse("tenant"))))
                .build());
    Assert.assertTrue(vectorWithFilter.hasSourcePassthroughFields());

    AutoEmbedFieldMapping searchStaticEmbedOnly =
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(
            SearchIndexDefinitionBuilder.builder()
                .defaultMetadata()
                .mappings(
                    DocumentFieldDefinitionBuilder.builder()
                        .dynamic(false)
                        .field(
                            "embedding",
                            FieldDefinitionBuilder.builder()
                                .searchAutoEmbed(
                                    new SearchAutoEmbedFieldDefinition(
                                        "voyage-3-large", FieldPath.parse("text")))
                                .build())
                        .build())
                .build());
    Assert.assertFalse(searchStaticEmbedOnly.hasSourcePassthroughFields());

    AutoEmbedFieldMapping searchStaticWithPassthrough =
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(
            SearchIndexDefinitionBuilder.builder()
                .defaultMetadata()
                .mappings(
                    DocumentFieldDefinitionBuilder.builder()
                        .dynamic(false)
                        .field(
                            "flag",
                            FieldDefinitionBuilder.builder()
                                .bool(new BooleanFieldDefinition())
                                .build())
                        .field(
                            "embedding",
                            FieldDefinitionBuilder.builder()
                                .searchAutoEmbed(
                                    new SearchAutoEmbedFieldDefinition(
                                        "voyage-3-large", FieldPath.parse("text")))
                                .build())
                        .build())
                .build());
    Assert.assertTrue(searchStaticWithPassthrough.hasSourcePassthroughFields());

    AutoEmbedFieldMapping searchDynamic =
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(
            SearchIndexDefinitionBuilder.builder()
                .defaultMetadata()
                .mappings(
                    DocumentFieldDefinitionBuilder.builder()
                        .dynamic(true)
                        .field(
                            "embedding",
                            FieldDefinitionBuilder.builder()
                                .searchAutoEmbed(
                                    new SearchAutoEmbedFieldDefinition(
                                        "voyage-3-large", FieldPath.parse("text")))
                                .build())
                        .build())
                .build());
    Assert.assertTrue(searchDynamic.hasSourcePassthroughFields());
  }

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
  public void createAutoEmbedMapping_mixedFieldTypes() {
    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("title"));
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("genre"));
    var vectorField =
        new VectorDataFieldDefinition(
            FieldPath.parse("embedding"),
            new VectorFieldSpecification(
                1024,
                VectorSimilarity.COSINE,
                VectorAutoEmbedQuantization.FLOAT.toLuceneQuantization(),
                new VectorIndexingAlgorithm.HnswIndexingAlgorithm(
                    VectorIndexingAlgorithm.HnswIndexingAlgorithm.DEFAULT_HNSW_OPTIONS)));
    var embeddedDocsField =
        new VectorEmbeddedDocumentsFieldDefinition(FieldPath.parse("chapters"), Map.of());
    List<VectorIndexFieldDefinition> fields =
        List.of(autoEmbedField, filterField, vectorField, embeddedDocsField);
    var indexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    AutoEmbedFieldMapping mapping =
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition);

    // Only AUTO_EMBED and FILTER fields should be in the mapping.
    Assert.assertEquals(2, mapping.fieldMap().size());
    Assert.assertTrue(mapping.isEmbed(FieldPath.parse("title")));
    Assert.assertTrue(mapping.isPassthrough(FieldPath.parse("genre")));
    // VECTOR and EMBEDDED_DOCUMENTS should be excluded.
    Assert.assertFalse(mapping.fieldMap().containsKey(FieldPath.parse("embedding")));
    Assert.assertFalse(mapping.fieldMap().containsKey(FieldPath.parse("chapters")));
  }

  @Test
  public void createMatViewAutoEmbedMapping_schemaVersion0_hashAsSuffix() {
    var defaultAutoEmbedField =
        new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("a"));
    var autoEmbedFieldWithSpecifications =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("b"),
            1024,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.FLOAT);
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("color"));

    List<VectorIndexFieldDefinition> fields =
        List.of(defaultAutoEmbedField, autoEmbedFieldWithSpecifications, filterField);
    var autoEmbedIndexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    var matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
            autoEmbedIndexDefinition, MAT_VIEW_SCHEMA_METADATA);

    // 2 auto-embed fields + 1 filter field + 2 hash fields (as passthrough).
    Assert.assertEquals(
        autoEmbedIndexDefinition.getFields().size() + 2, matViewMapping.fieldMap().size());
    Assert.assertTrue(
        matViewMapping.fieldMap().containsKey(FieldPath.parse("a" + HASH_FIELD_SUFFIX)));
    Assert.assertTrue(
        matViewMapping.fieldMap().containsKey(FieldPath.parse("b" + HASH_FIELD_SUFFIX)));
  }

  @Test
  public void createMatViewAutoEmbedMapping_schemaVersion1_hashAsPrefix() {
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("b.a"),
                FieldPath.parse("_autoEmbed.b.a"),
                FieldPath.parse("a"),
                FieldPath.parse("_autoEmbed.a")));
    var defaultAutoEmbedField =
        new VectorAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("a"));
    var autoEmbedFieldWithSpecifications =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("b.a"),
            1024,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.FLOAT);
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("color"));

    List<VectorIndexFieldDefinition> fields =
        List.of(defaultAutoEmbedField, autoEmbedFieldWithSpecifications, filterField);
    var autoEmbedIndexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    var matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
            autoEmbedIndexDefinition, schemaMetadata);

    // 2 auto-embed fields + 1 filter field + 2 hash fields (as passthrough).
    Assert.assertEquals(
        autoEmbedIndexDefinition.getFields().size() + 2, matViewMapping.fieldMap().size());
    Assert.assertTrue(
        matViewMapping
            .fieldMap()
            .containsKey(FieldPath.parse("_autoEmbed." + HASH_FIELD_SUFFIX + ".b.a")));
    Assert.assertTrue(
        matViewMapping
            .fieldMap()
            .containsKey(FieldPath.parse("_autoEmbed." + HASH_FIELD_SUFFIX + ".a")));
  }

  @Test
  public void getHashFieldPath_rootLevel() {
    Assert.assertEquals(
        "a" + HASH_FIELD_SUFFIX,
        AutoEmbedFieldMappingCreator.getHashFieldPath(FieldPath.parse("a"), 0).toString());
  }

  @Test
  public void getHashFieldPath_nested() {
    Assert.assertEquals(
        "a.b.c" + HASH_FIELD_SUFFIX,
        AutoEmbedFieldMappingCreator.getHashFieldPath(FieldPath.parse("a.b.c"), 0).toString());
  }

  @Test
  public void createMatViewAutoEmbedMapping_preservesHnswOptions() {
    var hnswOptions = new VectorFieldSpecification.HnswOptions(32, 200);
    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("a"),
            1024,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.FLOAT,
            new VectorIndexingAlgorithm.HnswIndexingAlgorithm(hnswOptions));
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("color"));
    List<VectorIndexFieldDefinition> fields = List.of(autoEmbedField, filterField);
    var indexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    var matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
            indexDefinition, MAT_VIEW_SCHEMA_METADATA);

    var embedSpec =
        (VectorAutoEmbedFieldSpecification)
            matViewMapping.embedFields().values().stream().findFirst().get().specification();
    Assert.assertTrue(
        embedSpec.indexingAlgorithm() instanceof VectorIndexingAlgorithm.HnswIndexingAlgorithm);
    var preservedHnsw =
        (VectorIndexingAlgorithm.HnswIndexingAlgorithm) embedSpec.indexingAlgorithm();
    Assert.assertEquals(32, preservedHnsw.options().maxEdges());
    Assert.assertEquals(200, preservedHnsw.options().numEdgeCandidates());
  }

  @Test
  public void createMatViewAutoEmbedMapping_preservesIndexingMethodFlat() {
    var autoEmbedField =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("a"),
            1024,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.FLOAT,
            new VectorIndexingAlgorithm.FlatIndexingAlgorithm());
    var filterField = new VectorIndexFilterFieldDefinition(FieldPath.parse("color"));
    List<VectorIndexFieldDefinition> fields = List.of(autoEmbedField, filterField);
    var indexDefinition = VectorIndexDefinitionBuilder.builder().setFields(fields).build();

    var matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
            indexDefinition, MAT_VIEW_SCHEMA_METADATA);

    var embedSpec =
        (VectorAutoEmbedFieldSpecification)
            matViewMapping.embedFields().values().stream().findFirst().get().specification();
    Assert.assertTrue(
        embedSpec.indexingAlgorithm() instanceof VectorIndexingAlgorithm.FlatIndexingAlgorithm);
  }

  @Test
  public void createMatViewAutoEmbedMapping_preservesNestedRootAndHashPaths() {
    var autoEmbedUnderNested =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("sections.embedding"),
            1024,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.FLOAT);
    var filterUnderNested = new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.name"));
    List<VectorIndexFieldDefinition> fields = List.of(autoEmbedUnderNested, filterUnderNested);
    var autoEmbedIndexDefinition =
        VectorIndexDefinitionBuilder.builder().setFields(fields).nestedRoot("sections").build();

    var matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(
            autoEmbedIndexDefinition, MAT_VIEW_SCHEMA_METADATA);

    FieldPath nestedAutoEmbedPath = FieldPath.parse("sections.embedding");
    FieldPath expectedHashPath = FieldPath.parse("sections.embedding" + HASH_FIELD_SUFFIX);
    Assert.assertTrue(
        "Should contain original auto-embed path",
        matViewMapping.fieldMap().containsKey(nestedAutoEmbedPath));
    Assert.assertTrue(
        "Should contain hash path for nested auto-embed field",
        matViewMapping.fieldMap().containsKey(expectedHashPath));
  }

  // ---- SearchIndexDefinition-based factory ----

  @Test
  public void createAutoEmbedMapping_search_dynamicWithOnlyAutoEmbed() {
    var autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("content"));
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);

    Assert.assertTrue(
        "Search factory should return SearchIndexAutoEmbedFieldMapping",
        mapping instanceof SearchIndexAutoEmbedFieldMapping);
    Assert.assertFalse(
        "Search factory must not return VectorAutoEmbedFieldMapping",
        mapping instanceof VectorAutoEmbedFieldMapping);

    // Embed field keyed on path()
    Assert.assertTrue(mapping.isEmbed(FieldPath.parse("content")));
    Assert.assertFalse(mapping.isPassthrough(FieldPath.parse("content")));
    // Dynamic mode → any non-embed path is passthrough
    Assert.assertTrue(mapping.isPassthrough(FieldPath.parse("anything")));
    Assert.assertTrue(mapping.passthroughDescendPredicate().test(FieldPath.parse("anything")));
  }

  @Test
  public void createAutoEmbedMapping_search_staticWithFilterField() {
    var autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("content"));
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);

    Assert.assertTrue(mapping instanceof SearchIndexAutoEmbedFieldMapping);
    Assert.assertTrue(mapping.isEmbed(FieldPath.parse("content")));
    Assert.assertTrue(
        "Statically declared non-auto-embed field must be passthrough",
        mapping.isPassthrough(FieldPath.parse("active")));
    Assert.assertFalse(
        "Undeclared paths must not be passthrough in static mode",
        mapping.isPassthrough(FieldPath.parse("random")));
    Assert.assertTrue(mapping.enumeratesAllIndexedPaths());
  }

  @Test
  public void createAutoEmbedMapping_search_nestedAutoEmbedUnderDocument() {
    // Auto-embed field nested under a `document` field must be found and embedded (keyed by
    // path), the sibling non-auto-embed field must be passthrough at its dotted path, and
    // the containing document must be descended into rather than passed through wholesale.
    var autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("content"));
    DocumentFieldDefinition nested =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("parent", FieldDefinitionBuilder.builder().document(nested).build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);

    // Embedded and keyed by path, not the "parent.embedding" declaration path.
    Assert.assertTrue(mapping.isEmbed(FieldPath.parse("content")));
    Assert.assertFalse(mapping.isEmbed(FieldPath.parse("parent.embedding")));
    // The sibling non-auto-embed field is passthrough at its full dotted path, and the containing
    // document is descended into (not wholesale passthrough).
    Assert.assertTrue(mapping.isPassthrough(FieldPath.parse("parent.active")));
    Assert.assertTrue(mapping.subDocumentExists(FieldPath.parse("parent")));
  }

  @Test
  public void createAutoEmbedMapping_search_dynamicDocumentWithAutoEmbed() {
    // A dynamic `document` containing an auto-embed declaration is passed through wholesale — its
    // undeclared children must reach the MV for the derived index's dynamic mapping — while the
    // auto-embed inside it is still collected and its source stays out of the copy.
    var autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("parent.content"));
    DocumentFieldDefinition nested =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("parent", FieldDefinitionBuilder.builder().document(nested).build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);

    Assert.assertTrue(mapping.isEmbed(FieldPath.parse("parent.content")));
    Assert.assertTrue(
        "Dynamic document is a wholesale passthrough at its own path",
        mapping.isPassthrough(FieldPath.parse("parent")));
    Assert.assertTrue(
        "Undeclared children under the dynamic document are passthrough",
        mapping.isPassthrough(FieldPath.parse("parent.color")));
    Assert.assertFalse(
        "Embed source under the dynamic document is owned by the embed pipeline",
        mapping.isPassthrough(FieldPath.parse("parent.content")));
    Assert.assertTrue(
        "MV writer must descend into the wholesale document",
        mapping.passthroughDescendPredicate().test(FieldPath.parse("parent")));
    Assert.assertFalse(
        "Declared children are covered by the wholesale root, not enumerated",
        ((SearchIndexAutoEmbedFieldMapping) mapping)
            .explicitPassthroughPaths()
            .contains(FieldPath.parse("parent.active")));
    Assert.assertFalse(mapping.enumeratesAllIndexedPaths());
  }

  @Test
  public void createAutoEmbedMapping_search_dynamicEmbeddedDocumentsIsNotEnumerable() {
    var nested = EmbeddedDocumentsFieldDefinitionBuilder.builder().dynamic(true).build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("items", FieldDefinitionBuilder.builder().embeddedDocuments(nested).build())
            .field(
                "embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("title")))
                    .build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);

    Assert.assertFalse(mapping.enumeratesAllIndexedPaths());
  }

  @Test
  public void createAutoEmbedMapping_search_dynamicAncestorOfNestedAutoEmbed() {
    // dynamic article > static meta > embed: the wholesale passthrough root is the dynamic
    // ancestor; nothing beneath it is enumerated, and the embed is still collected.
    var autoEmbedField =
        new SearchAutoEmbedFieldDefinition(
            "voyage-3-large", FieldPath.parse("article.meta.summary"));
    DocumentFieldDefinition meta =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "summary_embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .build();
    DocumentFieldDefinition article =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field("meta", FieldDefinitionBuilder.builder().document(meta).build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("article", FieldDefinitionBuilder.builder().document(article).build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);

    Assert.assertTrue(mapping.isEmbed(FieldPath.parse("article.meta.summary")));
    Assert.assertTrue(mapping.isPassthrough(FieldPath.parse("article")));
    Assert.assertTrue(
        "Undeclared fields anywhere under the dynamic ancestor are passthrough",
        mapping.isPassthrough(FieldPath.parse("article.tags")));
    Assert.assertFalse(
        "No per-field enumeration below the wholesale root",
        ((SearchIndexAutoEmbedFieldMapping) mapping)
            .explicitPassthroughPaths()
            .contains(FieldPath.parse("article.meta")));
  }

  @Test
  public void createAutoEmbedMapping_search_scalarAndDocumentAlternatives() {
    // A field declared as both a scalar and a document containing an auto-embed (search mappings
    // may declare several types per field): the scalar alternative's source value must reach the
    // MV, so the whole field is passthrough while the nested embed is still collected.
    DocumentFieldDefinition nested =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("text")))
                    .build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "parent",
                FieldDefinitionBuilder.builder()
                    .document(nested)
                    .bool(new BooleanFieldDefinition())
                    .build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);

    Assert.assertTrue(mapping.isEmbed(FieldPath.parse("text")));
    Assert.assertTrue(
        "Field with a scalar alternative is a wholesale passthrough at its own path",
        ((SearchIndexAutoEmbedFieldMapping) mapping)
            .explicitPassthroughPaths()
            .contains(FieldPath.parse("parent")));
  }

  @Test
  public void createAutoEmbedMapping_search_autoEmbedAndScalarAlternatives() {
    // autoEmbed + scalar on one field: the declaration path carries real source data for the
    // scalar alternative and must be passthrough alongside the collected embed.
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "title",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("text")))
                    .bool(new BooleanFieldDefinition())
                    .build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);

    Assert.assertTrue(mapping.isEmbed(FieldPath.parse("text")));
    Assert.assertTrue(mapping.isPassthrough(FieldPath.parse("title")));
  }

  @Test
  public void
      createAutoEmbedMapping_search_embedPathAlsoDeclaredScalar_isBothEmbedAndPassthrough() {
    // Dual role: a declared scalar field shares the embed source path, so the mapping must treat
    // the path as both an embed input and a passthrough copy for the MV.
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "description",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .field(
                "desc_embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("description")))
                    .build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);

    Assert.assertTrue(mapping.isEmbed(FieldPath.parse("description")));
    Assert.assertTrue(mapping.isPassthrough(FieldPath.parse("description")));
  }

  @Test
  public void collectAutoEmbedSourcePaths_nestedDocuments_collectsFullDottedPaths() {
    DocumentFieldDefinition parent =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "content",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .field(
                "content_embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("parent.content")))
                    .build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("parent", FieldDefinitionBuilder.builder().document(parent).build())
            .field(
                "top_embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("title")))
                    .build())
            .build();

    ImmutableSet<FieldPath> sourcePaths =
        AutoEmbedFieldMappingCreator.collectAutoEmbedSourcePaths(mappings.fields());

    Assert.assertEquals(
        Set.of(FieldPath.parse("parent.content"), FieldPath.parse("title")), sourcePaths);
  }

  @Test
  public void collectDualRoleSourcePaths_insideWholesaleSubtree_addsOnlyAutoEmbedSources() {
    // A dynamic document is copied wholesale; its explicitly declared scalar child is only
    // enumerated when it is also an auto-embed source, so the embed-path carve-out does not drop
    // its raw value while unrelated children stay covered by the wholesale root.
    DocumentFieldDefinition parent =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "content",
                FieldDefinitionBuilder.builder()
                    .string(StringFieldDefinitionBuilder.builder().build())
                    .build())
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("parent", FieldDefinitionBuilder.builder().document(parent).build())
            .field(
                "content_embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("parent.content")))
                    .build())
            .build();
    ImmutableSet<FieldPath> autoEmbedSourcePaths =
        AutoEmbedFieldMappingCreator.collectAutoEmbedSourcePaths(mappings.fields());
    ImmutableSet.Builder<FieldPath> passthroughBuilder = ImmutableSet.builder();

    AutoEmbedFieldMappingCreator.collectDualRoleSourcePaths(
        mappings.fields(), Optional.empty(), autoEmbedSourcePaths, passthroughBuilder);

    Assert.assertEquals(Set.of(FieldPath.parse("parent.content")), passthroughBuilder.build());
  }

  @Test
  public void producesSameMatViewContent_dynamicDualRoleDeclaration_returnsFalse() {
    // A dual-role declaration changes MV content even in dynamic mode: the raw value is copied for
    // the explicitly declared source path, so the mapping diff must not reuse the old MV.
    SearchIndexAutoEmbedFieldMapping plain =
        (SearchIndexAutoEmbedFieldMapping)
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(dynamicParentSearchDef(false));
    SearchIndexAutoEmbedFieldMapping dualRole =
        (SearchIndexAutoEmbedFieldMapping)
            AutoEmbedFieldMappingCreator.createAutoEmbedMapping(dynamicParentSearchDef(true));

    Assert.assertFalse(plain.producesSameMatViewContent(dualRole));
    Assert.assertFalse(dualRole.producesSameMatViewContent(plain));
  }

  /** Top-level dynamic mapping with a dynamic parent document containing an auto-embed. */
  private static SearchIndexDefinition dynamicParentSearchDef(boolean declareDualRoleScalar) {
    DocumentFieldDefinitionBuilder parentBuilder =
        DocumentFieldDefinitionBuilder.builder().dynamic(true);
    DocumentFieldDefinition parent =
        (declareDualRoleScalar
                ? parentBuilder.field(
                    "content",
                    FieldDefinitionBuilder.builder()
                        .string(StringFieldDefinitionBuilder.builder().build())
                        .build())
                : parentBuilder)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("parent.content")))
                    .build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field("parent", FieldDefinitionBuilder.builder().document(parent).build())
            .build();
    return SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();
  }

  @Test
  public void createAutoEmbedMapping_search_staticDocumentEnumeratesDeclaredChildren() {
    DocumentFieldDefinition nested =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "content_embed",
                FieldDefinitionBuilder.builder()
                    .searchAutoEmbed(
                        new SearchAutoEmbedFieldDefinition(
                            "voyage-3-large", FieldPath.parse("content")))
                    .build())
            .field("parent", FieldDefinitionBuilder.builder().document(nested).build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    SearchIndexAutoEmbedFieldMapping mapping =
        AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);

    Assert.assertTrue(
        mapping.explicitPassthroughPaths().contains(FieldPath.parse("parent.active")));
    Assert.assertFalse(mapping.explicitPassthroughPaths().contains(FieldPath.parse("parent")));
    Assert.assertTrue(mapping.isPassthrough(FieldPath.parse("parent.active")));
    Assert.assertFalse(
        "Undeclared children under a static document must stay out of the MV",
        mapping.isPassthrough(FieldPath.parse("parent.undeclared")));
    Assert.assertTrue(
        "The document ancestor remains a scalar fallback for ancestor overwrites",
        mapping.isPassthrough(FieldPath.parse("parent")));
    Assert.assertTrue(mapping.subDocumentExists(FieldPath.parse("parent")));
  }

  @Test
  public void createAutoEmbedMapping_search_embedKeyedByPathNotMappingKey() {
    // Declaration key differs from path: path must win.
    var autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("source_text"));
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "declared_key",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbedFieldMapping mapping = AutoEmbedFieldMappingCreator.createAutoEmbedMapping(searchDef);

    Assert.assertTrue(mapping.isEmbed(FieldPath.parse("source_text")));
    Assert.assertFalse(mapping.isEmbed(FieldPath.parse("declared_key")));
    // The declaration key should not leak in as a passthrough either — it describes the embed
    // output, not a source field.
    Assert.assertFalse(
        "Declaration key of an auto-embed field is not a passthrough source field",
        mapping.isPassthrough(FieldPath.parse("declared_key")));
  }

  /**
   * CLOUDP-402522: when the schema mapping remaps a filter field under the source nestedRoot, the
   * MV mapping must reflect the same MV path so that compareDocuments and downstream readers locate
   * the filter at its actual MV location instead of its (no-longer-present) source path.
   */
  @Test
  public void createMatViewAutoEmbedMapping_filterUnderNestedRoot_remappedToMvPath() {
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("sections.section_content"),
                FieldPath.parse("_autoEmbed.sections.section_content"),
                FieldPath.parse("sections.section_name"),
                FieldPath.parse("_autoEmbed.sections.section_name")));
    var autoEmbedUnderNested =
        new VectorAutoEmbedFieldDefinition(
            "voyage-3-large",
            "text",
            FieldPath.parse("sections.section_content"),
            1024,
            VectorSimilarity.COSINE,
            VectorAutoEmbedQuantization.FLOAT);
    var nestedFilter =
        new VectorIndexFilterFieldDefinition(FieldPath.parse("sections.section_name"));
    var topLevelFilter = new VectorIndexFilterFieldDefinition(FieldPath.parse("name"));
    var indexDefinition =
        VectorIndexDefinitionBuilder.builder()
            .setFields(List.of(autoEmbedUnderNested, nestedFilter, topLevelFilter))
            .nestedRoot("sections")
            .build();

    var matViewMapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(indexDefinition, schemaMetadata);

    FieldPath remappedFilterPath = FieldPath.parse("_autoEmbed.sections.section_name");
    Assert.assertTrue(
        "Filter under source nestedRoot must be remapped to the MV path.",
        matViewMapping.fieldMap().containsKey(remappedFilterPath));
    Assert.assertTrue(
        "Top-level filter must keep its source path (no schema mapping entry).",
        matViewMapping.fieldMap().containsKey(FieldPath.parse("name")));
    Assert.assertFalse(
        "Source filter path must not be present after remap.",
        matViewMapping.fieldMap().containsKey(FieldPath.parse("sections.section_name")));
  }

  @Test
  public void isFilterUnderSourceNestedRoot_descendantPath_returnsTrue() {
    Assert.assertTrue(
        AutoEmbedFieldMappingCreator.isFilterUnderSourceNestedRoot(
            FieldPath.parse("sections.section_name"), Optional.of(FieldPath.parse("sections"))));
  }

  @Test
  public void isFilterUnderSourceNestedRoot_topLevelPath_returnsFalse() {
    Assert.assertFalse(
        AutoEmbedFieldMappingCreator.isFilterUnderSourceNestedRoot(
            FieldPath.parse("name"), Optional.of(FieldPath.parse("sections"))));
  }

  @Test
  public void isFilterUnderSourceNestedRoot_noNestedRoot_returnsFalse() {
    Assert.assertFalse(
        AutoEmbedFieldMappingCreator.isFilterUnderSourceNestedRoot(
            FieldPath.parse("sections.section_name"), Optional.empty()));
  }

  @Test
  public void isFilterUnderSourceNestedRoot_pathEqualToNestedRoot_returnsFalse() {
    Assert.assertFalse(
        "isChildOf is strict; the nestedRoot itself is not a child of itself.",
        AutoEmbedFieldMappingCreator.isFilterUnderSourceNestedRoot(
            FieldPath.parse("sections"), Optional.of(FieldPath.parse("sections"))));
  }

  // ---- getMatViewNestedRoot ----

  @Test
  public void getMatViewNestedRoot_emptySource_returnsEmpty() {
    Assert.assertEquals(
        Optional.empty(),
        AutoEmbedFieldMappingCreator.getMatViewNestedRoot(
            Optional.empty(),
            Map.of(
                FieldPath.parse("sections.section_content"),
                FieldPath.parse("_autoEmbed.sections.section_content"))));
  }

  @Test
  public void getMatViewNestedRoot_emptyMapping_returnsSourceUnchanged() {
    // An empty mapping (no field-path remapping, e.g. MV schema version 0) should leave
    // nestedRoot unchanged.
    Optional<FieldPath> nestedRoot = Optional.of(FieldPath.parse("sections"));
    Assert.assertEquals(
        nestedRoot, AutoEmbedFieldMappingCreator.getMatViewNestedRoot(nestedRoot, Map.of()));
  }

  @Test
  public void getMatViewNestedRoot_mappingContainsOnlyFieldsOutsideNestedRoot_returnsUnchanged() {
    Optional<FieldPath> nestedRoot = Optional.of(FieldPath.parse("sections"));
    Assert.assertEquals(
        nestedRoot,
        AutoEmbedFieldMappingCreator.getMatViewNestedRoot(
            nestedRoot, Map.of(FieldPath.parse("title"), FieldPath.parse("_autoEmbed.title"))));
  }

  @Test
  public void getMatViewNestedRoot_directChildField_stripsOneSegment() {
    Optional<FieldPath> nestedRoot = Optional.of(FieldPath.parse("sections"));
    Assert.assertEquals(
        Optional.of(FieldPath.parse("_autoEmbed.sections")),
        AutoEmbedFieldMappingCreator.getMatViewNestedRoot(
            nestedRoot,
            Map.of(
                FieldPath.parse("sections.section_content"),
                FieldPath.parse("_autoEmbed.sections.section_content"))));
  }

  @Test
  public void getMatViewNestedRoot_grandchildField_stripsMultipleSegments() {
    Optional<FieldPath> nestedRoot = Optional.of(FieldPath.parse("sections"));
    Assert.assertEquals(
        Optional.of(FieldPath.parse("_autoEmbed.sections")),
        AutoEmbedFieldMappingCreator.getMatViewNestedRoot(
            nestedRoot,
            Map.of(
                FieldPath.parse("sections.paragraphs.vector"),
                FieldPath.parse("_autoEmbed.sections.paragraphs.vector"))));
  }

  @Test
  public void getMatViewNestedRoot_multiLevelNestedRoot_remapsConsistently() {
    Optional<FieldPath> nestedRoot = Optional.of(FieldPath.parse("sections.paragraphs"));
    Assert.assertEquals(
        Optional.of(FieldPath.parse("_autoEmbed.sections.paragraphs")),
        AutoEmbedFieldMappingCreator.getMatViewNestedRoot(
            nestedRoot,
            Map.of(
                FieldPath.parse("sections.paragraphs.vector"),
                FieldPath.parse("_autoEmbed.sections.paragraphs.vector"))));
  }

  @Test
  public void getMatViewNestedRoot_multipleConsistentFieldsUnderRoot_returnsSharedPrefix() {
    // Multiple auto-embed fields under the same nestedRoot sharing the same MV prefix (the
    // expected in-production shape) should resolve to that shared prefix regardless of which
    // mapping entry is inspected first.
    Optional<FieldPath> nestedRoot = Optional.of(FieldPath.parse("sections"));
    Map<FieldPath, FieldPath> mapping =
        Map.of(
            FieldPath.parse("sections.section_content"),
            FieldPath.parse("_autoEmbed.sections.section_content"),
            FieldPath.parse("sections.section_title"),
            FieldPath.parse("_autoEmbed.sections.section_title"));
    Assert.assertEquals(
        Optional.of(FieldPath.parse("_autoEmbed.sections")),
        AutoEmbedFieldMappingCreator.getMatViewNestedRoot(nestedRoot, mapping));
  }

  @Test
  public void getMatViewNestedRoot_inconsistentPrefixesUnderRoot_picksLexicographicallySmallest() {
    // Invariant violation: two auto-embed fields under the same source nestedRoot produce
    // different derived roots. The method logs a warning but still returns a deterministic result
    // (the lexicographically smallest derived root) so index derivation does not fail and the
    // result is reproducible regardless of map iteration order.
    Optional<FieldPath> nestedRoot = Optional.of(FieldPath.parse("sections"));
    Map<FieldPath, FieldPath> mapping =
        Map.of(
            FieldPath.parse("sections.section_content"),
            FieldPath.parse("_autoEmbed.sections.section_content"),
            FieldPath.parse("sections.section_title"),
            FieldPath.parse("_otherPrefix.sections.section_title"));

    Optional<FieldPath> result =
        AutoEmbedFieldMappingCreator.getMatViewNestedRoot(nestedRoot, mapping);

    // "_autoEmbed.sections" < "_otherPrefix.sections" lexicographically, so it is always picked.
    Assert.assertEquals(Optional.of(FieldPath.parse("_autoEmbed.sections")), result);
  }

  @Test
  public void createMatViewAutoEmbedMapping_search_static_rewritesEmbedAndAddsHashPassthrough() {
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1, Map.of(FieldPath.parse("content"), FieldPath.parse("_autoEmbed.content")));
    var autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("content"));
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbedFieldMapping mapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(searchDef, schemaMetadata);

    Assert.assertTrue(
        "Search factory should return SearchIndexAutoEmbedFieldMapping",
        mapping instanceof SearchIndexAutoEmbedFieldMapping);

    FieldPath matViewEmbedPath = FieldPath.parse("_autoEmbed.content");
    FieldPath hashPath = FieldPath.parse("_autoEmbed." + HASH_FIELD_SUFFIX + ".content");

    Assert.assertTrue(
        "Embed field should be keyed at the MV-rewritten path",
        mapping.embedFields().containsKey(matViewEmbedPath));
    Assert.assertFalse(
        "Source path should not appear as an embed key on the MV side",
        mapping.embedFields().containsKey(FieldPath.parse("content")));
    Assert.assertTrue("Hash path should be a passthrough", mapping.isPassthrough(hashPath));
    Assert.assertTrue(
        "Declared non-auto-embed field should remain passthrough at its original path",
        mapping.isPassthrough(FieldPath.parse("active")));
  }

  @Test
  public void createMatViewAutoEmbedMapping_search_dynamicFlagPreserved() {
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1, Map.of(FieldPath.parse("content"), FieldPath.parse("_autoEmbed.content")));
    var autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("content"));
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbedFieldMapping mapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(searchDef, schemaMetadata);

    SearchIndexAutoEmbedFieldMapping searchMapping = (SearchIndexAutoEmbedFieldMapping) mapping;
    Assert.assertTrue(
        "Dynamic flag must propagate to MV mapping", searchMapping.dynamic().isEnabled());
    Assert.assertTrue(
        "Hash path must be in the explicit passthrough set regardless of dynamic mode",
        searchMapping
            .explicitPassthroughPaths()
            .contains(FieldPath.parse("_autoEmbed." + HASH_FIELD_SUFFIX + ".content")));
  }

  @Test
  public void createMatViewAutoEmbedMapping_search_nestedAutoEmbedUnderDocument() {
    // The MV-side mapping must mirror the source-side walk for auto-embed declarations nested
    // under `document` fields: embed keyed at the MV-rewritten path, hash passthrough added, and
    // the sibling non-auto-embed field passthrough at its full dotted path. Without the descent,
    // the initial-sync scanner's MV-doc collector never descends into `_autoEmbed`, so every doc
    // is reported stale and re-embedded on resync.
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1, Map.of(FieldPath.parse("content"), FieldPath.parse("_autoEmbed.content")));
    var autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("content"));
    DocumentFieldDefinition nested =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("parent", FieldDefinitionBuilder.builder().document(nested).build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbedFieldMapping mapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(searchDef, schemaMetadata);

    FieldPath matViewEmbedPath = FieldPath.parse("_autoEmbed.content");
    FieldPath hashPath = FieldPath.parse("_autoEmbed." + HASH_FIELD_SUFFIX + ".content");
    Assert.assertTrue(
        "Nested auto-embed should be keyed at the MV-rewritten path",
        mapping.embedFields().containsKey(matViewEmbedPath));
    Assert.assertTrue("Hash path should be a passthrough", mapping.isPassthrough(hashPath));
    Assert.assertTrue(
        "Sibling non-auto-embed field should be passthrough at its dotted path",
        mapping.isPassthrough(FieldPath.parse("parent.active")));
    Assert.assertFalse(
        "Containing document should be descended into, not passed through wholesale",
        mapping.isPassthrough(FieldPath.parse("parent.embedding")));
    Assert.assertTrue(
        "MV-doc collectors must descend into _autoEmbed",
        mapping.subDocumentExists(FieldPath.parse("_autoEmbed")));
  }

  @Test
  public void createMatViewAutoEmbedMapping_search_dynamicDocumentWithAutoEmbed() {
    // MV-side mirror of the wholesale rule: the dynamic document is a single passthrough at its
    // source path (passthrough paths keep their source paths on the MV), the embed is keyed at the
    // MV-rewritten path, and the hash passthrough is added.
    var schemaMetadata =
        new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
            1,
            Map.of(
                FieldPath.parse("parent.content"), FieldPath.parse("_autoEmbed.parent.content")));
    var autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("parent.content"));
    DocumentFieldDefinition nested =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(true)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .field(
                "active",
                FieldDefinitionBuilder.builder().bool(new BooleanFieldDefinition()).build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("parent", FieldDefinitionBuilder.builder().document(nested).build())
            .build();
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder().defaultMetadata().mappings(mappings).build();

    AutoEmbedFieldMapping mapping =
        AutoEmbedFieldMappingCreator.createMatViewAutoEmbedMapping(searchDef, schemaMetadata);

    FieldPath hashPath = FieldPath.parse("_autoEmbed." + HASH_FIELD_SUFFIX + ".parent.content");
    Assert.assertTrue(
        "Embed keyed at the MV-rewritten path",
        mapping.embedFields().containsKey(FieldPath.parse("_autoEmbed.parent.content")));
    Assert.assertTrue("Hash path is a passthrough", mapping.isPassthrough(hashPath));
    Assert.assertTrue(
        "Dynamic document is a wholesale passthrough on the MV side",
        mapping.isPassthrough(FieldPath.parse("parent")));
    Assert.assertTrue(
        "MV-doc collectors must descend into _autoEmbed",
        mapping.subDocumentExists(FieldPath.parse("_autoEmbed")));
  }
}
