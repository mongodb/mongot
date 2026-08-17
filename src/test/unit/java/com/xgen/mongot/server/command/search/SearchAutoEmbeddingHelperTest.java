package com.xgen.mongot.server.command.search;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.util.bson.FloatVector.OriginalType.NATIVE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.VectorOrError;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.autoembedding.UnresolvedAutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.definition.AnalyzerBoundSearchIndexDefinition;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.counts.Count;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.ExactVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
import com.xgen.mongot.index.query.operators.VectorSearchQueryInput;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SearchAutoEmbeddingHelperTest {

  private static final Vector QUERY_VECTOR = Vector.fromFloats(new float[] {1f, 2f, 3f}, NATIVE);
  private static final FieldPath EMBEDDING_PATH = FieldPath.parse("embedding");
  private static final Map<FieldPath, FieldPath> AUTO_EMBED_FIELDS_MAPPING =
      Map.of(FieldPath.parse("content"), FieldPath.parse("_autoEmbed.content"));

  @Before
  public void clearCatalogBeforeTest() {
    EmbeddingModelCatalog.clear();
  }

  @After
  public void clearCatalogAfterTest() {
    EmbeddingModelCatalog.clear();
  }

  @Test
  public void testEmbedAndReplaceCriteria_success() throws Exception {
    SearchIndexDefinition searchDef = SearchIndex.mockAutoEmbeddingSearchDefinition(new ObjectId());
    Supplier<EmbeddingServiceManager> embeddingSupplier =
        mockEmbeddingServiceManager(new VectorOrError(QUERY_VECTOR));

    VectorSearchCriteria criteria =
        ApproximateVectorQueryCriteriaBuilder.builder()
            .limit(10)
            .numCandidates(100)
            .query(new VectorSearchQueryInput.Text("test query"))
            .path(EMBEDDING_PATH)
            .build();

    VectorSearchCriteria result =
        SearchAutoEmbeddingHelper.embedAndReplaceCriteria(
            criteria,
            "test query",
            Optional.empty(),
            searchDef,
            AUTO_EMBED_FIELDS_MAPPING,
            embeddingSupplier);

    assertThat(result.queryVector()).isPresent();
    assertThat(result.queryVector().get()).isEqualTo(QUERY_VECTOR);
    assertThat(result.query()).isEmpty();
    assertThat(result.path()).isEqualTo(FieldPath.parse("_autoEmbed.content"));
  }

  @Test
  public void testEmbedAndReplaceCriteria_nestedPath_success() throws Exception {
    // A nested auto-embed field (parent.embedding) must resolve through the field hierarchy, not
    // the top-level-only fields() map.
    SearchIndexDefinition searchDef = nestedAutoEmbeddingSearchDefinition();
    Supplier<EmbeddingServiceManager> embeddingSupplier =
        mockEmbeddingServiceManager(new VectorOrError(QUERY_VECTOR));

    VectorSearchCriteria criteria =
        ApproximateVectorQueryCriteriaBuilder.builder()
            .limit(10)
            .numCandidates(100)
            .query(new VectorSearchQueryInput.Text("test query"))
            .path(FieldPath.parse("parent.embedding"))
            .build();

    VectorSearchCriteria result =
        SearchAutoEmbeddingHelper.embedAndReplaceCriteria(
            criteria,
            "test query",
            Optional.empty(),
            searchDef,
            AUTO_EMBED_FIELDS_MAPPING,
            embeddingSupplier);

    assertThat(result.queryVector()).isPresent();
    assertThat(result.queryVector().get()).isEqualTo(QUERY_VECTOR);
    assertThat(result.path()).isEqualTo(FieldPath.parse("_autoEmbed.content"));
  }

  @Test
  public void testEmbedAndReplaceCriteria_noModelForPath_throwsInvalidQueryException() {
    SearchIndexDefinition searchDef = SearchIndex.mockAutoEmbeddingSearchDefinition(new ObjectId());
    Supplier<EmbeddingServiceManager> embeddingSupplier =
        mockEmbeddingServiceManager(new VectorOrError(QUERY_VECTOR));

    VectorSearchCriteria criteria =
        ApproximateVectorQueryCriteriaBuilder.builder()
            .limit(10)
            .numCandidates(100)
            .query(new VectorSearchQueryInput.Text("test query"))
            .path(FieldPath.parse("nonexistent_field"))
            .build();

    Assert.assertThrows(
        InvalidQueryException.class,
        () ->
            SearchAutoEmbeddingHelper.embedAndReplaceCriteria(
                criteria,
                "test query",
                Optional.empty(),
                searchDef,
                AUTO_EMBED_FIELDS_MAPPING,
                embeddingSupplier));
  }

  @Test
  public void testEmbedAndReplaceCriteria_nullEmbeddingService_throwsInvalidQueryException() {
    SearchIndexDefinition searchDef = SearchIndex.mockAutoEmbeddingSearchDefinition(new ObjectId());
    Supplier<EmbeddingServiceManager> nullSupplier = () -> null;

    VectorSearchCriteria criteria =
        ApproximateVectorQueryCriteriaBuilder.builder()
            .limit(10)
            .numCandidates(100)
            .query(new VectorSearchQueryInput.Text("test query"))
            .path(EMBEDDING_PATH)
            .build();

    Assert.assertThrows(
        InvalidQueryException.class,
        () ->
            SearchAutoEmbeddingHelper.embedAndReplaceCriteria(
                criteria,
                "test query",
                Optional.empty(),
                searchDef,
                AUTO_EMBED_FIELDS_MAPPING,
                nullSupplier));
  }

  @Test
  public void testEmbedAndReplaceCriteria_embeddingFailure_throwsInvalidQueryException() {
    SearchIndexDefinition searchDef = SearchIndex.mockAutoEmbeddingSearchDefinition(new ObjectId());
    Supplier<EmbeddingServiceManager> embeddingSupplier =
        mockEmbeddingServiceManager(new VectorOrError("model returned error"));

    VectorSearchCriteria criteria =
        ApproximateVectorQueryCriteriaBuilder.builder()
            .limit(10)
            .numCandidates(100)
            .query(new VectorSearchQueryInput.Text("test query"))
            .path(EMBEDDING_PATH)
            .build();

    Assert.assertThrows(
        InvalidQueryException.class,
        () ->
            SearchAutoEmbeddingHelper.embedAndReplaceCriteria(
                criteria,
                "test query",
                Optional.empty(),
                searchDef,
                AUTO_EMBED_FIELDS_MAPPING,
                embeddingSupplier));
  }

  @Test
  public void testEmbedAndReplaceCriteria_disallowedQueryModel_throwsInvalidQueryException() {
    SearchIndexDefinition searchDef = SearchIndex.mockAutoEmbeddingSearchDefinition(new ObjectId());
    Supplier<EmbeddingServiceManager> embeddingSupplier =
        mockEmbeddingServiceManager(new VectorOrError(QUERY_VECTOR));

    VectorSearchCriteria criteria =
        ApproximateVectorQueryCriteriaBuilder.builder()
            .limit(10)
            .numCandidates(100)
            .query(new VectorSearchQueryInput.Text("test query"))
            .path(EMBEDDING_PATH)
            .build();

    // A user-supplied query model that is not on the allow-list is rejected, not silently replaced
    // by the index model.
    Assert.assertThrows(
        InvalidQueryException.class,
        () ->
            SearchAutoEmbeddingHelper.embedAndReplaceCriteria(
                criteria,
                "test query",
                Optional.of("not-a-real-model"),
                searchDef,
                AUTO_EMBED_FIELDS_MAPPING,
                embeddingSupplier));
  }

  @Test
  public void testResolveQueryModel_noQueryModel_returnsIndexModel() throws Exception {
    assertThat(SearchAutoEmbeddingHelper.resolveQueryModel("voyage-3-large", Optional.empty()))
        .isEqualTo("voyage-3-large");
  }

  @Test
  public void testResolveQueryModel_disallowedQueryModel_throwsInvalidQueryException() {
    Assert.assertThrows(
        InvalidQueryException.class,
        () ->
            SearchAutoEmbeddingHelper.resolveQueryModel(
                "voyage-3-large", Optional.of("not-a-real-model")));
  }

  @Test
  public void testApproximateVectorSearchCriteria_withQueryVector() {
    ApproximateVectorSearchCriteria criteria =
        ApproximateVectorQueryCriteriaBuilder.builder()
            .limit(10)
            .numCandidates(100)
            .query(new VectorSearchQueryInput.Text("test query"))
            .path(EMBEDDING_PATH)
            .build();

    assertThat(criteria.queryVector()).isEmpty();
    assertThat(criteria.query()).isPresent();

    VectorSearchCriteria result = criteria.withQueryVector(QUERY_VECTOR);

    assertThat(result.queryVector()).isPresent();
    assertThat(result.queryVector().get()).isEqualTo(QUERY_VECTOR);
    assertThat(result.query()).isEmpty();
    assertThat(result.path()).isEqualTo(EMBEDDING_PATH);
    assertThat(result).isInstanceOf(ApproximateVectorSearchCriteria.class);
  }

  @Test
  public void testExactVectorSearchCriteria_withQueryVector() {
    ExactVectorSearchCriteria criteria =
        new ExactVectorSearchCriteria(
            EMBEDDING_PATH,
            Optional.empty(),
            Optional.of(new VectorSearchQueryInput.Text("test query")),
            Optional.empty(),
            Optional.empty(),
            10,
            false,
            Optional.empty());

    assertThat(criteria.queryVector()).isEmpty();
    assertThat(criteria.query()).isPresent();

    VectorSearchCriteria result = criteria.withQueryVector(QUERY_VECTOR);

    assertThat(result.queryVector()).isPresent();
    assertThat(result.queryVector().get()).isEqualTo(QUERY_VECTOR);
    assertThat(result.query()).isEmpty();
    assertThat(result.path()).isEqualTo(EMBEDDING_PATH);
    assertThat(result).isInstanceOf(ExactVectorSearchCriteria.class);
  }

  @Test
  public void testOperatorQuery_withOperator_preservesAllFields() {
    VectorSearchCriteria criteria =
        ApproximateVectorQueryCriteriaBuilder.builder()
            .limit(10)
            .numCandidates(100)
            .queryVector(QUERY_VECTOR)
            .path(EMBEDDING_PATH)
            .build();

    VectorSearchOperator originalOp = new VectorSearchOperator(null, criteria);
    OperatorQuery query =
        new OperatorQuery(
            originalOp,
            "testIndex",
            Count.DEFAULT,
            Optional.empty(),
            Optional.empty(),
            false,
            false,
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    VectorSearchOperator replacementOp = new VectorSearchOperator(null, criteria);
    OperatorQuery result = query.withOperator(replacementOp);

    assertThat(result.operator()).isSameInstanceAs(replacementOp);
    assertThat(result.index()).isEqualTo("testIndex");
    assertThat(result.count()).isEqualTo(Count.DEFAULT);
    assertThat(result.returnStoredSource()).isFalse();
  }

  @Test
  public void resolveAutoEmbeddingFieldsMapping_unresolvedOfflineIndex_usesPersistedMapping() {
    SearchIndexDefinition rawDef = SearchIndex.mockAutoEmbeddingSearchDefinition(new ObjectId());
    SearchIndexDefinitionGeneration rawGen =
        new SearchIndexDefinitionGeneration(
            AnalyzerBoundSearchIndexDefinition.create(rawDef, Collections.emptyList()),
            Generation.CURRENT);
    InitializedSearchIndex reconstructed = mock(InitializedSearchIndex.class);
    when(reconstructed.getStatus())
        .thenReturn(IndexStatus.stale("serving offline", new BsonTimestamp(7, 1)));
    // Offline: the catalog holds an unresolved generation (no materialized view to supply the
    // mapping), which carries the mapping persisted with the reconstructed derived index.
    IndexGeneration unresolved =
        UnresolvedAutoEmbeddingIndexGeneration.createStale(
            rawGen, reconstructed, AUTO_EMBED_FIELDS_MAPPING);
    IndexCatalog indexCatalog = mock(IndexCatalog.class);
    when(indexCatalog.getIndex(any(), any(), any(), any())).thenReturn(Optional.of(unresolved));

    Map<FieldPath, FieldPath> mapping =
        SearchAutoEmbeddingHelper.resolveAutoEmbeddingFieldsMapping(
            indexCatalog,
            rawDef.getDatabase(),
            rawDef.getCollectionUuid(),
            Optional.empty(),
            rawDef.getName());

    assertThat(mapping).isEqualTo(AUTO_EMBED_FIELDS_MAPPING);
  }

  private static Supplier<EmbeddingServiceManager> mockEmbeddingServiceManager(
      VectorOrError vectorOrError) {
    setupRegistry();
    EmbeddingServiceManager manager = mock(EmbeddingServiceManager.class);
    doReturn(List.of(vectorOrError))
        .when(manager)
        .embed(
            Mockito.anyList(),
            any(EmbeddingModelConfig.class),
            any(EmbeddingServiceConfig.ServiceTier.class),
            any(EmbeddingRequestContext.class));
    return Suppliers.ofInstance(manager);
  }

  private static SearchIndexDefinition nestedAutoEmbeddingSearchDefinition() {
    SearchAutoEmbedFieldDefinition autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("content"));
    DocumentFieldDefinition innerDoc =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field(
                "embedding",
                FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
            .build();
    DocumentFieldDefinition mappings =
        DocumentFieldDefinitionBuilder.builder()
            .dynamic(false)
            .field("parent", FieldDefinitionBuilder.builder().document(innerDoc).build())
            .build();
    return SearchIndexDefinitionBuilder.builder()
        .indexId(new ObjectId())
        .name("mock")
        .database("mock")
        .lastObservedCollectionName("mock")
        .collectionUuid(UUID.randomUUID())
        .mappings(mappings)
        .build();
  }

  private static void setupRegistry() {
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
                    Optional.of(1000)),
                new EmbeddingServiceConfig.ErrorHandlingConfig(50, 50L, 10L, 0.1),
                new EmbeddingServiceConfig.VoyageEmbeddingCredentials(
                    "token123", "2024-10-15T22:32:20.925Z"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty())));
  }
}
