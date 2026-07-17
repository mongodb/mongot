package com.xgen.mongot.index.autoembedding;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.definition.AnalyzerBoundSearchIndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.StaleStatusReason;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingCommitMetadata;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.StaleStateInfo;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.DocumentFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.FieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests for {@link AutoEmbeddingIndexGenerationFactory}: the source-collection gate in {@link
 * AutoEmbeddingIndexGenerationFactory#getAutoEmbeddingIndexGeneration} and {@link
 * AutoEmbeddingIndexGenerationFactory#derivedIndexDefinitionGeneration} (vector and search
 * auto-embed source definitions).
 */
public class AutoEmbeddingIndexGenerationFactoryTest {

  private static final String MV_DATABASE_NAME = "__mdb_internal_search";
  // Multi-tenant internal-search database: NOT the plain "__mdb_internal_search". On a
  // multi-tenant cluster the name is namespaced per tenant, so it cannot be recomputed
  // offline and must come from the persisted commit metadata.
  private static final String TENANT_MV_DATABASE_NAME = "acme___mdb_internal_search";
  private static final UUID MV_COLLECTION_UUID = UUID.randomUUID();

  @Test
  public void derivedIndexDefinitionGeneration_vectorIndex_buildsVectorDerivedDefinition() {
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition vectorDef =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .name("mock")
            .database("mock")
            .lastObservedCollectionName("mock")
            .collectionUuid(UUID.randomUUID())
            .withAutoEmbedField("title")
            .build();
    VectorIndexDefinitionGeneration rawGen =
        new VectorIndexDefinitionGeneration(vectorDef, Generation.CURRENT);
    InitializedMaterializedViewIndex matViewIndex = matViewIndexMock();

    IndexDefinitionGeneration derived =
        AutoEmbeddingIndexGenerationFactory.derivedIndexDefinitionGeneration(rawGen, matViewIndex);

    assertNotNull(derived);
    assertTrue(
        "Vector source must derive a vector index definition generation",
        derived instanceof VectorIndexDefinitionGeneration);
    assertEquals(MV_DATABASE_NAME, derived.getIndexDefinition().getDatabase());
    assertEquals(MV_COLLECTION_UUID, derived.getIndexDefinition().getCollectionUuid());
  }

  @Test
  public void derivedIndexDefinitionGeneration_searchIndex_buildsSearchDerivedDefinition() {
    ObjectId indexId = new ObjectId();
    SearchAutoEmbedFieldDefinition autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("content"));
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(indexId)
            .name("mock")
            .database("mock")
            .lastObservedCollectionName("mock")
            .collectionUuid(UUID.randomUUID())
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "content_embed",
                        FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
                    .build())
            .build();
    SearchIndexDefinitionGeneration rawGen =
        new SearchIndexDefinitionGeneration(
            AnalyzerBoundSearchIndexDefinition.create(searchDef, Collections.emptyList()),
            Generation.CURRENT);
    InitializedMaterializedViewIndex matViewIndex = matViewIndexMock();

    IndexDefinitionGeneration derived =
        AutoEmbeddingIndexGenerationFactory.derivedIndexDefinitionGeneration(rawGen, matViewIndex);

    assertNotNull(derived);
    assertTrue(
        "Search source must derive a search index definition generation",
        derived instanceof SearchIndexDefinitionGeneration);
    assertEquals(MV_DATABASE_NAME, derived.getIndexDefinition().getDatabase());
    assertEquals(MV_COLLECTION_UUID, derived.getIndexDefinition().getCollectionUuid());
  }

  @Test
  public void getAutoEmbeddingIndexGeneration_sourceCollectionAbsent_skipsMaterializedView()
      throws Exception {
    VectorIndexDefinitionGeneration rawGen = autoEmbedVectorGen();
    MaterializedViewIndexFactory matViewIndexFactory = mock(MaterializedViewIndexFactory.class);
    IndexFactory indexFactory = mock(IndexFactory.class);
    InitializedIndexCatalog catalog = mock(InitializedIndexCatalog.class);
    when(matViewIndexFactory.isSourceCollectionAbsentOnMongod(any())).thenReturn(true);

    IndexGeneration result =
        AutoEmbeddingIndexGenerationFactory.getAutoEmbeddingIndexGeneration(
            indexFactory, matViewIndexFactory, rawGen, catalog, () -> false);

    assertTrue(
        "Absent source collection must yield an unresolved generation",
        result instanceof UnresolvedAutoEmbeddingIndexGeneration);
    IndexStatus status = result.getIndex().getStatus();
    assertEquals(IndexStatus.StatusCode.DOES_NOT_EXIST, status.getStatusCode());
    assertEquals(Optional.of(IndexStatus.Reason.COLLECTION_NOT_FOUND), status.getReason());
    // The unresolved generation must carry the source collection UUID so the recovery stager can
    // re-stage it once the collection reappears on this shard (see IndexRecoveryStagerTest).
    assertEquals(
        rawGen.getIndexDefinition().getCollectionUuid(),
        result.getDefinition().getCollectionUuid());
    // No materialized view collection, lease, or leaderStatus gauge is created on a shard that does
    // not host the source collection.
    verify(matViewIndexFactory, never()).getIndex(any());
    verify(indexFactory, never()).getIndex(any());
  }

  @Test
  public void getAutoEmbeddingIndexGeneration_sourceCollectionPresent_buildsCompositeGeneration()
      throws Exception {
    VectorIndexDefinitionGeneration rawGen = autoEmbedVectorGen();
    MaterializedViewIndexFactory matViewIndexFactory = mock(MaterializedViewIndexFactory.class);
    IndexFactory indexFactory = mock(IndexFactory.class);
    InitializedIndexCatalog catalog = mock(InitializedIndexCatalog.class);
    // Build the mat-view mock before stubbing — matViewIndexMock() does its own stubbing, which
    // Mockito disallows nested inside a thenReturn(...).
    InitializedMaterializedViewIndex matViewIndex = matViewIndexMock();
    when(matViewIndex.isCompatibleWith(any())).thenReturn(true);
    Index derivedIndex = mock(Index.class);
    when(derivedIndex.isCompatibleWith(any())).thenReturn(true);
    when(matViewIndexFactory.isSourceCollectionAbsentOnMongod(any())).thenReturn(false);
    when(matViewIndexFactory.getIndex(any())).thenReturn(matViewIndex);
    when(indexFactory.getIndex(any())).thenReturn(derivedIndex);

    IndexGeneration result =
        AutoEmbeddingIndexGenerationFactory.getAutoEmbeddingIndexGeneration(
            indexFactory, matViewIndexFactory, rawGen, catalog, () -> false);

    assertTrue(
        "Present source collection must build a composite auto-embedding generation",
        result instanceof AutoEmbeddingIndexGeneration);
    verify(matViewIndexFactory).getIndex(any());
    // Precedence: when the lease/materialized view resolves, the mongod-backed status is the source
    // of truth and the on-disk fallback is never consulted.
    verify(indexFactory, never()).readPersistedCommitUserData(any());
  }

  @Test
  public void
      getAutoEmbeddingIndexGeneration_mongodBroken_diskQueryableNotReconstructable_reportsRetry()
          throws Exception {
    // wasMatViewReady=true but the commit lacks the metadata to rebuild and timestamp the derived
    // index offline (e.g. a pre-feature commit that persisted the ready flag alone), so it cannot
    // serve STALE and parks in a recoverable RETRY.
    IndexGeneration result =
        resolveWithBrokenMongod(
            Optional.of(
                IndexCommitUserData.EMPTY
                    .withAutoEmbeddingMetadata(new AutoEmbeddingCommitMetadata(true))
                    .toEncodedData()));

    assertTrue(result instanceof UnresolvedAutoEmbeddingIndexGeneration);
    assertEquals(
        Optional.of(IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_RETRY),
        result.getIndex().getStatus().getReason());
  }

  @Test
  public void getAutoEmbeddingIndexGeneration_mongodBroken_diskUnreadable_reportsFailed()
      throws Exception {
    // Unreadable commit (I/O error, corrupt, or no on-disk commit): readPersistedCommitUserData
    // returns empty, so the fallback conservatively treats the index as not-queryable -> FAILED.
    IndexGeneration result = resolveWithBrokenMongod(Optional.empty());

    assertTrue(result instanceof UnresolvedAutoEmbeddingIndexGeneration);
    assertEquals(
        Optional.of(IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_RETRY),
        result.getIndex().getStatus().getReason());
  }

  @Test
  public void getAutoEmbeddingIndexGeneration_mongodBroken_diskMissingMetadata_reportsFailed()
      throws Exception {
    // Post-upgrade commit written before this feature: the commit parses but has no
    // autoEmbeddingMetadata sub-document, so the fallback treats it as not-queryable -> FAILED.
    IndexGeneration result =
        resolveWithBrokenMongod(Optional.of(IndexCommitUserData.EMPTY.toEncodedData()));

    assertTrue(result instanceof UnresolvedAutoEmbeddingIndexGeneration);
    assertEquals(
        Optional.of(IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_RETRY),
        result.getIndex().getStatus().getReason());
  }

  @Test
  public void
      getAutoEmbeddingIndexGeneration_mongodBroken_diskQueryableWithMetadata_servesStaleOffline()
          throws Exception {
    VectorIndexDefinitionGeneration rawGen = autoEmbedVectorGen();
    BsonTimestamp committedOptime = new BsonTimestamp(42, 1);
    MaterializedViewIndexFactory matViewIndexFactory = mock(MaterializedViewIndexFactory.class);
    IndexFactory indexFactory =
        indexFactoryWithCommit(
            Optional.of(
                IndexCommitUserData.createStale(
                        StaleStateInfo.create(committedOptime, StaleStatusReason.DOCS_EXCEEDED))
                    .withAutoEmbeddingMetadata(reconstructableMetadata(TENANT_MV_DATABASE_NAME))
                    .toEncodedData()));
    InitializedIndexCatalog catalog = mock(InitializedIndexCatalog.class);
    when(matViewIndexFactory.isSourceCollectionAbsentOnMongod(any())).thenReturn(false);
    when(matViewIndexFactory.getIndex(any()))
        .thenThrow(
            new MaterializedViewTransientException(
                "mongod unreachable", MaterializedViewTransientException.Reason.UNKNOWN));
    InitializedVectorIndex reconstructed = mock(InitializedVectorIndex.class);
    when(reconstructed.getStatus())
        .thenReturn(IndexStatus.stale("serving offline", committedOptime));
    when(indexFactory.getIndex(any())).thenReturn(mock(Index.class));
    when(indexFactory.getInitializedIndex(any(), any())).thenReturn(reconstructed);

    IndexGeneration result =
        AutoEmbeddingIndexGenerationFactory.getAutoEmbeddingIndexGeneration(
            indexFactory, matViewIndexFactory, rawGen, catalog, () -> false);

    assertTrue(result instanceof UnresolvedAutoEmbeddingIndexGeneration);
    // The factory sets the reconstructed derived index STALE as of the commit's optime, and the
    // unresolved generation delegates its status to it -> reported STALE, not FAILED.
    ArgumentCaptor<IndexStatus> staleStatus = ArgumentCaptor.forClass(IndexStatus.class);
    verify(reconstructed).setStatus(staleStatus.capture());
    assertEquals(IndexStatus.StatusCode.STALE, staleStatus.getValue().getStatusCode());
    assertEquals(Optional.of(committedOptime), staleStatus.getValue().getOptime());
    assertEquals(IndexStatus.StatusCode.STALE, result.getIndex().getStatus().getStatusCode());
    UnresolvedAutoEmbeddingIndex unresolved = (UnresolvedAutoEmbeddingIndex) result.getIndex();
    assertThat(unresolved.getReconstructedDerivedIndex()).hasValue(reconstructed);
    // The persisted user->internal field mapping is threaded onto the unresolved index so
    // VectorSearchCommand can remap the query path while serving offline.
    assertThat(unresolved.getAutoEmbeddingFieldsMapping())
        .containsExactly(FieldPath.parse("title"), FieldPath.parse("_autoEmbed.title"));
  }

  @Test
  public void getAutoEmbeddingIndexGeneration_mongodBroken_malformedResumeToken_reportsRetry()
      throws Exception {
    // A reconstructable commit whose only optime source is a resume token that is valid hex but
    // not a timestamp payload: optime extraction must degrade (no STALE without an optime) instead
    // of aborting resolution, parking the index in a recoverable RETRY.
    VectorIndexDefinitionGeneration rawGen = autoEmbedVectorGen();
    MaterializedViewIndexFactory matViewIndexFactory = mock(MaterializedViewIndexFactory.class);
    IndexFactory indexFactory =
        indexFactoryWithCommit(
            Optional.of(
                IndexCommitUserData.createChangeStreamResume(
                        ChangeStreamResumeInfo.create(
                            new MongoNamespace("db", "coll"),
                            new BsonDocument("_data", new BsonString("00"))),
                        IndexFormatVersion.CURRENT)
                    .withAutoEmbeddingMetadata(reconstructableMetadata(TENANT_MV_DATABASE_NAME))
                    .toEncodedData()));
    InitializedIndexCatalog catalog = mock(InitializedIndexCatalog.class);
    when(matViewIndexFactory.isSourceCollectionAbsentOnMongod(any())).thenReturn(false);
    when(matViewIndexFactory.getIndex(any()))
        .thenThrow(
            new MaterializedViewTransientException(
                "mongod unreachable", MaterializedViewTransientException.Reason.UNKNOWN));
    IndexGeneration result =
        AutoEmbeddingIndexGenerationFactory.getAutoEmbeddingIndexGeneration(
            indexFactory, matViewIndexFactory, rawGen, catalog, () -> false);

    assertTrue(result instanceof UnresolvedAutoEmbeddingIndexGeneration);
    assertEquals(IndexStatus.StatusCode.FAILED, result.getIndex().getStatus().getStatusCode());
    assertEquals(
        Optional.of(IndexStatus.Reason.AUTO_EMBEDDING_RESOLUTION_RETRY),
        result.getIndex().getStatus().getReason());
    // The optime is validated before reconstruction, so the rejected commit never opens (and
    // never leaks) a derived Lucene reader/writer.
    verify(indexFactory, never()).getInitializedIndex(any(), any());
  }

  @Test
  public void
      reconstructOfflineDerivedVectorIndex_persistedTenantMetadata_rebuildsWithTenantDatabase()
          throws Exception {
    IndexFactory indexFactory =
        indexFactoryWithCommit(
            Optional.of(commitWith(reconstructableMetadata(TENANT_MV_DATABASE_NAME))));
    InitializedVectorIndex reconstructed = mock(InitializedVectorIndex.class);
    ArgumentCaptor<IndexDefinitionGeneration> derivedGen =
        ArgumentCaptor.forClass(IndexDefinitionGeneration.class);
    when(indexFactory.getIndex(derivedGen.capture())).thenReturn(mock(Index.class));
    when(indexFactory.getInitializedIndex(any(), any())).thenReturn(reconstructed);

    Optional<InitializedIndex> result =
        AutoEmbeddingIndexGenerationFactory.reconstructOfflineDerivedVectorIndex(
            indexFactory, autoEmbedVectorGen());

    assertThat(result).isPresent();
    assertEquals(reconstructed, result.get());
    // MTM: the derived definition is rebuilt using the persisted tenant-namespaced internal-search
    // database name and mongod-assigned collection UUID, neither derivable offline any other way.
    assertEquals(TENANT_MV_DATABASE_NAME, derivedGen.getValue().getIndexDefinition().getDatabase());
    assertEquals(
        MV_COLLECTION_UUID, derivedGen.getValue().getIndexDefinition().getCollectionUuid());
  }

  @Test
  public void reconstructOfflineDerivedVectorIndex_noPersistedCommit_returnsEmpty()
      throws Exception {
    IndexFactory indexFactory = indexFactoryWithCommit(Optional.empty());
    assertThat(
            AutoEmbeddingIndexGenerationFactory.reconstructOfflineDerivedVectorIndex(
                indexFactory, autoEmbedVectorGen()))
        .isEmpty();
  }

  @Test
  public void reconstructOfflineDerivedVectorIndex_commitWithoutAutoEmbeddingMetadata_returnsEmpty()
      throws Exception {
    IndexFactory indexFactory =
        indexFactoryWithCommit(Optional.of(IndexCommitUserData.EMPTY.toEncodedData()));
    assertThat(
            AutoEmbeddingIndexGenerationFactory.reconstructOfflineDerivedVectorIndex(
                indexFactory, autoEmbedVectorGen()))
        .isEmpty();
  }

  @Test
  public void reconstructOfflineDerivedVectorIndex_metadataMissingDatabaseName_returnsEmpty()
      throws Exception {
    AutoEmbeddingCommitMetadata metadata =
        new AutoEmbeddingCommitMetadata(true, Optional.empty(), Optional.of(collectionMetadata()));
    IndexFactory indexFactory = indexFactoryWithCommit(Optional.of(commitWith(metadata)));
    assertThat(
            AutoEmbeddingIndexGenerationFactory.reconstructOfflineDerivedVectorIndex(
                indexFactory, autoEmbedVectorGen()))
        .isEmpty();
  }

  @Test
  public void reconstructOfflineDerivedVectorIndex_metadataMissingCollectionMetadata_returnsEmpty()
      throws Exception {
    AutoEmbeddingCommitMetadata metadata =
        new AutoEmbeddingCommitMetadata(
            true, Optional.of(TENANT_MV_DATABASE_NAME), Optional.empty());
    IndexFactory indexFactory = indexFactoryWithCommit(Optional.of(commitWith(metadata)));
    assertThat(
            AutoEmbeddingIndexGenerationFactory.reconstructOfflineDerivedVectorIndex(
                indexFactory, autoEmbedVectorGen()))
        .isEmpty();
  }

  @Test
  public void reconstructOfflineDerivedVectorIndex_searchAutoEmbedSource_returnsEmpty()
      throws Exception {
    // Full reconstruction metadata is present, but a search auto-embed source is outside the
    // vector-only proof scope, so reconstruction degrades to empty rather than throwing.
    IndexFactory indexFactory =
        indexFactoryWithCommit(
            Optional.of(commitWith(reconstructableMetadata(TENANT_MV_DATABASE_NAME))));
    assertThat(
            AutoEmbeddingIndexGenerationFactory.reconstructOfflineDerivedVectorIndex(
                indexFactory, autoEmbedSearchGen()))
        .isEmpty();
  }

  @Test
  public void reconstructOfflineDerivedVectorIndex_openingIndexThrows_returnsEmpty()
      throws Exception {
    IndexFactory indexFactory =
        indexFactoryWithCommit(
            Optional.of(commitWith(reconstructableMetadata(TENANT_MV_DATABASE_NAME))));
    when(indexFactory.getIndex(any())).thenReturn(mock(Index.class));
    when(indexFactory.getInitializedIndex(any(), any())).thenThrow(new IOException("disk gone"));
    assertThat(
            AutoEmbeddingIndexGenerationFactory.reconstructOfflineDerivedVectorIndex(
                indexFactory, autoEmbedVectorGen()))
        .isEmpty();
  }

  /**
   * Resolves an auto-embedding generation while the materialized view is unreachable (mongod
   * broken), stubbing the on-disk commit read to return {@code persistedCommit}.
   */
  private static IndexGeneration resolveWithBrokenMongod(Optional<EncodedUserData> persistedCommit)
      throws Exception {
    VectorIndexDefinitionGeneration rawGen = autoEmbedVectorGen();
    MaterializedViewIndexFactory matViewIndexFactory = mock(MaterializedViewIndexFactory.class);
    IndexFactory indexFactory = mock(IndexFactory.class);
    InitializedIndexCatalog catalog = mock(InitializedIndexCatalog.class);
    when(matViewIndexFactory.isSourceCollectionAbsentOnMongod(any())).thenReturn(false);
    when(matViewIndexFactory.getIndex(any()))
        .thenThrow(
            new MaterializedViewTransientException(
                "mongod unreachable", MaterializedViewTransientException.Reason.UNKNOWN));
    when(indexFactory.readPersistedCommitUserData(any())).thenReturn(persistedCommit);

    return AutoEmbeddingIndexGenerationFactory.getAutoEmbeddingIndexGeneration(
        indexFactory, matViewIndexFactory, rawGen, catalog, () -> false);
  }

  private static VectorIndexDefinitionGeneration autoEmbedVectorGen() {
    VectorIndexDefinition def =
        VectorIndexDefinitionBuilder.builder()
            .indexId(new ObjectId())
            .name("mock")
            .database("mock")
            .lastObservedCollectionName("mock")
            .collectionUuid(UUID.randomUUID())
            .withAutoEmbedField("title")
            .build();
    return new VectorIndexDefinitionGeneration(def, Generation.CURRENT);
  }

  private static InitializedMaterializedViewIndex matViewIndexMock() {
    InitializedMaterializedViewIndex matViewIndex = mock(InitializedMaterializedViewIndex.class);
    when(matViewIndex.getMaterializedViewDatabaseName()).thenReturn(MV_DATABASE_NAME);
    when(matViewIndex.getMaterializedViewCollectionUuid()).thenReturn(MV_COLLECTION_UUID);
    when(matViewIndex.getSchemaMetadata())
        .thenReturn(new MaterializedViewSchemaMetadata(0, Map.of()));
    return matViewIndex;
  }

  private static IndexFactory indexFactoryWithCommit(Optional<EncodedUserData> persistedCommit) {
    IndexFactory indexFactory = mock(IndexFactory.class);
    when(indexFactory.readPersistedCommitUserData(any())).thenReturn(persistedCommit);
    return indexFactory;
  }

  private static EncodedUserData commitWith(AutoEmbeddingCommitMetadata metadata) {
    return IndexCommitUserData.EMPTY.withAutoEmbeddingMetadata(metadata).toEncodedData();
  }

  /**
   * A commit carrying everything offline reconstruction needs: the ready flag, the (tenant)
   * database name, and the collection metadata whose schema maps the raw autoEmbed field "title" to
   * its derived internal path "_autoEmbed.title" as a materialized view would persist.
   */
  private static AutoEmbeddingCommitMetadata reconstructableMetadata(String databaseName) {
    return new AutoEmbeddingCommitMetadata(
        true, Optional.of(databaseName), Optional.of(collectionMetadata()));
  }

  private static MaterializedViewCollectionMetadata collectionMetadata() {
    MaterializedViewSchemaMetadata schema =
        new MaterializedViewSchemaMetadata(
            1L, Map.of(FieldPath.parse("title"), FieldPath.parse("_autoEmbed.title")));
    return new MaterializedViewCollectionMetadata(schema, MV_COLLECTION_UUID, "movies");
  }

  private static SearchIndexDefinitionGeneration autoEmbedSearchGen() {
    SearchAutoEmbedFieldDefinition autoEmbedField =
        new SearchAutoEmbedFieldDefinition("voyage-3-large", FieldPath.parse("content"));
    SearchIndexDefinition searchDef =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .indexId(new ObjectId())
            .name("mock")
            .database("mock")
            .lastObservedCollectionName("mock")
            .collectionUuid(UUID.randomUUID())
            .mappings(
                DocumentFieldDefinitionBuilder.builder()
                    .dynamic(false)
                    .field(
                        "content_embed",
                        FieldDefinitionBuilder.builder().searchAutoEmbed(autoEmbedField).build())
                    .build())
            .build();
    return new SearchIndexDefinitionGeneration(
        AnalyzerBoundSearchIndexDefinition.create(searchDef, Collections.emptyList()),
        Generation.CURRENT);
  }
}
