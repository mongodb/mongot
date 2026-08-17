package com.xgen.mongot.server.command.search;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexWriter;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGenerationFactory;
import com.xgen.mongot.index.autoembedding.UnresolvedAutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.index.lucene.LuceneIndexFactory;
import com.xgen.mongot.index.lucene.directory.EnvironmentVariantPerfConfig;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.monitor.NoOpDiskMonitor;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingCommitMetadata;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.BsonVectorParser;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.mongodb.MongoDbServerInfo;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.LuceneConfigBuilder;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import com.xgen.testing.mongot.server.command.search.definition.request.VectorSearchCommandDefinitionBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * Drives the real {@link VectorSearchCommand} path for a $vectorSearch served offline from a
 * derived Lucene index reconstructed purely from the persisted {@link AutoEmbeddingCommitMetadata}
 * (mongod unreachable, view unresolved). The query uses the user path (e.g. {@code "title"}) while
 * the derived index stores the vector at the internal path (e.g. {@code "_autoEmbed.title"}), so
 * serving requires remapping via the persisted schema mapping -- no mongod, lease, or view.
 */
public class VectorSearchCommandOfflineServeTest {

  // Tenant-namespaced internal-search database: not a compile-time constant, must come from the
  // persisted metadata.
  private static final String MV_DATABASE_NAME = "acme___mdb_internal_search";
  private static final UUID MV_COLLECTION_UUID = UUID.randomUUID();
  private static final int DIMENSIONS = 4;

  private static final SearchCommandsRegister.BootstrapperMetadata BOOTSTRAPPER_METADATA =
      new SearchCommandsRegister.BootstrapperMetadata(
          "testVersion",
          "localhost",
          () -> MongoDbServerInfo.EMPTY,
          FeatureFlags.getDefault(),
          DynamicFeatureFlagRegistry.empty());

  @Test
  public void run_userPathQuery_servedOfflineFromReconstructedDerivedIndex() throws Exception {
    ObjectId indexId = new ObjectId();
    UUID sourceCollectionUuid = UUID.randomUUID();

    // Raw auto-embedding definition with a user-facing field "title".
    VectorIndexDefinition rawVectorDef =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .name("vec")
            .database("sample_mflix")
            .lastObservedCollectionName("movies")
            .collectionUuid(sourceCollectionUuid)
            .withAutoEmbedField(
                "title",
                "voyage-3-large",
                DIMENSIONS,
                VectorAutoEmbedQuantization.FLOAT,
                VectorSimilarity.COSINE)
            .build();
    VectorIndexDefinitionGeneration rawGen =
        new VectorIndexDefinitionGeneration(rawVectorDef, Generation.CURRENT);
    FieldPath userPath = FieldPath.parse("title");

    MaterializedViewSchemaMetadata schema =
        new MaterializedViewSchemaMetadata(
            1L, Map.of(userPath, FieldPath.parse("_autoEmbed.title")));
    MaterializedViewCollectionMetadata collectionMetadata =
        new MaterializedViewCollectionMetadata(schema, MV_COLLECTION_UUID, indexId.toHexString());
    VectorIndexDefinition derivedDef =
        getDerivedVectorIndexDefinition(rawVectorDef, MV_DATABASE_NAME, MV_COLLECTION_UUID, schema);
    VectorIndexDefinitionGeneration derivedGen =
        new VectorIndexDefinitionGeneration(derivedDef, Generation.CURRENT);
    FieldPath derivedPath = derivedVectorFieldPath(derivedDef);
    Vector docVector =
        Vector.fromFloats(new float[] {1f, 0f, 0f, 0f}, FloatVector.OriginalType.NATIVE);

    try (LuceneIndexFactory factory = openFactory()) {
      // Write + commit the derived Lucene index (vector at the internal path) with the metadata
      // offline reconstruction relies on.
      Index derivedIndex = factory.getIndex(derivedGen);
      InitializedIndex initialized = factory.getInitializedIndex(derivedIndex, derivedGen);
      InitializedVectorIndex liveIndex =
          Check.instanceOf(initialized, InitializedVectorIndex.class);
      indexVectorDoc(liveIndex.getWriter(), indexId, 1, derivedPath, docVector);
      liveIndex
          .getWriter()
          .commit(
              IndexCommitUserData.EMPTY
                  .withAutoEmbeddingMetadata(
                      new AutoEmbeddingCommitMetadata(
                          true, Optional.of(MV_DATABASE_NAME), Optional.of(collectionMetadata)))
                  .toEncodedData());
      initialized.close();

      // Reconstruct offline and make it queryable.
      Optional<InitializedIndex> reconstructed =
          AutoEmbeddingIndexGenerationFactory.reconstructOfflineDerivedIndex(factory, rawGen);
      assertThat(reconstructed).isPresent();
      InitializedVectorIndex reconstructedVectorIndex =
          Check.instanceOf(reconstructed.get(), InitializedVectorIndex.class);
      reconstructedVectorIndex.setStatus(IndexStatus.steady());
      reconstructedVectorIndex.getReader().refresh();

      // Catalog holds the unresolved auto-embedding generation carrying the persisted
      // user->internal field mapping, and the initialized-index catalog holds the reconstructed
      // derived index under the raw generation id, as registered while serving offline.
      IndexGeneration unresolvedGeneration =
          UnresolvedAutoEmbeddingIndexGeneration.createStale(
              rawGen, reconstructedVectorIndex, schema.autoEmbeddingFieldsMapping());
      IndexCatalog indexCatalog = mock(IndexCatalog.class);
      when(indexCatalog.getIndex(any(), any(), any(), any()))
          .thenReturn(Optional.of(unresolvedGeneration));
      InitializedIndexCatalog initializedIndexCatalog = mock(InitializedIndexCatalog.class);
      when(initializedIndexCatalog.getIndex(any()))
          .thenReturn(Optional.of(reconstructedVectorIndex));

      VectorSearchCommand command =
          new VectorSearchCommand(
              VectorSearchCommandDefinitionBuilder.builder()
                  .db(rawVectorDef.getDatabase())
                  .collectionName("movies")
                  .collectionUuid(sourceCollectionUuid)
                  .vectorSearchQuery(
                      VectorQueryBuilder.builder()
                          .index(rawVectorDef.getName())
                          .criteria(
                              ApproximateVectorQueryCriteriaBuilder.builder()
                                  .limit(10)
                                  .numCandidates(100)
                                  .queryVector(docVector)
                                  .path(userPath)
                                  .build())
                          .build())
                  .build(),
              indexCatalog,
              initializedIndexCatalog,
              BOOTSTRAPPER_METADATA,
              Suppliers.ofInstance(mock(EmbeddingServiceManager.class)),
              new VectorSearchCommand.Metrics(
                  new MetricsFactory("mockNamespace", new SimpleMeterRegistry())),
              mock(MongotCursorManager.class),
              CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

      BsonDocument result = command.run();

      // The user-path query is served offline: the persisted schema mapping remaps "title" ->
      // "_autoEmbed.title", so the reconstructed derived index returns the indexed document.
      assertThat(result.getInt32("ok").getValue()).isEqualTo(1);
      BsonArray batch = result.getDocument("cursor").getArray("nextBatch");
      assertThat(batch).hasSize(1);
      assertThat(batch.get(0).asDocument().getInt32("_id").getValue()).isEqualTo(1);

      reconstructedVectorIndex.close();
    }
  }

  /**
   * Auto-embedding indexes do not support stored source (the derived vector definition carries no
   * {@code storedSource}), so a {@code returnStoredSource: true} query served offline is rejected
   * up front with a clear error rather than routed to the reconstructed index that cannot honor it.
   */
  @Test
  public void run_returnStoredSourceOnOfflineAutoEmbedding_rejectedNotSilentlyEmpty() {
    ObjectId indexId = new ObjectId();
    UUID sourceCollectionUuid = UUID.randomUUID();
    FieldPath userPath = FieldPath.parse("title");

    VectorIndexDefinition rawVectorDef =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .name("vec")
            .database("sample_mflix")
            .lastObservedCollectionName("movies")
            .collectionUuid(sourceCollectionUuid)
            .withAutoEmbedField(
                "title",
                "voyage-3-large",
                DIMENSIONS,
                VectorAutoEmbedQuantization.FLOAT,
                VectorSimilarity.COSINE)
            .build();
    VectorIndexDefinitionGeneration rawGen =
        new VectorIndexDefinitionGeneration(rawVectorDef, Generation.CURRENT);
    MaterializedViewSchemaMetadata schema =
        new MaterializedViewSchemaMetadata(
            1L, Map.of(userPath, FieldPath.parse("_autoEmbed.title")));
    VectorIndexDefinition derivedDef =
        getDerivedVectorIndexDefinition(rawVectorDef, MV_DATABASE_NAME, MV_COLLECTION_UUID, schema);

    // The reconstructed index only needs its definition (storedSource-empty) and a queryable
    // status: the query is rejected before any serving, so no real Lucene reader is required.
    InitializedVectorIndex reconstructed = mock(InitializedVectorIndex.class);
    when(reconstructed.getDefinition()).thenReturn(derivedDef);
    when(reconstructed.getStatus()).thenReturn(IndexStatus.steady());
    IndexGeneration unresolvedGeneration =
        UnresolvedAutoEmbeddingIndexGeneration.createStale(
            rawGen, reconstructed, schema.autoEmbeddingFieldsMapping());
    IndexCatalog indexCatalog = mock(IndexCatalog.class);
    when(indexCatalog.getIndex(any(), any(), any(), any()))
        .thenReturn(Optional.of(unresolvedGeneration));
    InitializedIndexCatalog initializedIndexCatalog = mock(InitializedIndexCatalog.class);
    when(initializedIndexCatalog.getIndex(any())).thenReturn(Optional.of(reconstructed));

    Vector queryVector =
        Vector.fromFloats(new float[] {1f, 0f, 0f, 0f}, FloatVector.OriginalType.NATIVE);
    VectorSearchCommand command =
        new VectorSearchCommand(
            VectorSearchCommandDefinitionBuilder.builder()
                .db(rawVectorDef.getDatabase())
                .collectionName("movies")
                .collectionUuid(sourceCollectionUuid)
                .vectorSearchQuery(
                    VectorQueryBuilder.builder()
                        .index(rawVectorDef.getName())
                        .criteria(
                            ApproximateVectorQueryCriteriaBuilder.builder()
                                .limit(10)
                                .numCandidates(100)
                                .queryVector(queryVector)
                                .path(userPath)
                                .build())
                        .returnStoredSource(true)
                        .build())
                .build(),
            indexCatalog,
            initializedIndexCatalog,
            BOOTSTRAPPER_METADATA,
            Suppliers.ofInstance(mock(EmbeddingServiceManager.class)),
            new VectorSearchCommand.Metrics(
                new MetricsFactory("mockNamespace", new SimpleMeterRegistry())),
            mock(MongotCursorManager.class),
            CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    BsonDocument result = command.run();

    assertThat(result.getInt32("ok").getValue()).isEqualTo(0);
    assertThat(result.getString("errmsg").getValue())
        .contains("storedSource is not configured for this index");
  }

  private static FieldPath derivedVectorFieldPath(VectorIndexDefinition derivedDef) {
    return derivedDef.getFields().stream()
        .filter(field -> field.getType() == VectorIndexFieldDefinition.Type.VECTOR)
        .findFirst()
        .orElseThrow(() -> new AssertionError("derived definition has no vector field"))
        .getPath();
  }

  private static void indexVectorDoc(
      IndexWriter writer, ObjectId indexId, int docId, FieldPath vectorPath, Vector vector)
      throws Exception {
    BsonDocument document = new BsonDocument().append("_id", new BsonInt32(docId));
    appendNested(
        document, ImmutableList.copyOf(vectorPath.getSegments()), BsonVectorParser.encode(vector));
    document.append(indexId.toString(), new BsonDocument("_id", new BsonInt32(docId)));
    RawBsonDocument raw = BsonUtils.documentToRaw(document);
    writer.updateIndex(
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(raw), indexId), raw));
  }

  private static void appendNested(
      BsonDocument root, ImmutableList<String> segments, BsonValue leafValue) {
    root.append(segments.get(0), nest(segments, 1, leafValue));
  }

  private static BsonValue nest(ImmutableList<String> segments, int index, BsonValue leafValue) {
    if (index == segments.size()) {
      return leafValue;
    }
    return new BsonDocument(segments.get(index), nest(segments, index + 1, leafValue));
  }

  private static LuceneIndexFactory openFactory() throws Exception {
    var config =
        LuceneConfigBuilder.builder()
            .dataPath(TestUtils.getTempFolder().getRoot().toPath())
            .build();
    return LuceneIndexFactory.fromConfig(
        config,
        FeatureFlags.getDefault(),
        new DynamicFeatureFlagRegistry(
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
        EnvironmentVariantPerfConfig.getDefault(),
        MeterAndFtdcRegistry.createWithSimpleRegistries(),
        Optional.empty(),
        AnalyzerRegistry.factory(),
        new NoOpDiskMonitor());
  }
}
