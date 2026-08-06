package com.xgen.mongot.index.autoembedding;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingIndexDefinitionUtils.getDerivedVectorIndexDefinition;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexWriter;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.index.lucene.LuceneIndexFactory;
import com.xgen.mongot.index.lucene.directory.EnvironmentVariantPerfConfig;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryHelper;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.query.QueryExecutionContext;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.monitor.NoOpDiskMonitor;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingCommitMetadata;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.BsonVectorParser;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.LuceneConfigBuilder;
import com.xgen.testing.mongot.index.query.ApproximateVectorQueryCriteriaBuilder;
import com.xgen.testing.mongot.index.query.VectorQueryBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * Proves that the {@link AutoEmbeddingCommitMetadata} persisted in a derived Lucene index's commit
 * is by itself sufficient to serve a $vectorSearch when the materialized view cannot be resolved
 * (mongod unreachable) -- no materialized view, lease, or mongod involved.
 *
 * <p>The test writes a derived Lucene vector index to disk (with one vector) through the real
 * {@link LuceneIndexFactory}, commits {@code AutoEmbeddingCommitMetadata} carrying the
 * reconstruction inputs, closes it, then reconstructs a queryable index purely from that on-disk
 * commit and asserts the query returns the indexed document.
 */
public class AutoEmbeddingOfflineQueryTest {

  // Tenant-namespaced internal-search database: on a multi-tenant cluster this is not a constant,
  // so it must come from the persisted metadata rather than being recomputed offline.
  private static final String MV_DATABASE_NAME = "acme___mdb_internal_search";
  private static final UUID MV_COLLECTION_UUID = UUID.randomUUID();
  private static final int DIMENSIONS = 4;

  @Test
  public void reconstructOfflineDerivedVectorIndex_servesQueryFromCommitMetadataWithoutMatView()
      throws Exception {
    ObjectId indexId = new ObjectId();

    // Raw auto-embedding vector definition -- the control-plane config, available on disk offline.
    VectorIndexDefinition rawVectorDef =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .name("vec")
            .database("sample_mflix")
            .lastObservedCollectionName("movies")
            .collectionUuid(UUID.randomUUID())
            .withAutoEmbedField(
                "title",
                "voyage-3-large",
                DIMENSIONS,
                VectorAutoEmbedQuantization.FLOAT,
                VectorSimilarity.COSINE)
            .build();
    VectorIndexDefinitionGeneration rawGen =
        new VectorIndexDefinitionGeneration(rawVectorDef, Generation.CURRENT);

    // MV-sourced inputs that are NOT derivable from the raw definition: schema field mapping,
    // collection UUID, and (tenant-namespaced) database name. These are what the commit persists.
    MaterializedViewSchemaMetadata schema =
        new MaterializedViewSchemaMetadata(
            1L, Map.of(FieldPath.parse("title"), FieldPath.parse("_autoEmbed.title")));
    MaterializedViewCollectionMetadata collectionMetadata =
        new MaterializedViewCollectionMetadata(schema, MV_COLLECTION_UUID, indexId.toHexString());

    VectorIndexDefinition derivedDef =
        getDerivedVectorIndexDefinition(rawVectorDef, MV_DATABASE_NAME, MV_COLLECTION_UUID, schema);
    VectorIndexDefinitionGeneration derivedGen =
        new VectorIndexDefinitionGeneration(derivedDef, Generation.CURRENT);
    FieldPath vectorPath = derivedVectorFieldPath(derivedDef);

    Vector docVector =
        Vector.fromFloats(new float[] {1f, 0f, 0f, 0f}, FloatVector.OriginalType.NATIVE);

    try (LuceneIndexFactory factory = openFactory()) {
      // 1) Write the derived Lucene index to disk with one vector, then commit the metadata that
      // offline reconstruction will rely on. Closing releases the writer/reader.
      Index derivedIndex = factory.getIndex(derivedGen);
      InitializedIndex initialized = factory.getInitializedIndex(derivedIndex, derivedGen);
      InitializedVectorIndex liveIndex =
          Check.instanceOf(initialized, InitializedVectorIndex.class);
      indexVectorDoc(liveIndex.getWriter(), indexId, 1, vectorPath, docVector);
      IndexCommitUserData userData =
          IndexCommitUserData.EMPTY.withAutoEmbeddingMetadata(
              new AutoEmbeddingCommitMetadata(
                  true, Optional.of(MV_DATABASE_NAME), Optional.of(collectionMetadata)));
      liveIndex.getWriter().commit(userData.toEncodedData());
      initialized.close();

      // 2) Simulate offline serving: reconstruct the derived index using ONLY the persisted
      // AutoEmbeddingCommitMetadata (no materialized view, lease, or mongod).
      Optional<InitializedIndex> reconstructed =
          AutoEmbeddingIndexGenerationFactory.reconstructOfflineDerivedVectorIndex(factory, rawGen);
      assertThat(reconstructed).isPresent();
      assertThat(reconstructed.get()).isInstanceOf(InitializedVectorIndex.class);
      InitializedVectorIndex vectorIndex = (InitializedVectorIndex) reconstructed.get();

      // The metadata alone rebuilt the exact derived definition, including the tenant-namespaced
      // internal-search database name that cannot be a compile-time constant offline.
      assertThat(vectorIndex.getDefinition()).isEqualTo(derivedDef);

      // 3) The reconstructed index serves the query: the indexed vector comes back.
      vectorIndex.setStatus(IndexStatus.steady());
      VectorIndexReader reader = vectorIndex.getReader();
      reader.refresh();
      BsonArray results = query(reader, derivedDef.getName(), vectorPath, docVector);
      assertThat(results).hasSize(1);
      assertThat(results.get(0).asDocument().getInt32("_id").getValue()).isEqualTo(1);

      vectorIndex.close();
    }
  }

  @Test
  public void sourceCollectionAbsentWithPersistedCommit_dropDeletesDerivedDirectories()
      throws Exception {
    Path dataPath = TestUtils.getTempFolder().getRoot().toPath();
    ObjectId indexId = new ObjectId();
    VectorIndexDefinition rawVectorDef =
        VectorIndexDefinitionBuilder.builder()
            .indexId(indexId)
            .name("vec")
            .database("sample_mflix")
            .lastObservedCollectionName("movies")
            .collectionUuid(UUID.randomUUID())
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
            1L, Map.of(FieldPath.parse("title"), FieldPath.parse("_autoEmbed.title")));
    MaterializedViewCollectionMetadata collectionMetadata =
        new MaterializedViewCollectionMetadata(schema, MV_COLLECTION_UUID, indexId.toHexString());
    VectorIndexDefinition derivedDef =
        getDerivedVectorIndexDefinition(rawVectorDef, MV_DATABASE_NAME, MV_COLLECTION_UUID, schema);
    VectorIndexDefinitionGeneration derivedGen =
        new VectorIndexDefinitionGeneration(derivedDef, Generation.CURRENT);
    String generationName = IndexDirectoryHelper.getNameForGeneration(rawGen);
    Path indexPath = dataPath.resolve(generationName);
    Path metadataPath = dataPath.resolve("indexMetadata").resolve(generationName);

    try (LuceneIndexFactory factory = openFactory(dataPath)) {
      Index derivedIndex = factory.getIndex(derivedGen);
      InitializedVectorIndex initialized =
          Check.instanceOf(
              factory.getInitializedIndex(derivedIndex, derivedGen), InitializedVectorIndex.class);
      initialized
          .getWriter()
          .commit(
              IndexCommitUserData.EMPTY
                  .withAutoEmbeddingMetadata(
                      new AutoEmbeddingCommitMetadata(
                          false, Optional.of(MV_DATABASE_NAME), Optional.of(collectionMetadata)))
                  .toEncodedData());
      initialized.close();
    }
    assertThat(Files.exists(indexPath)).isTrue();
    assertThat(Files.exists(metadataPath)).isTrue();

    // Reopen from the same data path to model a former owner restarting after source deletion.
    try (LuceneIndexFactory factory = openFactory(dataPath)) {
      MaterializedViewIndexFactory matViewIndexFactory = mock(MaterializedViewIndexFactory.class);
      when(matViewIndexFactory.isSourceCollectionAbsentOnMongod(any())).thenReturn(true);
      when(matViewIndexFactory.createAutoEmbeddingGenerationMetrics(any()))
          .thenReturn(mock(AutoEmbeddingGenerationMetrics.class));
      IndexGeneration unresolved =
          AutoEmbeddingIndexGenerationFactory.getAutoEmbeddingIndexGeneration(
              factory,
              matViewIndexFactory,
              rawGen,
              mock(InitializedIndexCatalog.class),
              () -> false);

      assertThat(unresolved.getIndex().getStatus().getStatusCode())
          .isEqualTo(IndexStatus.StatusCode.DOES_NOT_EXIST);
      assertThat(unresolved.getIndex().getStatus().getReason())
          .hasValue(IndexStatus.Reason.COLLECTION_NOT_FOUND);
      assertThat(
              ((UnresolvedAutoEmbeddingIndex) unresolved.getIndex()).getReconstructedDerivedIndex())
          .isEmpty();

      unresolved.getIndex().close();
      unresolved.getIndex().drop();

      assertThat(Files.exists(indexPath)).isFalse();
      assertThat(Files.exists(metadataPath)).isFalse();
    }
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
    // Nest the vector under the field path's segments (e.g. _autoEmbed.title ->
    // {_autoEmbed: {title: <vector>}}) so the document matches a real materialized-view document;
    // a single dotted key would be a shape MongoDB cannot produce.
    appendNested(
        document, ImmutableList.copyOf(vectorPath.getSegments()), BsonVectorParser.encode(vector));
    // The metadata namespace is keyed by the index's real indexId (as in production), not a fresh
    // ObjectId, so the document shape matches what the derived indexer actually reads.
    document.append(indexId.toString(), new BsonDocument("_id", new BsonInt32(docId)));
    RawBsonDocument raw = BsonUtils.documentToRaw(document);
    writer.updateIndex(
        DocumentEvent.createInsert(
            DocumentMetadata.fromMetadataNamespace(Optional.of(raw), indexId), raw));
  }

  /** Places {@code leafValue} at the nested location described by {@code segments} under root. */
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

  private static BsonArray query(
      VectorIndexReader reader, String indexName, FieldPath vectorPath, Vector queryVector)
      throws Exception {
    VectorSearchCriteria criteria =
        ApproximateVectorQueryCriteriaBuilder.builder()
            .queryVector(queryVector)
            .path(vectorPath)
            .numCandidates(100)
            .limit(10)
            .build();
    VectorSearchQuery vectorQuery =
        VectorQueryBuilder.builder().index(indexName).criteria(criteria).build();
    return reader.query(
        new MaterializedVectorSearchQuery(vectorQuery, vectorQuery.criteria().queryVector().get()),
        QueryExecutionContext.empty());
  }

  private static LuceneIndexFactory openFactory() throws Exception {
    return openFactory(TestUtils.getTempFolder().getRoot().toPath());
  }

  private static LuceneIndexFactory openFactory(Path dataPath) throws Exception {
    var config = LuceneConfigBuilder.builder().dataPath(dataPath).build();
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
