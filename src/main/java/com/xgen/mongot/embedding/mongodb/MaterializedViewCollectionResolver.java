package com.xgen.mongot.embedding.mongodb;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.CURRENT_MAT_VIEW_SCHEMA_VERSION;
import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO;
import static com.xgen.mongot.util.mongodb.MongoDbDatabase.getCollectionInfo;
import static com.xgen.mongot.util.mongodb.MongoDbDatabase.getCollectionInfos;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.common.InternalDatabaseResolver;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.embedding.utils.AutoEmbedFieldMappingCreator;
import com.xgen.mongot.embedding.utils.MongoClientOperationExecutor;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification;
import com.xgen.mongot.index.definition.VectorTextFieldSpecification;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import com.xgen.mongot.util.retry.ExponentialBackoffPolicy;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the logic to determine which materialized view collection to use for a given index
 * definition. Responsible for discovering existing collections and creating new ones as needed.
 */
public class MaterializedViewCollectionResolver {

  private static final Logger LOG =
      LoggerFactory.getLogger(MaterializedViewCollectionResolver.class);

  public static final String MV_COLLECTION_SCHEMA_NAMESPACE = "_autoEmbed";
  private static final String DELIM = "-";
  // Using a character not allowed in field attributes to avoid collisions.
  private static final String HASH_STRING_DELIM = ";";
  private static final int NAMESPACE_EXISTS_ERROR_CODE = 48;
  // Length of the hash in bytes before hex encoding (16 bytes -> 32 hex chars). We need to
  // truncate the hash to ensure the collection name does not exceed the limits imposed by Atlas
  // (95 bytes).
  private static final int DEFINITION_HASH_BYTES = 16;

  /**
   * Parent metrics namespace; combined with {@link #METRICS_RESOURCE_NAME} this produces metric
   * names of the form {@code embedding.materializedView.collectionResolver.requestLatency} etc.
   * Mirrors the namespace used by other auto-embed components such as
   * {@link com.xgen.mongot.index.autoembedding.MaterializedViewIndexFactory}.
   */
  private static final String METRICS_NAMESPACE = "embedding.materializedView";

  /** Per-resource metric name under {@link #METRICS_NAMESPACE}. */
  private static final String METRICS_RESOURCE_NAME = "collectionResolver";

  private final MaterializedViewCollectionMetadataCatalog metadataCatalog;

  private final LeaseManager leaseManager;

  private final AutoEmbeddingMaterializedViewConfig materializedViewConfig;

  private final InternalDatabaseResolver dbResolver;

  private final AutoEmbeddingMongoClient autoEmbeddingMongoClient;

  /**
   * Wraps the resolver's direct MongoDB calls (listCollections, createCollection) with retries on
   * transient errors. Lease operations called from this class are already retried by the lease
   * manager's own executor; this executor covers the remaining DB operations on the index init
   * path. See {@link com.xgen.mongot.embedding.utils.MongoClientOperationExecutor} for the set of
   * exceptions classified as retryable (e.g. MongoTimeoutException, MongoSocketException).
   */
  private final MongoClientOperationExecutor operationExecutor;

  /** Constructs the resolver. Use {@link #create} from production code. */
  public MaterializedViewCollectionResolver(
      InternalDatabaseResolver dbResolver,
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MaterializedViewCollectionMetadataCatalog metadataCatalog,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig,
      LeaseManager leaseManager,
      MeterRegistry meterRegistry) {
    this.autoEmbeddingMongoClient = autoEmbeddingMongoClient;
    this.metadataCatalog = metadataCatalog;
    this.materializedViewConfig = materializedViewConfig;
    this.leaseManager = leaseManager;
    this.dbResolver = dbResolver;
    // Tight retry budget: 2 retries (3 attempts total). Each attempt absorbs the driver's 10s
    // server-selection timeout; combined with backoff this gives a ~31s in-place budget which
    // covers the typical mongod unreachability window during a maintenance restart. Longer
    // outages fall through to the async retry path via IndexRecoveryStager rather than
    // blocking journal-restore / conf-call processing on this thread further. The retry loop
    // aborts immediately if the underlying mongo client is closed (mongot shutdown) so we don't
    // delay shutdown by the retry budget.
    this.operationExecutor =
        new MongoClientOperationExecutor(
            new MetricsFactory(METRICS_NAMESPACE, meterRegistry),
            METRICS_RESOURCE_NAME,
            ExponentialBackoffPolicy.builder()
                .initialDelay(Duration.ofMillis(500))
                .backoffFactor(2)
                .maxDelay(Duration.ofMillis(1000))
                .maxRetries(2)
                .jitter(0.1)
                .build(),
            autoEmbeddingMongoClient::isClosed);
  }

  public static MaterializedViewCollectionResolver create(
      InternalDatabaseResolver dbResolver,
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MaterializedViewCollectionMetadataCatalog metadataCatalog,
      LeaseManager leaseManager,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig,
      MeterRegistry meterRegistry) {
    // TODO(CLOUDP-360542): Support sync source change.
    return new MaterializedViewCollectionResolver(
        dbResolver,
        autoEmbeddingMongoClient,
        metadataCatalog,
        materializedViewConfig,
        leaseManager,
        meterRegistry);
  }

  /**
   * Main entry point for the resolver. Uses the given index definition to determine whether any
   * existing materialized view collections can be re-used, or if a new one needs to be created.
   *
   * @param indexDefinitionGeneration The index definition generation to use.
   * @return A metadata object encapsulating details like the collection name and UUID.
   * @throws MaterializedViewTransientException if materializedViewCollectionMongoClient is missing
   *     due to sync source, or materializedViewCollectionMongoClient is closed during sync source
   *     update or lease manager error. Caller will retry on this exception.
   */
  public MaterializedViewCollectionMetadata getOrCreateMaterializedViewForIndex(
      MaterializedViewIndexDefinitionGeneration indexDefinitionGeneration)
      throws MaterializedViewTransientException {
    // Fail fast if no client has been registered yet. Each retried operation below additionally
    // re-fetches via requireResolverClient() so a sync-source rotation mid-call (which closes
    // and replaces the resolver client) doesn't leave us retrying against a torn-down reference;
    // mirrors how DynamicLeaderLeaseManager re-fetches getLeaseManagerMongoClient() per call.
    if (this.autoEmbeddingMongoClient.getMaterializedViewResolverMongoClient().isEmpty()) {
      throw new MaterializedViewTransientException(
          "Missing materialized view collection client",
          MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE);
    }
    String matViewDb =
        this.dbResolver.resolve(indexDefinitionGeneration.getIndexDefinition().getDatabase());
    try {
      var collectionName = getOrCreateCollectionName(indexDefinitionGeneration, matViewDb);
      MongoDbCollectionInfo collectionInfo =
          this.operationExecutor.execute(
              "getCollectionInfo",
              () -> getCollectionInfo(requireResolverClient(), matViewDb, collectionName));

      MaterializedViewCollectionMetadata materializedViewCollectionMetadata =
          this.leaseManager.initializeLease(
              indexDefinitionGeneration,
              createProposedMetadata(
                  collectionName,
                  Check.instanceOf(collectionInfo, MongoDbCollectionInfo.Collection.class)
                      .info()
                      .uuid(),
                  indexDefinitionGeneration.getIndexDefinition(),
                  this.materializedViewConfig.materializedViewSchemaVersion.orElse(
                      CURRENT_MAT_VIEW_SCHEMA_VERSION)));
      try {
        this.leaseManager.executeOpsCommandsAfterInitializeLease(
            materializedViewCollectionMetadata.collectionName());
      } catch (RuntimeException e) {
        LOG.atError()
            .setCause(e)
            .addKeyValue("collectionName", materializedViewCollectionMetadata.collectionName())
            .log("Ops hook failed during MV initialization; continuing catalog registration");
      }
      this.metadataCatalog.addMetadata(
          indexDefinitionGeneration.getGenerationId(), materializedViewCollectionMetadata);
      this.metadataCatalog.addDatabaseName(indexDefinitionGeneration.getGenerationId(), matViewDb);
      LOG.atInfo()
          .addKeyValue("generationId", indexDefinitionGeneration.getGenerationId())
          .addKeyValue("collectionName", collectionName)
          .addKeyValue("database", matViewDb)
          .log("Registered materialized view metadata and database name");
      return materializedViewCollectionMetadata;
    } catch (MaterializedViewTransientException e) {
      // Preserve the executor / lease manager's classified reason (e.g. EXCEEDED_DISK_LIMIT,
      // USER_WRITES_BLOCKED, SYSTEM_OVERLOADED, LEASE_OPERATION_FAILED) instead of collapsing
      // every transient failure into COLLECTION_RESOLUTION_FAILED.
      LOG.atWarn()
          .addKeyValue("generationId", indexDefinitionGeneration.getGenerationId())
          .addKeyValue("database", matViewDb)
          .addKeyValue("reason", e.getReason())
          .setCause(e)
          .log("Failed to resolve materialized view collection");
      throw e;
    } catch (Exception e) {
      LOG.atWarn()
          .addKeyValue("generationId", indexDefinitionGeneration.getGenerationId())
          .addKeyValue("database", matViewDb)
          .setCause(e)
          .log("Failed to resolve materialized view collection");
      throw new MaterializedViewTransientException(
          String.valueOf(e.getMessage()),
          e,
          MaterializedViewTransientException.Reason.COLLECTION_RESOLUTION_FAILED);
    }
  }

  /**
   * Returns the current resolver mongo client from the {@link AutoEmbeddingMongoClient}'s atomic
   * reference, throwing {@link MaterializedViewTransientException} (retryable) if absent. Called
   * fresh per-attempt inside retried suppliers so a sync-source rotation during retry picks up
   * the new client rather than continuing against a closed one.
   */
  private MongoClient requireResolverClient() throws MaterializedViewTransientException {
    return this.autoEmbeddingMongoClient
        .getMaterializedViewResolverMongoClient()
        .orElseThrow(
            () ->
                new MaterializedViewTransientException(
                    "Missing materialized view collection client",
                    MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE));
  }

  private String getOrCreateCollectionName(
      IndexDefinitionGeneration indexDefinitionGeneration, String matViewDb) throws Exception {
    long matViewNameFormatVersion =
        indexDefinitionGeneration
            .getIndexDefinition()
            .getMaterializedViewNameFormatVersion()
            .orElse(this.materializedViewConfig.defaultMaterializedViewNameFormatVersion);
    String uniqueIndexId =
        indexDefinitionGeneration
            .getIndexDefinition()
            .getIndexIdAtCreationTime()
            .orElse(indexDefinitionGeneration.getIndexId())
            .toHexString();
    long autoEmbeddingDefinitionVersion =
        indexDefinitionGeneration
            .getIndexDefinition()
            .getAutoEmbeddingDefinitionVersion()
            .orElse(0L);

    String collectionName;
    // v0 uses indexId as collection name
    if (matViewNameFormatVersion == 0L) {
      collectionName = uniqueIndexId;
    } else {
      var hash =
          computeHash(
              indexDefinitionGeneration.asMaterializedView().getIndexDefinition());
      collectionName =
          uniqueIndexId
              + DELIM
              + hash
              + DELIM
              + matViewNameFormatVersion
              + DELIM
              + autoEmbeddingDefinitionVersion;
    }
    String resolvedName = collectionName;
    boolean collectionExists =
        this.operationExecutor
            .execute(
                "getCollectionInfos",
                () ->
                    getCollectionInfos(requireResolverClient(), matViewDb)
                        .getCollectionInfo(matViewDb, resolvedName))
            .isPresent();
    if (!collectionExists) {
      // Create new mat view collection when it's not found.
      createCollection(matViewDb, collectionName);
      LOG.atInfo()
          .addKeyValue("collectionName", collectionName)
          .addKeyValue("database", matViewDb)
          .log("Created new materialized view collection");
    } else {
      LOG.atInfo()
          .addKeyValue("collectionName", collectionName)
          .addKeyValue("database", matViewDb)
          .log("Reusing existing materialized view collection");
    }
    return collectionName;
  }

  private MaterializedViewCollectionMetadata createProposedMetadata(
      String collectionName,
      UUID uuid,
      IndexDefinition indexDefinition,
      int mvSchemaVersion) {
    if (mvSchemaVersion == 0) {
      return new MaterializedViewCollectionMetadata(VERSION_ZERO, uuid, collectionName);
    } else if (mvSchemaVersion == 1) {
      AutoEmbedFieldMapping mapping =
          AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition);
      ImmutableMap<FieldPath, FieldPath> schemaFieldsMapping =
          mapping.embedFields().entrySet().stream()
              .collect(
                  toImmutableMap(
                      Map.Entry::getKey,
                      entry ->
                          FieldPath.parse(
                              MV_COLLECTION_SCHEMA_NAMESPACE
                                  + FieldPath.DELIMITER
                                  + entry.getKey().toString())));
      return new MaterializedViewCollectionMetadata(
          new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(
              mvSchemaVersion, schemaFieldsMapping),
          uuid,
          collectionName);
    }
    throw new IllegalArgumentException(
        "Unsupported materialized view schema version: " + mvSchemaVersion);
  }

  /**
   * Creates a new materialized view collection with the given name while gracefully handling the
   * case where the collection already exists.
   */
  private void createCollection(String matViewDb, String collectionName) throws Exception {
    this.operationExecutor.execute(
        "createCollection",
        () -> {
          try {
            requireResolverClient().getDatabase(matViewDb).createCollection(collectionName);
          } catch (MongoCommandException e) {
            if (e.getErrorCode() != NAMESPACE_EXISTS_ERROR_CODE) {
              throw e;
            }
            // Collection already exists — another instance created it concurrently. Treated as a
            // success so the executor's failedRequests counter doesn't tick on the (common) case
            // where we re-resolve a previously-created MV collection.
            LOG.atInfo()
                .addKeyValue("collectionName", collectionName)
                .addKeyValue("database", matViewDb)
                .log("MatView collection already exists, skipping creation");
          }
          return null;
        });
  }

  /**
   * Computes a hash of the auto-embed fields in an index definition. Convenience overload that
   * extracts auto-embed fields before delegating to {@link #computeHash(AutoEmbedFieldMapping)}.
   */
  public static String computeHash(IndexDefinition indexDefinition) {
    return computeHash(AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition));
  }

  /**
   * Computes a hash of the auto-embed field mapping. The hash is used to determine if the index
   * definition has changed in a way that requires a new materialized view collection.
   *
   * <p>The hash includes path, model name, modality, num dimensions, and auto-embed quantization
   * for each AUTO_EMBED field. Legacy TEXT fields are not valid inputs.
   */
  public static String computeHash(AutoEmbedFieldMapping mapping) {
    List<Map.Entry<FieldPath, AutoEmbedFieldMapping.AutoEmbedField.EmbedField>> sortedEntries =
        mapping.embedFields().entrySet().stream()
            .sorted(Comparator.comparing(e -> e.getKey().toString()))
            .toList();

    for (var entry : sortedEntries) {
      switch (entry.getValue().specification()) {
        case VectorAutoEmbedFieldSpecification ignored -> {
          // do nothing, this is what we want here
        }
        case VectorTextFieldSpecification ignored ->
            throw new IllegalArgumentException(
                "Legacy TEXT field specification is not supported for materialized view hash"
                    + " computation: "
                    + entry.getKey());
      }
    }

    StringBuilder sb = new StringBuilder();
    for (var entry : sortedEntries) {
      FieldPath path = entry.getKey();
      VectorAutoEmbedFieldSpecification spec =
          (VectorAutoEmbedFieldSpecification) entry.getValue().specification();
      // Include path, model name, modality, num dimensions, and quantization in hash as those
      // fields impact the embeddings generated or how they are stored.
      sb.append(path.toString())
          .append(HASH_STRING_DELIM)
          .append(spec.modelName())
          .append(HASH_STRING_DELIM)
          .append(spec.modality())
          .append(HASH_STRING_DELIM)
          .append(spec.numDimensions())
          .append(HASH_STRING_DELIM)
          .append(spec.autoEmbedQuantization());
      // For future reference: Include additional field params conditionally for newer hash versions
      // as needed. Note that default values need careful handling to ensure that simply bumping the
      // hash version doesn't change the hash value for all indexes.
    }
    byte[] hashBytes = Hashing.sha256().hashString(sb.toString(), StandardCharsets.UTF_8).asBytes();
    byte[] truncated = Arrays.copyOf(hashBytes, DEFINITION_HASH_BYTES);
    return BaseEncoding.base16().lowerCase().encode(truncated);
  }
}
