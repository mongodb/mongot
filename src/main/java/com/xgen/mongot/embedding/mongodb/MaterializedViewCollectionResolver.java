package com.xgen.mongot.embedding.mongodb;

import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.CURRENT_MAT_VIEW_SCHEMA_VERSION;
import static com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata.VERSION_ZERO;
import static com.xgen.mongot.util.mongodb.MongoDbDatabase.getCollectionInfo;
import static com.xgen.mongot.util.mongodb.MongoDbDatabase.getCollectionInfos;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.CreateCollectionOptions;
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
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;
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
   * Mirrors the namespace used by other auto-embed components such as {@link
   * com.xgen.mongot.index.autoembedding.MaterializedViewIndexFactory}.
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
          this.leaseManager
              .initializeLease(
                  indexDefinitionGeneration,
                  createProposedMetadata(
                      collectionName,
                      Check.instanceOf(collectionInfo, MongoDbCollectionInfo.Collection.class)
                          .info()
                          .uuid(),
                      indexDefinitionGeneration.getIndexDefinition(),
                      this.materializedViewConfig.materializedViewSchemaVersion.orElse(
                          CURRENT_MAT_VIEW_SCHEMA_VERSION)))
              // initializeLease returns the persisted lease metadata when it already exists (normal
              // restart), discarding the freshly-proposed one. Re-attach the mat view's collation
              // from the same getCollectionInfo read above, so the resync scanner can decide the
              // comparator without a best-effort network read. The collation is immutable, so this
              // is
              // always the source collation for a collated mat view.
              .withCollation(
                  Check.instanceOf(collectionInfo, MongoDbCollectionInfo.Collection.class)
                      .options()
                      .collation());
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
   * fresh per-attempt inside retried suppliers so a sync-source rotation during retry picks up the
   * new client rather than continuing against a closed one.
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
      var hash = computeHash(indexDefinitionGeneration.asMaterializedView().getIndexDefinition());
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
      // Create new mat view collection when it's not found. Seed it with the source collection's
      // collation so its _id index orders string _ids the same way the source cursor does: the
      // resync sort-merge takes its mat-view window bounds from the source cursor order, so the two
      // server-side orders have to agree for each window to be visited exactly once.
      createCollection(matViewDb, collectionName, readSourceCollation(indexDefinitionGeneration));
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
      String collectionName, UUID uuid, IndexDefinition indexDefinition, int mvSchemaVersion) {
    if (mvSchemaVersion == 0) {
      return new MaterializedViewCollectionMetadata(VERSION_ZERO, uuid, collectionName);
    } else if (mvSchemaVersion == 1) {
      AutoEmbedFieldMapping mapping =
          AutoEmbedFieldMappingCreator.createAutoEmbedMapping(indexDefinition);
      // AUTO_EMBED (and legacy TEXT) source paths are always remapped into the MV namespace. Use a
      // map so vector FILTER remapping below can be layered on without duplicate-key conflicts.
      Map<FieldPath, FieldPath> schemaFields = new LinkedHashMap<>();
      mapping
          .embedFields()
          .keySet()
          .forEach(path -> schemaFields.put(path, toMvNamespacePath(path)));
      // CLOUDP-402522: for vector auto-embed indexes with a nestedRoot, also remap FILTER fields
      // whose path is a strict descendant of the source nestedRoot. They must live in the same MV
      // namespace as the auto-embedded vector so block-join indexing places them on the same
      // Lucene child documents. Top-level filters and fields outside the nestedRoot keep their
      // source path.
      if (indexDefinition instanceof VectorIndexDefinition vectorDef) {
        Optional<FieldPath> sourceNestedRoot = vectorDef.getNestedRoot();
        vectorDef.getMappings().fieldMap().entrySet().stream()
            .filter(entry -> shouldRemapFieldToMvNamespace(entry.getValue(), sourceNestedRoot))
            .forEach(entry -> schemaFields.put(entry.getKey(), toMvNamespacePath(entry.getKey())));
      }
      ImmutableMap<FieldPath, FieldPath> schemaFieldsMapping = ImmutableMap.copyOf(schemaFields);
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
   * Returns true when the given field's path must be remapped into the {@link
   * #MV_COLLECTION_SCHEMA_NAMESPACE} namespace in the materialized view. AUTO_EMBED fields are
   * always remapped (vectors are written to the MV at the prefixed path). FILTER fields are
   * remapped only when their path is a strict descendant of the source {@code nestedRoot}, so that
   * they live alongside the embedded vector in the Lucene block structure (CLOUDP-402522).
   */
  private static boolean shouldRemapFieldToMvNamespace(
      VectorIndexFieldDefinition field, Optional<FieldPath> sourceNestedRoot) {
    return switch (field.getType()) {
      case AUTO_EMBED -> true;
      case FILTER ->
          AutoEmbedFieldMappingCreator.isFilterUnderSourceNestedRoot(
              field.getPath(), sourceNestedRoot);
      default -> false;
    };
  }

  /** Prefixes a source field path with the materialized-view schema namespace. */
  private static FieldPath toMvNamespacePath(FieldPath sourcePath) {
    return FieldPath.parse(
        MV_COLLECTION_SCHEMA_NAMESPACE + FieldPath.DELIMITER + sourcePath.toString());
  }

  /**
   * Creates a new materialized view collection with the given name while gracefully handling the
   * case where the collection already exists.
   */
  private void createCollection(
      String matViewDb, String collectionName, Optional<BsonDocument> sourceCollation)
      throws Exception {
    this.operationExecutor.execute(
        "createCollection",
        () -> {
          try {
            // Only pass collation options when the source has a non-simple collation; the plain
            // name-only createCollection keeps the common simple-collation case on the existing
            // path.
            if (sourceCollation.isEmpty()) {
              requireResolverClient().getDatabase(matViewDb).createCollection(collectionName);
            } else {
              var createOptions = new CreateCollectionOptions();
              createOptions.collation(toDriverCollation(sourceCollation.get()));
              requireResolverClient()
                  .getDatabase(matViewDb)
                  .createCollection(collectionName, createOptions);
            }
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

  // Reads the source collection's default collation, so a newly created mat view can be created
  // with
  // the same collation. Empty for a simple collation or an absent source (in which case the mat
  // view
  // keeps the default simple collation). Read through the operation executor so a transient error
  // is
  // retried and propagates rather than silently creating a simple mat view for a collated source (a
  // simple mat view that got created this way would be reused and never corrected).
  private Optional<BsonDocument> readSourceCollation(IndexDefinitionGeneration g) throws Exception {
    var def = g.getIndexDefinition();
    return this.operationExecutor.execute(
        "getCollectionInfo",
        () ->
            getCollectionInfos(requireResolverClient(), def.getDatabase())
                .getCollectionInfo(def.getDatabase(), def.getLastObservedCollectionName())
                .filter(MongoDbCollectionInfo.Collection.class::isInstance)
                .map(MongoDbCollectionInfo.Collection.class::cast)
                .map(MongoDbCollectionInfo.Collection::options)
                .flatMap(MongoDbCollectionInfo.Collection.Options::collation));
  }

  // Builds a driver Collation from a MongoDB collation spec (as reported by listCollections),
  // mapping
  // the documented option values. Mirrors mongot's collation-parity harness mappings.
  private static Collation toDriverCollation(BsonDocument spec) {
    int strength = Math.max(1, Math.min(5, intOr(spec, "strength", 3)));
    boolean caseLevel = boolOr(spec, "caseLevel", false);
    String caseFirst = strOr(spec, "caseFirst", "off");
    var builder =
        Collation.builder()
            .locale(strOr(spec, "locale", "simple"))
            .collationStrength(CollationStrength.valueOf(STRENGTH_NAMES[strength - 1]))
            .caseLevel(caseLevel)
            .numericOrdering(boolOr(spec, "numericOrdering", false))
            .backwards(boolOr(spec, "backwards", false))
            .normalization(boolOr(spec, "normalization", false))
            .collationAlternate(
                "shifted".equals(strOr(spec, "alternate", "non-ignorable"))
                    ? CollationAlternate.SHIFTED
                    : CollationAlternate.NON_IGNORABLE);
    // caseFirst applies when caseLevel is on OR strength >= 3 (MongoDB compares case at tertiary+
    // regardless of caseLevel); a strength-1/2 + caseLevel:false source has no case to order.
    // TODO(CLOUDP-448794): maxVariable is deliberately not mapped — see CollationUtils for why. It
    // must be mapped in BOTH the mat-view collation and the ICU comparator; doing it here alone
    // (CollationMaxVariable) without the comparator would make them disagree.
    if (!"off".equals(caseFirst) && (caseLevel || strength >= 3)) {
      builder.collationCaseFirst(CollationCaseFirst.valueOf(caseFirst.toUpperCase(Locale.ROOT)));
    }
    return builder.build();
  }

  private static final String[] STRENGTH_NAMES = {
    "PRIMARY", "SECONDARY", "TERTIARY", "QUATERNARY", "IDENTICAL"
  };

  private static String strOr(BsonDocument doc, String key, String def) {
    BsonValue v = doc.get(key);
    return v instanceof BsonString ? v.asString().getValue() : def;
  }

  private static int intOr(BsonDocument doc, String key, int def) {
    // Accept any BSON numeric type for a numeric option (e.g. strength) — see CollationUtils.intOr.
    BsonValue v = doc.get(key);
    return v instanceof BsonNumber ? ((BsonNumber) v).intValue() : def;
  }

  private static boolean boolOr(BsonDocument doc, String key, boolean def) {
    BsonValue v = doc.get(key);
    return v instanceof BsonBoolean ? v.asBoolean().getValue() : def;
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
      VectorAutoEmbedFieldSpecification.MaterializedViewIdentity identity =
          spec.materializedViewIdentity();
      // Include path, model name, modality, num dimensions, and quantization in hash as those
      // fields impact the embeddings generated or how they are stored.
      sb.append(path.toString())
          .append(HASH_STRING_DELIM)
          .append(identity.modelName())
          .append(HASH_STRING_DELIM)
          .append(identity.modality())
          .append(HASH_STRING_DELIM)
          .append(identity.numDimensions())
          .append(HASH_STRING_DELIM)
          .append(identity.autoEmbedQuantization());
      // For future reference: Include additional field params conditionally for newer hash versions
      // as needed. Note that default values need careful handling to ensure that simply bumping the
      // hash version doesn't change the hash value for all indexes.
    }
    byte[] hashBytes = Hashing.sha256().hashString(sb.toString(), StandardCharsets.UTF_8).asBytes();
    byte[] truncated = Arrays.copyOf(hashBytes, DEFINITION_HASH_BYTES);
    return BaseEncoding.base16().lowerCase().encode(truncated);
  }
}
