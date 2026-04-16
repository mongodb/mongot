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
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorTextFieldSpecification;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.mongodb.CheckedMongoException;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import java.nio.charset.StandardCharsets;
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

  private final MaterializedViewCollectionMetadataCatalog metadataCatalog;

  @SuppressWarnings("UnusedVariable") // will be used later for schema mapping and GC logic
  private final LeaseManager leaseManager;

  @SuppressWarnings("UnusedVariable") // will be used later for schema mapping and GC logic
  private final AutoEmbeddingMaterializedViewConfig materializedViewConfig;

  private final InternalDatabaseResolver dbResolver;

  private final AutoEmbeddingMongoClient autoEmbeddingMongoClient;

  public MaterializedViewCollectionResolver(
      InternalDatabaseResolver dbResolver,
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MaterializedViewCollectionMetadataCatalog metadataCatalog,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig,
      LeaseManager leaseManager) {
    this.autoEmbeddingMongoClient = autoEmbeddingMongoClient;
    this.metadataCatalog = metadataCatalog;
    this.materializedViewConfig = materializedViewConfig;
    this.leaseManager = leaseManager;
    this.dbResolver = dbResolver;
  }

  public static MaterializedViewCollectionResolver create(
      InternalDatabaseResolver dbResolver,
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MaterializedViewCollectionMetadataCatalog metadataCatalog,
      LeaseManager leaseManager,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig) {
    // TODO(CLOUDP-360542): Support sync source change.
    return new MaterializedViewCollectionResolver(
        dbResolver,
        autoEmbeddingMongoClient,
        metadataCatalog,
        materializedViewConfig,
        leaseManager);
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
    var mongoClientOpt = this.autoEmbeddingMongoClient.getMaterializedViewResolverMongoClient();
    if (mongoClientOpt.isEmpty()) {
      throw new MaterializedViewTransientException(
          "Missing materialized view collection client",
          MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE);
    }
    var mongoClient = mongoClientOpt.get();
    String matViewDb =
        this.dbResolver.resolve(indexDefinitionGeneration.getIndexDefinition().getDatabase());
    try {
      var collectionName =
          getOrCreateCollectionName(indexDefinitionGeneration, mongoClient, matViewDb);
      MongoDbCollectionInfo collectionInfo =
          getCollectionInfo(mongoClient, matViewDb, collectionName);

      MaterializedViewCollectionMetadata materializedViewCollectionMetadata =
          this.leaseManager.initializeLease(
              indexDefinitionGeneration,
              createProposedMetadata(
                  collectionName,
                  Check.instanceOf(collectionInfo, MongoDbCollectionInfo.Collection.class)
                      .info()
                      .uuid(),
                  indexDefinitionGeneration.getIndexDefinition().asVectorDefinition(),
                  this.materializedViewConfig.materializedViewSchemaVersion.orElse(
                      CURRENT_MAT_VIEW_SCHEMA_VERSION)));
      this.metadataCatalog.addMetadata(
          indexDefinitionGeneration.getGenerationId(), materializedViewCollectionMetadata);
      this.metadataCatalog.addDatabaseName(indexDefinitionGeneration.getGenerationId(), matViewDb);
      LOG.atInfo()
          .addKeyValue("generationId", indexDefinitionGeneration.getGenerationId())
          .addKeyValue("collectionName", collectionName)
          .addKeyValue("database", matViewDb)
          .log("Registered materialized view metadata and database name");
      return materializedViewCollectionMetadata;
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

  private String getOrCreateCollectionName(
      IndexDefinitionGeneration indexDefinitionGeneration,
      MongoClient mongoClient,
      String matViewDb)
      throws CheckedMongoException {
    // TODO(CLOUDP-384821): Update index catalog service for community to set
    // MaterializedViewNameFormatVersion for new indexes.
    // MMS should set fallback defaultMaterializedViewNameFormatVersion >= 1, community currectly
    // sets it to 0 for backward compatibility, new indexes should use per-index name format
    // version.
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
              indexDefinitionGeneration
                  .asMaterializedView()
                  .getIndexDefinition()
                  .asVectorDefinition());
      collectionName =
          uniqueIndexId
              + DELIM
              + hash
              + DELIM
              + matViewNameFormatVersion
              + DELIM
              + autoEmbeddingDefinitionVersion;
    }
    if (getCollectionInfos(mongoClient, matViewDb)
        .getCollectionInfo(matViewDb, collectionName)
        .isEmpty()) {
      // Create new mat view collection when it's not found.
      createCollection(mongoClient, matViewDb, collectionName);
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
      VectorIndexDefinition indexDefinition,
      int mvSchemaVersion) {
    if (mvSchemaVersion == 0) {
      return new MaterializedViewCollectionMetadata(VERSION_ZERO, uuid, collectionName);
    } else if (mvSchemaVersion == 1) {
      ImmutableMap<FieldPath, FieldPath> schemaFieldsMapping =
          indexDefinition.getMappings().fieldMap().entrySet().stream()
              .filter(
                  entry -> entry.getValue().getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED)
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
  private void createCollection(MongoClient mongoClient, String matViewDb, String collectionName) {
    try {
      mongoClient.getDatabase(matViewDb).createCollection(collectionName);
    } catch (MongoCommandException e) {
      if (e.getErrorCode() != NAMESPACE_EXISTS_ERROR_CODE) {
        throw new MaterializedViewTransientException(
            String.valueOf(e.getMessage()),
            e,
            MaterializedViewTransientException.Reason.COLLECTION_RESOLUTION_FAILED);
      }
      // Collection already exists — another instance created it concurrently.
      LOG.atInfo()
          .addKeyValue("collectionName", collectionName)
          .addKeyValue("database", matViewDb)
          .log("MatView collection already exists, skipping creation");
    }
  }

  /**
   * Computes a hash of the auto-embed fields in a vector index definition. Convenience overload
   * that extracts auto-embed fields before delegating to {@link
   * #computeHash(AutoEmbedFieldMapping)}.
   */
  public static String computeHash(VectorIndexDefinition indexDefinition) {
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
