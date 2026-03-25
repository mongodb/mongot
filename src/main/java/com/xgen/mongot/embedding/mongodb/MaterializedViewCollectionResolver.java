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
import com.mongodb.client.model.Filters;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.mongodb.common.AutoEmbeddingMongoClient;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.mongodb.CheckedMongoException;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.bson.BsonDocument;

/**
 * Implements the logic to determine which materialized view collection to use for a given index
 * definition. Responsible for discovering existing collections and creating new ones as needed.
 */
public class MaterializedViewCollectionResolver {

  private static final String DELIM = "-";
  // Using a character not allowed in field attributes to avoid collisions.
  private static final String HASH_STRING_DELIM = ";";
  private static final int NAMESPACE_EXISTS_ERROR_CODE = 48;
  private static final String MV_COLLECTION_SCHEMA_NAMESPACE = "_autoEmbed";
  private static final String MV_DATABASE_NAME = "__mdb_internal_search";

  // Length of the hash in bytes before hex encoding (16 bytes -> 32 hex chars). We need to
  // truncate the hash to ensure the collection name does not exceed the limits imposed by Atlas
  // (95 bytes).
  private static final int DEFINITION_HASH_BYTES = 16;

  private final MaterializedViewCollectionMetadataCatalog metadataCatalog;

  @SuppressWarnings("UnusedVariable") // will be used later for schema mapping and GC logic
  private final LeaseManager leaseManager;

  @SuppressWarnings("UnusedVariable") // will be used later for schema mapping and GC logic
  private final AutoEmbeddingMaterializedViewConfig materializedViewConfig;

  private final String matViewDatabaseName;

  private final AutoEmbeddingMongoClient autoEmbeddingMongoClient;

  public MaterializedViewCollectionResolver(
      String matViewDatabaseName,
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MaterializedViewCollectionMetadataCatalog metadataCatalog,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig,
      LeaseManager leaseManager) {
    this.autoEmbeddingMongoClient = autoEmbeddingMongoClient;
    this.metadataCatalog = metadataCatalog;
    this.materializedViewConfig = materializedViewConfig;
    this.leaseManager = leaseManager;
    this.matViewDatabaseName = matViewDatabaseName;
  }

  public static MaterializedViewCollectionResolver create(
      AutoEmbeddingMongoClient autoEmbeddingMongoClient,
      MaterializedViewCollectionMetadataCatalog metadataCatalog,
      LeaseManager leaseManager,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig) {
    // TODO(CLOUDP-360542): Support sync source change.
    return new MaterializedViewCollectionResolver(
        MV_DATABASE_NAME,
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
      IndexDefinitionGeneration indexDefinitionGeneration)
      throws MaterializedViewTransientException {
    var mongoClientOpt = this.autoEmbeddingMongoClient.getMaterializedViewResolverMongoClient();
    if (mongoClientOpt.isEmpty()) {
      throw new MaterializedViewTransientException(
          "Missing materialized view collection client",
          MaterializedViewTransientException.Reason.MONGO_CLIENT_NOT_AVAILABLE);
    }
    var mongoClient = mongoClientOpt.get();
    try {
      var collectionName = getOrCreateCollectionName(indexDefinitionGeneration, mongoClient);
      MongoDbCollectionInfo collectionInfo =
          getCollectionInfo(mongoClient, this.matViewDatabaseName, collectionName);

      MaterializedViewCollectionMetadata materializedViewCollectionMetadata =
          this.leaseManager.initializeLease(
              indexDefinitionGeneration,
              createProposedMetadata(
                  collectionName,
                  Check.instanceOf(collectionInfo, MongoDbCollectionInfo.Collection.class)
                      .info()
                      .uuid(),
                  indexDefinitionGeneration
                      .asMaterializedView()
                      .getIndexDefinition()
                      .asVectorDefinition(),
                  this.materializedViewConfig.materializedViewSchemaVersion.orElse(
                      CURRENT_MAT_VIEW_SCHEMA_VERSION)));
      this.metadataCatalog.addMetadata(
          indexDefinitionGeneration.getGenerationId(), materializedViewCollectionMetadata);
      return materializedViewCollectionMetadata;
    } catch (Exception e) {
      throw new MaterializedViewTransientException(e);
    }
  }

  private String getOrCreateCollectionName(
      IndexDefinitionGeneration indexDefinitionGeneration, MongoClient mongoClient)
      throws CheckedMongoException {
    var indexId = indexDefinitionGeneration.getIndexId().toHexString();
    // TODO(CLOUDP-384821): Remove this once we finalize the naming logic.
    // Returns indexId as mat view collection name for community users for now.
    if (this.materializedViewConfig.defaultMaterializedViewNameFormatVersion == 0L) {
      if (getCollectionInfos(mongoClient, this.matViewDatabaseName)
          .getCollectionInfo(this.matViewDatabaseName, indexId)
          .isEmpty()) {
        createCollection(mongoClient, indexId);
      }
      return indexId;
    }
    List<String> collectionNames =
        mongoClient
            .getDatabase(this.matViewDatabaseName)
            .listCollections(BsonDocument.class)
            .filter(Filters.regex("name", "^" + Pattern.quote(indexId)))
            .into(new ArrayList<>())
            .stream()
            .map(doc -> doc.getString("name").getValue())
            .toList();
    String collectionName;
    if (collectionNames.isEmpty()) {
      var hash =
          computeHash(
              indexDefinitionGeneration
                  .asMaterializedView()
                  .getIndexDefinition()
                  .asVectorDefinition());
      collectionName =
          indexId
              + DELIM
              + hash
              + DELIM
              + this.materializedViewConfig.defaultMaterializedViewNameFormatVersion;
      createCollection(mongoClient, collectionName);
    } else {
      // Backwards compatibility check for community. In community, we have a single collection
      // per
      // index. Continue to use that until we start allowing auto-embedding field updates in
      // community.
      if (collectionNames.size() == 1 && collectionNames.get(0).equals(indexId)) {
        collectionName = indexId;
      } else {
        // Check if any of the existing collections can be re-used.
        var hash =
            computeHash(
                indexDefinitionGeneration
                    .asMaterializedView()
                    .getIndexDefinition()
                    .asVectorDefinition());
        var reusableCollection =
            collectionNames.stream()
                .filter(name -> name.startsWith(indexId + DELIM + hash))
                .findFirst();
        if (reusableCollection.isPresent()) {
          collectionName = reusableCollection.get();
        } else {
          // Existing collections cannot be re-used. Create a new one.
          collectionName =
              indexId
                  + DELIM
                  + hash
                  + DELIM
                  + this.materializedViewConfig.defaultMaterializedViewNameFormatVersion;
          createCollection(mongoClient, collectionName);
        }
      }
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
  private void createCollection(MongoClient mongoClient, String collectionName) {
    try {
      mongoClient.getDatabase(this.matViewDatabaseName).createCollection(collectionName);
    } catch (MongoCommandException e) {
      if (e.getErrorCode() != NAMESPACE_EXISTS_ERROR_CODE) {
        throw new MaterializedViewTransientException(e);
      }
      // Ignore if the collection already exists.
    }
  }

  /**
   * Computes a hash of the index definition. The hash is used to determine if the index definition
   * has changed in a way that requires a new materialized view collection.
   */
  public static String computeHash(VectorIndexDefinition indexDefinition) {
    var autoEmbedFields =
        indexDefinition.getFields().stream()
            .filter(field -> field.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED)
            .map(VectorIndexFieldDefinition::asVectorAutoEmbedField)
            .toList();

    // Order-independent: sort so same set of fields always hashes the same
    var sortedFields =
        autoEmbedFields.stream()
            .sorted(
                Comparator.comparing(
                    VectorIndexFieldDefinition::getPath, Comparator.comparing(FieldPath::toString)))
            .toList();

    StringBuilder sb = new StringBuilder();
    for (var field : sortedFields) {
      // Include path, model name, modality and num dimensions in hash as those fields impact the
      // embeddings generated.
      // TODO(CLOUDP-382790): Ensure any other relevant field params are added to the hash
      // TODO(CLOUDP-388224): add quantization to the hash
      // function before launch.
      sb.append(field.getPath().toString())
          .append(HASH_STRING_DELIM)
          .append(field.specification().modelName())
          .append(HASH_STRING_DELIM)
          .append(field.specification().modality())
          .append(HASH_STRING_DELIM)
          .append(field.specification().numDimensions());
      // For future reference: Include additional field params conditionally for newer hash versions
      // as needed. Note that default values need careful handling to ensure that simply bumping the
      // hash version doesn't change the hash value for all indexes.
    }
    byte[] hashBytes = Hashing.sha256().hashString(sb.toString(), StandardCharsets.UTF_8).asBytes();
    byte[] truncated = Arrays.copyOf(hashBytes, DEFINITION_HASH_BYTES);
    return BaseEncoding.base16().lowerCase().encode(truncated);
  }
}
