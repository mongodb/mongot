package com.xgen.mongot.embedding.mongodb;

import static com.xgen.mongot.util.mongodb.MongoDbDatabase.getCollectionInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadataCatalog;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.mongodb.MongoClientBuilder;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.bson.BsonDocument;

/**
 * Implements the logic to determine which materialized view collection to use for a given index
 * definition. Responsible for discovering existing collections and creating new ones as needed.
 */
public class MaterializedViewCollectionResolver {

  private static final long CURRENT_HASH_VERSION = 1L;
  private static final String DELIM = "-";
  // Using a character not allowed in field attributes to avoid collisions.
  private static final String HASH_STRING_DELIM = ";";
  private static final int DEFAULT_MAX_CONNECTIONS = 2;
  private static final int NAMESPACE_EXISTS_ERROR_CODE = 48;

  @VisibleForTesting static final String MV_DATABASE_NAME = "__mdb_internal_search";

  // Length of the hash in bytes before hex encoding (16 bytes -> 32 hex chars). We need to
  // truncate the hash to ensure the collection name does not exceed the limits imposed by Atlas
  // (95 bytes).
  private static final int DEFINITION_HASH_BYTES = 16;

  private final MongoClient mongoClient;
  private final MaterializedViewCollectionMetadataCatalog metadataCatalog;

  @SuppressWarnings("UnusedVariable") // will be used later for schema mapping and GC logic
  private final LeaseManager leaseManager;

  @SuppressWarnings("UnusedVariable") // will be used later for schema mapping and GC logic
  private final AutoEmbeddingMaterializedViewConfig materializedViewConfig;

  public MaterializedViewCollectionResolver(
      MongoClient mongoClient,
      MaterializedViewCollectionMetadataCatalog metadataCatalog,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig,
      LeaseManager leaseManager) {
    this.mongoClient = mongoClient;
    this.metadataCatalog = metadataCatalog;
    this.materializedViewConfig = materializedViewConfig;
    this.leaseManager = leaseManager;
  }

  public static MaterializedViewCollectionResolver create(
      SyncSourceConfig syncSourceConfig,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      MaterializedViewCollectionMetadataCatalog metadataCatalog,
      LeaseManager leaseManager,
      AutoEmbeddingMaterializedViewConfig materializedViewConfig) {
    // TODO(CLOUDP-360542): Support sync source change.
    return new MaterializedViewCollectionResolver(
        createMongoClient(syncSourceConfig, meterAndFtdcRegistry.meterRegistry()),
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
   */
  public MaterializedViewCollectionMetadata getOrCreateMaterializedViewForIndex(
      IndexDefinitionGeneration indexDefinitionGeneration) {
    var indexId = indexDefinitionGeneration.getIndexId().toHexString();

    List<String> collectionNames =
        this.mongoClient
            .getDatabase(MV_DATABASE_NAME)
            .listCollections(BsonDocument.class)
            .filter(Filters.regex("name", "^" + Pattern.quote(indexId)))
            .into(new ArrayList<>())
            .stream()
            .map(doc -> doc.getString("name").getValue())
            .toList();

    String collectionName;
    if (collectionNames.isEmpty()) {
      var hash = computeHash(indexDefinitionGeneration.asMaterializedView().getIndexDefinition());
      collectionName = indexId + DELIM + hash + DELIM + CURRENT_HASH_VERSION;
      createCollection(collectionName);
    } else {
      // Backwards compatibility check for community. In community, we have a single collection per
      // index. Continue to use that until we start allowing auto-embedding field updates in
      // community.
      if (collectionNames.size() == 1 && collectionNames.get(0).equals(indexId)) {
        collectionName = indexId;
      } else {
        // Check if any of the existing collections can be re-used.
        var hash = computeHash(indexDefinitionGeneration.asMaterializedView().getIndexDefinition());
        var reusableCollection =
            collectionNames.stream()
                .filter(name -> name.startsWith(indexId + DELIM + hash))
                .findFirst();
        if (reusableCollection.isPresent()) {
          collectionName = reusableCollection.get();
        } else {
          // Existing collections cannot be re-used. Create a new one.
          collectionName = indexId + DELIM + hash + DELIM + CURRENT_HASH_VERSION;
          createCollection(collectionName);
        }
      }
    }

    try {
      MongoDbCollectionInfo collectionInfo =
          getCollectionInfo(this.mongoClient, MV_DATABASE_NAME, collectionName);
      // TODO(CLOUDP-363914): Insert or reuse MaterializedViewCollectionMetadata in Lease table for
      //  mongot synchronization
      var retValue =
          new MaterializedViewCollectionMetadata(
              new MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata(0, Map.of()),
              Check.instanceOf(collectionInfo, MongoDbCollectionInfo.Collection.class)
                  .info()
                  .uuid(),
              collectionName);
      this.metadataCatalog.addMetadata(indexDefinitionGeneration.getGenerationId(), retValue);
      return retValue;
    } catch (Exception e) {
      throw new MaterializedViewNonTransientException(e);
    }
  }

  /**
   * Creates a new materialized view collection with the given name while gracefully handling the
   * case where the collection already exists.
   */
  private void createCollection(String collectionName) {
    try {
      this.mongoClient.getDatabase(MV_DATABASE_NAME).createCollection(collectionName);
    } catch (MongoCommandException e) {
      if (e.getErrorCode() != NAMESPACE_EXISTS_ERROR_CODE) {
        throw e;
      }
    }
  }

  /**
   * Computes a hash of the index definition. The hash is used to determine if the index definition
   * has changed in a way that requires a new materialized view collection.
   */
  private String computeHash(VectorIndexDefinition indexDefinition) {
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

  private static MongoClient createMongoClient(
      SyncSourceConfig syncSourceConfig, MeterRegistry meterRegistry) {
    // Use mongosUri if available, otherwise fall back to mongodUri. This allows the MongoDB
    // driver
    // to automatically discover replica set topology and route writes to the primary, avoiding
    // NotWritablePrimary errors after failovers.
    var connectionString = syncSourceConfig.mongosUri.orElse(syncSourceConfig.mongodUri);
    return MongoClientBuilder.buildNonReplicationWithDefaults(
        connectionString,
        "AutoEmbedding Materialized View Collection Resolver",
        DEFAULT_MAX_CONNECTIONS,
        syncSourceConfig.sslContext,
        meterRegistry);
  }
}
