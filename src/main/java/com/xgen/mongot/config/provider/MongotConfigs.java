package com.xgen.mongot.config.provider;

import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.definition.config.IndexDefinitionConfig;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.index.lucene.directory.EnvironmentVariantPerfConfig;
import com.xgen.mongot.lifecycle.LifecycleConfig;
import com.xgen.mongot.replication.mongodb.DurabilityConfig;
import com.xgen.mongot.replication.mongodb.common.AutoEmbeddingMaterializedViewConfig;
import com.xgen.mongot.replication.mongodb.common.MongoDbReplicationConfig;
import com.xgen.mongot.replication.mongodb.initialsync.config.InitialSyncConfig;
import com.xgen.mongot.server.executors.RegularBlockingRequestSettings;
import com.xgen.mongot.util.bson.parser.SanitizableDocumentEncodable;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonDocument;

public class MongotConfigs implements SanitizableDocumentEncodable {

  public final LuceneConfig luceneConfig;
  public final MongoDbReplicationConfig replicationConfig;
  public final InitialSyncConfig initialSyncConfig;
  public final DurabilityConfig durabilityConfig;
  public final CursorConfig cursorConfig;
  public final IndexDefinitionConfig indexDefinitionConfig;
  public final LifecycleConfig lifecycleConfig;
  public final FeatureFlags featureFlags;
  public final EnvironmentVariantPerfConfig environmentVariantPerfConfig;
  public final RegularBlockingRequestSettings regularBlockingRequestSettings;
  public final AutoEmbeddingMaterializedViewConfig autoEmbeddingMaterializedViewConfig;

  public MongotConfigs(
      LuceneConfig luceneConfig,
      MongoDbReplicationConfig replicationConfig,
      InitialSyncConfig initialSyncConfig,
      DurabilityConfig durabilityConfig,
      CursorConfig cursorConfig,
      IndexDefinitionConfig indexDefinitionConfig,
      LifecycleConfig lifecycleConfig,
      FeatureFlags featureFlags,
      EnvironmentVariantPerfConfig environmentVariantPerfConfig,
      RegularBlockingRequestSettings regularBlockingRequestSettings,
      AutoEmbeddingMaterializedViewConfig autoEmbeddingMaterializedViewConfig
  ) {
    this.luceneConfig = luceneConfig;
    this.replicationConfig = replicationConfig;
    this.initialSyncConfig = initialSyncConfig;
    this.durabilityConfig = durabilityConfig;
    this.cursorConfig = cursorConfig;
    this.indexDefinitionConfig = indexDefinitionConfig;
    this.lifecycleConfig = lifecycleConfig;
    this.featureFlags = featureFlags;
    this.environmentVariantPerfConfig = environmentVariantPerfConfig;
    this.regularBlockingRequestSettings = regularBlockingRequestSettings;
    this.autoEmbeddingMaterializedViewConfig = autoEmbeddingMaterializedViewConfig;
  }

  /** Initializes a MongotConfig with all default values. */
  public static MongotConfigs getDefault(Path dataPath) {
    var luceneConfig =
        LuceneConfig.create(
            dataPath,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    var replicationConfig = MongoDbReplicationConfig.getDefault();

    var initialSyncConfig = new InitialSyncConfig();

    var durabilityConfig = DurabilityConfig.create(Optional.empty(), Optional.empty());

    var cursorConfig = CursorConfig.getDefault();

    var indexDefinitionConfig = IndexDefinitionConfig.getDefault();
    var lifecycleConfig = LifecycleConfig.getDefault();
    var featureFlags = FeatureFlags.withQueryFeaturesEnabled();
    var environmentVariantPerfConfig = EnvironmentVariantPerfConfig.getDefault();
    var regularBlockingRequestSettings = RegularBlockingRequestSettings.defaults();
    var autoEmbeddingMaterializedViewConfig = AutoEmbeddingMaterializedViewConfig.getDefault();
    return new MongotConfigs(
        luceneConfig,
        replicationConfig,
        initialSyncConfig,
        durabilityConfig,
        cursorConfig,
        indexDefinitionConfig,
        lifecycleConfig,
        featureFlags,
        environmentVariantPerfConfig,
        regularBlockingRequestSettings,
        autoEmbeddingMaterializedViewConfig);
  }

  @Override
  public BsonDocument toBson() {
    return new BsonDocument()
        .append("luceneConfig", this.luceneConfig.toBson())
        .append("replicationConfig", this.replicationConfig.toBson())
        .append("initialSyncConfig", this.initialSyncConfig.toBson())
        .append("durabilityConfig", this.durabilityConfig.toBson())
        .append("cursorConfig", this.cursorConfig.toBson())
        .append("indexDefinitionConfig", this.indexDefinitionConfig.toBson())
        .append("lifecycleConfig", this.lifecycleConfig.toBson())
        .append("featureflagConfig", this.featureFlags.toBson())
        .append("environmentVariantPerfConfig", this.environmentVariantPerfConfig.toBson())
        .append(
            "autoEmbeddingMaterializedViewConfig",
            this.autoEmbeddingMaterializedViewConfig.toBson());
  }

  @Override
  public BsonDocument toSanitizedBson() {
    return new BsonDocument()
        .append("luceneConfig", this.luceneConfig.toBson())
        .append("replicationConfig", this.replicationConfig.toBson())
        .append("initialSyncConfig", this.initialSyncConfig.toBson())
        .append("durabilityConfig", this.durabilityConfig.toBson())
        .append("cursorConfig", this.cursorConfig.toBson())
        .append("indexDefinitionConfig", this.indexDefinitionConfig.toBson())
        .append("lifecycleConfig", this.lifecycleConfig.toBson())
        .append("featureflagConfig", this.featureFlags.toBson())
        .append("environmentVariantPerfConfig", this.environmentVariantPerfConfig.toBson())
        .append(
            "autoEmbeddingMaterializedViewConfig",
            this.autoEmbeddingMaterializedViewConfig.toBson());
  }
}
