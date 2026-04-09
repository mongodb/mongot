package com.xgen.mongot.config.provider.community;

import com.mongodb.ReadPreference;
import com.xgen.mongot.config.provider.community.parser.PathField;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.nio.file.Path;
import java.util.Optional;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record SyncSourceConfig(
    ReplicaSetConfig replicaSet,
    Optional<RouterConfig> router,
    Optional<Path> caFile,
    Optional<ReadPreferenceConfig> replicationReader)
    implements DocumentEncodable {

  private static final Logger LOG = LoggerFactory.getLogger(SyncSourceConfig.class);

  /**
   * This class returns the read preference the customer configured for replication.
   *
   * <p>We have added a new `replicationReader` config block that is the default when set. While we
   * deprecate the read preference arguments in the replicaSet and router config blocks, the
   * replicaSet.readPreference will always be used as the fallback.
   *
   * <p>When we go GA we will remove the deprecated replicaSet/router readPreference arguments and
   * just support the `replicationReader` defaulting to secondaryPreferred when not set.
   *
   * <p>TODO(CLOUDP-395903) - remove deprecated readPreference configuration block.
   */
  public ReadPreference getReplicationReaderReadPreference() {
    return this.replicationReader
        .map(rp -> rp.readPreference().asReadPreference(rp.tagSets()))
        .or(() -> this.replicaSet.readPreference().map(MongoReadPreferenceName::asReadPreference))
        .orElse(ReadPreference.secondaryPreferred());
  }

  private static class Fields {
    public static final Field.Required<ReplicaSetConfig> REPLICA_SET =
        Field.builder("replicaSet")
            .classField(ReplicaSetConfig::fromBson, ReplicaSetConfig::toBson)
            .disallowUnknownFields()
            .required();

    public static final Field.Optional<RouterConfig> ROUTER =
        Field.builder("router")
            .classField(RouterConfig::fromBson, RouterConfig::toBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.Optional<Path> CA_FILE =
        Field.builder("caFile")
            .classField(PathField.PARSER, PathField.ENCODER)
            .optional()
            .noDefault();

    public static final Field.Optional<ReadPreferenceConfig> REPLICATION_READER =
        Field.builder("replicationReader")
            .classField(ReadPreferenceConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  public static SyncSourceConfig fromBson(DocumentParser parser) throws BsonParseException {
    ReplicaSetConfig replicaSet = parser.getField(Fields.REPLICA_SET).unwrap();
    Optional<RouterConfig> router = parser.getField(Fields.ROUTER).unwrap();
    Optional<Path> caFile = parser.getField(Fields.CA_FILE).unwrap();
    Optional<ReadPreferenceConfig> replicationReader =
        parser.getField(Fields.REPLICATION_READER).unwrap();

    if (replicaSet.readPreference().isPresent()) {
      LOG.atWarn()
          .log(
              "syncSource.replicaSet.readPreference is deprecated and will be removed in the next"
                  + " release. Use syncSource.replicationReader instead.");
    }
    if (router.isPresent() && router.get().readPreference().isPresent()) {
      LOG.atWarn()
          .log(
              "syncSource.router.readPreference is deprecated and will be removed in the next"
                  + " release. Use syncSource.replicationReader instead.");
    }

    SyncSourceConfig syncSourceConfig =
        new SyncSourceConfig(replicaSet, router, caFile, replicationReader);

    syncSourceConfig.replicaSet.validate(parser, syncSourceConfig.caFile);
    if (syncSourceConfig.router.isPresent()) {
      syncSourceConfig.router.get().validate(parser, syncSourceConfig.caFile);
    }
    return syncSourceConfig;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.REPLICA_SET, this.replicaSet)
        .field(Fields.ROUTER, this.router)
        .field(Fields.CA_FILE, this.caFile)
        .field(Fields.REPLICATION_READER, this.replicationReader)
        .build();
  }
}
