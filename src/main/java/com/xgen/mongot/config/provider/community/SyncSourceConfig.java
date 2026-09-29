package com.xgen.mongot.config.provider.community;

import com.mongodb.ReadPreference;
import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;
import picocli.CommandLine;

public record SyncSourceConfig(
    ReplicaSetConfig replicaSet,
    Optional<RouterConfig> router,
    Optional<ReadPreferenceConfig> replicationReader)
    implements DocumentEncodable {

  public ReadPreference getReplicationReaderReadPreference() {
    return this.replicationReader
        .map(rp -> rp.readPreference().asReadPreference(rp.tagSets()))
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

    public static final Field.Optional<ReadPreferenceConfig> REPLICATION_READER =
        Field.builder("replicationReader")
            .classField(ReadPreferenceConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  public static final class SyncSourceConfigCliOptionRegistry extends CliOptionRegistry {
    @CommandLine.Mixin
    public ReplicaSetConfig.ReplicaSetConfigCliOptionRegistry replicaSetConfigCliOptionRegistry =
        new ReplicaSetConfig.ReplicaSetConfigCliOptionRegistry();

    @CommandLine.Mixin
    public RouterConfig.RouterConfigCliOptionRegistry routerConfigCliOptionRegistry =
        new RouterConfig.RouterConfigCliOptionRegistry();

    @CommandLine.Mixin
    public ReadPreferenceConfig.ReadPreferenceConfigCliOptionRegistry
        replicationReaderCliOptionRegistry =
            new ReadPreferenceConfig.ReadPreferenceConfigCliOptionRegistry();

    public SyncSourceConfigCliOptionRegistry() {
      addChildRegistry(Fields.REPLICA_SET, this.replicaSetConfigCliOptionRegistry);
      addChildRegistry(Fields.ROUTER, this.routerConfigCliOptionRegistry);
      addChildRegistry(Fields.REPLICATION_READER, this.replicationReaderCliOptionRegistry);
    }
  }

  public static SyncSourceConfig fromBson(DocumentParser parser) throws BsonParseException {
    ReplicaSetConfig replicaSet = parser.getField(Fields.REPLICA_SET).unwrap();
    Optional<RouterConfig> router = parser.getField(Fields.ROUTER).unwrap();
    Optional<ReadPreferenceConfig> replicationReader =
        parser.getField(Fields.REPLICATION_READER).unwrap();

    SyncSourceConfig syncSourceConfig = new SyncSourceConfig(replicaSet, router, replicationReader);

    syncSourceConfig.replicaSet.validate(parser);
    if (syncSourceConfig.router.isPresent()) {
      syncSourceConfig.router.get().validate(parser);
    }
    return syncSourceConfig;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.REPLICA_SET, this.replicaSet)
        .field(Fields.ROUTER, this.router)
        .field(Fields.REPLICATION_READER, this.replicationReader)
        .build();
  }
}
