package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * The {@code durability} block of the community mongot config file. Tunes how often indexes are
 * committed to make updates durable.
 */
public record CommunityDurabilityConfig(Optional<Integer> commitIntervalMs)
    implements DocumentEncodable {

  static class Fields {
    public static final Field.Optional<Integer> COMMIT_INTERVAL_MS =
        Field.builder("commitIntervalMs")
            .intField()
            .mustBePositive()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .noDefault();
  }

  public static CommunityDurabilityConfig fromBson(DocumentParser parser)
      throws BsonParseException {
    return new CommunityDurabilityConfig(parser.getField(Fields.COMMIT_INTERVAL_MS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.COMMIT_INTERVAL_MS, this.commitIntervalMs)
        .build();
  }
}
