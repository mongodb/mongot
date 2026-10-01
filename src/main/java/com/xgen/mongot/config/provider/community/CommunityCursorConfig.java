package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;

/** The {@code cursor} block of the mongot config file. */
public record CommunityCursorConfig(
    Optional<Integer> idleCursorHandlingRateMs, Optional<Integer> cursorIdleTimeMs)
    implements DocumentEncodable {

  static class Fields {
    public static final Field.Optional<Integer> IDLE_CURSOR_HANDLING_RATE_MS =
        Field.builder("idleCursorHandlingRateMs")
            .intField()
            .mustBePositive()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .noDefault();

    public static final Field.Optional<Integer> CURSOR_IDLE_TIME_MS =
        Field.builder("cursorIdleTimeMs")
            .intField()
            .mustBePositive()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .noDefault();
  }

  public static CommunityCursorConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new CommunityCursorConfig(
        parser.getField(Fields.IDLE_CURSOR_HANDLING_RATE_MS).unwrap(),
        parser.getField(Fields.CURSOR_IDLE_TIME_MS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.IDLE_CURSOR_HANDLING_RATE_MS, this.idleCursorHandlingRateMs)
        .field(Fields.CURSOR_IDLE_TIME_MS, this.cursorIdleTimeMs)
        .build();
  }
}
