package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;
import picocli.CommandLine;

/** The {@code querying} block of the Community mongot config file. */
public record CommunityQueryingConfig(Optional<LuceneConfig> luceneConfig)
    implements DocumentEncodable {

  static class Fields {
    public static final Field.Optional<LuceneConfig> LUCENE_CONFIG =
        Field.builder("lucene")
            .classField(LuceneConfig::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  public static final class CommunityQueryingConfigCliOptionRegistry extends CliOptionRegistry {
    @CommandLine.Mixin
    public LuceneConfig.LuceneConfigCliOptionRegistry luceneConfigCliOptionRegistry =
        new LuceneConfig.LuceneConfigCliOptionRegistry();

    public CommunityQueryingConfigCliOptionRegistry() {
      addChildRegistry(Fields.LUCENE_CONFIG, this.luceneConfigCliOptionRegistry);
    }
  }

  public static CommunityQueryingConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new CommunityQueryingConfig(parser.getField(Fields.LUCENE_CONFIG).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.LUCENE_CONFIG, this.luceneConfig).build();
  }

  public record LuceneConfig(Optional<Integer> maxClauseLimit, Optional<Double> floorSegmentMB)
      implements DocumentEncodable {

    static class Fields {
      public static final Field.Optional<Integer> MAX_CLAUSE_LIMIT =
          Field.builder("maxClauseLimit").intField().mustBePositive().optional().noDefault();

      /**
       * Sets vector FloorSegmentMB in Lucene's {@code TieredMergePolicy}. Larger values improve
       * query performance at the cost of merging smaller segments more aggressively. Must be
       * strictly positive. Only consumed by the vector merge policy; regular search indexes use
       * Lucene's default floor segment size. Defaults to 64.0 when unset.
       */
      public static final Field.Optional<Double> FLOOR_SEGMENT_MB =
          Field.builder("vectorFloorSegmentMB")
              .doubleField()
              .mustBePositive()
              .optional()
              .noDefault();
    }

    public static final class LuceneConfigCliOptionRegistry extends CliOptionRegistry {
      @CommandLine.Option(
          names = "--queryingLuceneMaxClauseLimit",
          description = "Maximum number of clauses a single query can expand to")
      public Optional<Integer> maxClauseLimit = Optional.empty();

      @CommandLine.Option(
          names = "--queryingLuceneVectorFloorSegmentMB",
          description =
              "Floor segment size, used by the merge policy for MongoDB Vector Search indexes")
      public Optional<Double> vectorFloorSegmentMB = Optional.empty();

      public LuceneConfigCliOptionRegistry() {
        addIntegerLeafValue(LuceneConfig.Fields.MAX_CLAUSE_LIMIT, () -> this.maxClauseLimit);
        addDoubleLeafValue(LuceneConfig.Fields.FLOOR_SEGMENT_MB, () -> this.vectorFloorSegmentMB);
      }
    }

    public static LuceneConfig fromBson(DocumentParser parser) throws BsonParseException {
      return new LuceneConfig(
          parser.getField(Fields.MAX_CLAUSE_LIMIT).unwrap(),
          parser.getField(Fields.FLOOR_SEGMENT_MB).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.MAX_CLAUSE_LIMIT, this.maxClauseLimit)
          .field(Fields.FLOOR_SEGMENT_MB, this.floorSegmentMB)
          .build();
    }
  }
}
