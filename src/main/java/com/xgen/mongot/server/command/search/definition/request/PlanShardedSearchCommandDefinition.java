package com.xgen.mongot.server.command.search.definition.request;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;

public record PlanShardedSearchCommandDefinition(
    String db,
    String collectionName,
    Optional<UUID> collectionUuid,
    Optional<String> viewName,
    BsonDocument queryDocument,
    SearchFeatures searchFeatures) {

  public static class Fields {
    static final Field.Required<String> PLAN_SHARDED_SEARCH =
        Field.builder("planShardedSearch").stringField().required();

    static final Field.Required<String> DB = Field.builder("$db").stringField().required();

    static final Field.Optional<UUID> COLLECTION_UUID =
        Field.builder("collectionUUID").uuidField().optional().noDefault();

    static final Field.Optional<String> VIEW_NAME =
        Field.builder("viewName").stringField().optional().noDefault();

    static final Field.Required<BsonDocument> QUERY =
        Field.builder("query").documentField().required();

    public static final Field.WithDefault<SearchFeatures> SEARCH_FEATURES =
        Field.builder("searchFeatures")
            .classField(SearchFeatures::fromBson)
            .allowUnknownFields()
            .optional()
            .withDefault(new SearchFeatures(SearchFeatures.Fields.SHARDED_SORT.getDefaultValue()));
  }

  public static final String NAME = "planShardedSearch";

  public static PlanShardedSearchCommandDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new PlanShardedSearchCommandDefinition(
        parser.getField(Fields.DB).unwrap(),
        parser.getField(Fields.PLAN_SHARDED_SEARCH).unwrap(),
        parser.getField(Fields.COLLECTION_UUID).unwrap(),
        parser.getField(Fields.VIEW_NAME).unwrap(),
        parser.getField(Fields.QUERY).unwrap(),
        parser.getField(Fields.SEARCH_FEATURES).unwrap());
  }

  public record SearchFeatures(int shardedSort) implements DocumentEncodable {
    private static class Fields {
      static final Field.WithDefault<Integer> SHARDED_SORT =
          Field.builder("shardedSort")
              .intField()
              .validate(
                  shardedSort ->
                      shardedSort == 0 || shardedSort == 1
                          ? Optional.empty()
                          : Optional.of("shardedSort must be set to 0 or 1"))
              .optional()
              .withDefault(0);
    }

    public static SearchFeatures fromBson(DocumentParser parser) throws BsonParseException {
      return new SearchFeatures(parser.getField(Fields.SHARDED_SORT).unwrap());
    }

    public boolean supportsShardedSort() {
      return this.shardedSort == 1;
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder().field(Fields.SHARDED_SORT, this.shardedSort).build();
    }
  }
}
