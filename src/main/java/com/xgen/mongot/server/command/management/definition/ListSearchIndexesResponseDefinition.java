package com.xgen.mongot.server.command.management.definition;

import static com.xgen.mongot.cursor.serialization.MongotCursorResult.EXHAUSTED_CURSOR_ID;

import com.xgen.mongot.server.command.management.definition.common.UserIndexDefinition;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public record ListSearchIndexesResponseDefinition(int ok, Cursor cursor)
    implements DocumentEncodable {

  static class Fields {
    static final Field.Required<Integer> OK =
        Field.builder("ok").intField().mustBeWithinBounds(Range.is(1)).required();
    static final Field.Required<Cursor> CURSOR =
        Field.builder("cursor").classField(Cursor::fromBson).disallowUnknownFields().required();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.OK, this.ok)
        .field(Fields.CURSOR, this.cursor)
        .build();
  }

  public static ListSearchIndexesResponseDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new ListSearchIndexesResponseDefinition(
        parser.getField(Fields.OK).unwrap(), parser.getField(Fields.CURSOR).unwrap());
  }

  public record Cursor(String namespace, List<BsonDocument> firstBatch)
      implements DocumentEncodable {
    // A cursor ID of 0 is hardcoded because this response explicitly does not implement cursors.
    // An IndexInformationTooLarge error will be returned if the response is larger than a single
    // batch.
    private static final int ID = EXHAUSTED_CURSOR_ID;

    public static class Fields {

      public static final Field.Required<Integer> ID =
          Field.builder("id")
              .intField()
              .mustBeWithinBounds(Range.is(EXHAUSTED_CURSOR_ID))
              .required();
      public static final Field.Required<String> NS = Field.builder("ns").stringField().required();

      // TODO(CLOUDP-280897): Use a type for index definitions
      public static final Field.Required<List<BsonDocument>> FIRST_BATCH =
          Field.builder("firstBatch").documentField().asList().required();
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.ID, ID)
          .field(Fields.NS, this.namespace)
          .field(Fields.FIRST_BATCH, this.firstBatch)
          .build();
    }

    public static Cursor fromBson(DocumentParser parser) throws BsonParseException {
      return new Cursor(
          parser.getField(Fields.NS).unwrap(), parser.getField(Fields.FIRST_BATCH).unwrap());
    }
  }

  public record IndexEntry(
      ObjectId indexId,
      String name,
      String status,
      boolean queryable,
      DefinitionVersion latestDefinitionVersion,
      BsonDocument latestDefinition,
      List<HostStatusDetail> hostStatusDetails,
      Optional<String> synonymMappingStatus,
      Optional<List<SynonymMappingStatusDetail>> synonymMappingStatusDetail,
      Optional<Long> numDocs)
      implements DocumentEncodable {

    private static class Fields {

      private static final Field.Required<ObjectId> ID =
          Field.builder("id").objectIdField().encodeAsString().required();

      private static final Field.Required<String> NAME =
          Field.builder("name").stringField().required();

      private static final Field.Required<String> STATUS =
          Field.builder("status").stringField().required();

      private static final Field.Required<Boolean> QUERYABLE =
          Field.builder("queryable").booleanField().required();

      private static final Field.Required<BsonDocument> LATEST_DEFINITION_VERSION =
          Field.builder("latestDefinitionVersion").documentField().required();

      private static final Field.Required<BsonDocument> LATEST_DEFINITION =
          Field.builder("latestDefinition").documentField().required();

      public static final Field.Required<List<BsonDocument>> STATUS_DETAIL =
          Field.builder("statusDetail")
              .listOf(Value.builder().documentValue().required())
              .required();

      private static final Field.Optional<String> SYNONYM_MAPPING_STATUS =
          Field.builder("synonymMappingStatus").stringField().optional().noDefault();

      public static final Field.Optional<List<BsonDocument>> SYNONYM_MAPPING_STATUS_DETAIL =
          Field.builder("synonymMappingStatusDetail")
              .listOf(Value.builder().documentValue().required())
              .optional()
              .noDefault();

      // This is only used by e2e tests and populated when the `internalListAllIndexesForTesting`
      // flag is set.
      private static final Field.Optional<Long> NUM_DOCS =
          Field.builder("numDocs").longField().optional().noDefault();
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.ID, this.indexId)
          .field(Fields.NAME, this.name)
          .field(Fields.STATUS, this.status)
          .field(Fields.QUERYABLE, this.queryable)
          .field(Fields.LATEST_DEFINITION_VERSION, this.latestDefinitionVersion.toBson())
          .field(Fields.LATEST_DEFINITION, this.latestDefinition)
          .field(Fields.STATUS_DETAIL, toDocumentList(this.hostStatusDetails))
          .field(Fields.SYNONYM_MAPPING_STATUS, this.synonymMappingStatus)
          .field(
              Fields.SYNONYM_MAPPING_STATUS_DETAIL,
              this.synonymMappingStatusDetail.map(
                  ListSearchIndexesResponseDefinition::toDocumentList))
          .field(Fields.NUM_DOCS, this.numDocs)
          .build();
    }
  }

  public record DefinitionVersion(int version, Instant createdAt) implements DocumentEncodable {

    private static class Fields {
      private static final Field.Required<Integer> VERSION =
          Field.builder("version").intField().required();

      private static final Field.Required<BsonDateTime> CREATED_AT =
          Field.builder("createdAt").bsonDateTimeField().required();
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.VERSION, this.version)
          .field(Fields.CREATED_AT, new BsonDateTime(this.createdAt.toEpochMilli()))
          .build();
    }
  }

  public record HostStatusDetail(
      String hostName,
      String status,
      boolean queryable,
      Optional<HostIndexStatusDetail> mainIndex,
      Optional<HostIndexStatusDetail> stagedIndex)
      implements DocumentEncodable {

    static class Fields {

      private static final Field.Required<String> HOST_NAME =
          Field.builder("hostname").stringField().required();

      private static final Field.Required<String> STATUS =
          Field.builder("status").stringField().required();

      private static final Field.Required<Boolean> QUERYABLE =
          Field.builder("queryable").booleanField().required();

      private static final Field.Optional<BsonDocument> MAIN_INDEX =
          Field.builder("mainIndex").documentField().optional().noDefault();

      private static final Field.Optional<BsonDocument> STAGED_INDEX =
          Field.builder("stagedIndex").documentField().optional().noDefault();
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.HOST_NAME, this.hostName)
          .field(Fields.STATUS, this.status)
          .field(Fields.QUERYABLE, this.queryable)
          .field(Fields.MAIN_INDEX, this.mainIndex.map(HostIndexStatusDetail::toBson))
          .field(Fields.STAGED_INDEX, this.stagedIndex.map(HostIndexStatusDetail::toBson))
          .build();
    }
  }

  public record HostIndexStatusDetail(
      String status,
      boolean queryable,
      Optional<String> synonymMappingStatus,
      Optional<List<SynonymMappingStatusDetail>> synonymMappingStatusDetail,
      DefinitionVersion definitionVersion,
      UserIndexDefinition definition,
      Optional<String> message)
      implements DocumentEncodable {

    static class Fields {
      private static final Field.Required<String> STATUS =
          Field.builder("status").stringField().required();

      private static final Field.Required<Boolean> QUERYABLE =
          Field.builder("queryable").booleanField().required();

      private static final Field.Optional<String> SYNONYM_MAPPING_STATUS =
          Field.builder("synonymMappingStatus").stringField().optional().noDefault();

      private static final Field.Optional<List<BsonDocument>> SYNONYM_MAPPING_STATUS_DETAILS =
          Field.builder("synonymMappingStatusDetails")
              .listOf(Value.builder().documentValue().required())
              .optional()
              .noDefault();

      private static final Field.Required<BsonDocument> LATEST_DEFINITION_VERSION =
          Field.builder("definitionVersion").documentField().required();

      private static final Field.Required<BsonDocument> DEFINITION =
          Field.builder("definition").documentField().required();

      private static final Field.Optional<String> MESSAGE =
          Field.builder("message").stringField().optional().noDefault();
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.STATUS, this.status)
          .field(Fields.QUERYABLE, this.queryable)
          .field(Fields.SYNONYM_MAPPING_STATUS, this.synonymMappingStatus)
          .field(
              Fields.SYNONYM_MAPPING_STATUS_DETAILS,
              this.synonymMappingStatusDetail.map(
                  ListSearchIndexesResponseDefinition::toDocumentList))
          .field(Fields.LATEST_DEFINITION_VERSION, this.definitionVersion.toBson())
          .field(Fields.DEFINITION, this.definition.toBson())
          .field(Fields.MESSAGE, this.message)
          .build();
    }
  }

  public record SynonymMappingStatusDetail(
      String status, boolean queryable, Optional<String> message) implements DocumentEncodable {

    static class Fields {
      private static final Field.Required<String> STATUS =
          Field.builder("status").stringField().required();

      private static final Field.Required<Boolean> QUERYABLE =
          Field.builder("queryable").booleanField().required();

      private static final Field.Optional<String> MESSAGE =
          Field.builder("message").stringField().optional().noDefault();
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.STATUS, this.status)
          .field(Fields.QUERYABLE, this.queryable)
          .field(Fields.MESSAGE, this.message)
          .build();
    }
  }

  private static List<BsonDocument> toDocumentList(List<? extends DocumentEncodable> docs) {
    return docs.stream().map(DocumentEncodable::toBson).toList();
  }
}
