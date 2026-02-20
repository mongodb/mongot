package com.xgen.mongot.catalogservice;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.synonym.SynonymDetailedStatus;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.types.ObjectId;

public record IndexStatsEntry(
    IndexStatsKey key,
    IndexDefinition.Type type,
    Optional<DetailedIndexStats> mainIndex,
    Optional<DetailedIndexStats> stagedIndex)
    implements DocumentEncodable {

  public IndexStatsEntry {
    Objects.requireNonNull(key);
    Objects.requireNonNull(type);
    Objects.requireNonNull(mainIndex);
    Objects.requireNonNull(stagedIndex);
  }

  public static class Fields {
    static final Field.Required<IndexStatsKey> INDEX_STATS_KEY =
        Field.builder("_id").classField(IndexStatsKey::fromBson).allowUnknownFields().required();

    static final Field.Required<IndexDefinition.Type> TYPE =
        Field.builder("type").enumField(IndexDefinition.Type.class).asUpperUnderscore().required();

    static final Field.Optional<DetailedIndexStats> MAIN_INDEX =
        Field.builder("mainIndex")
            .classField(DetailedIndexStats::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<DetailedIndexStats> STAGED_INDEX =
        Field.builder("stagedIndex")
            .classField(DetailedIndexStats::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.INDEX_STATS_KEY, this.key)
        .field(Fields.TYPE, this.type)
        .field(Fields.MAIN_INDEX, this.mainIndex)
        .field(Fields.STAGED_INDEX, this.stagedIndex)
        .build();
  }

  public static IndexStatsEntry fromBson(DocumentParser parser) throws BsonParseException {
    return new IndexStatsEntry(
        parser.getField(Fields.INDEX_STATS_KEY).unwrap(),
        parser.getField(Fields.TYPE).unwrap(),
        parser.getField(Fields.MAIN_INDEX).unwrap(),
        parser.getField(Fields.STAGED_INDEX).unwrap());
  }

  public static BsonDocument keyAsBson(IndexStatsKey indexKey) {
    return BsonDocumentBuilder.builder().field(Fields.INDEX_STATS_KEY, indexKey).build();
  }

  public static BsonDocument serverIdFilter(ObjectId serverId) {
    return new BsonDocument()
        .append(
            Fields.INDEX_STATS_KEY.getName() + "." + IndexStatsKey.Fields.SERVER_ID.getName(),
            new BsonObjectId(serverId));
  }

  public static BsonDocument indexIdFilter(ObjectId indexId) {
    return new BsonDocument()
        .append(
            Fields.INDEX_STATS_KEY.getName() + "." + IndexStatsKey.Fields.INDEX_ID.getName(),
            new BsonObjectId(indexId));
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    IndexStatsEntry entry = (IndexStatsEntry) o;
    return Objects.equals(this.key, entry.key)
        && this.type == entry.type
        && Objects.equals(this.mainIndex, entry.mainIndex)
        && Objects.equals(this.stagedIndex, entry.stagedIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.key, this.type, this.mainIndex, this.stagedIndex);
  }

  public record IndexStatsKey(ObjectId serverId, ObjectId indexId) implements DocumentEncodable {

    static class Fields {
      static final Field.Required<ObjectId> SERVER_ID =
          Field.builder("serverId").objectIdField().required();
      static final Field.Required<ObjectId> INDEX_ID =
          Field.builder("indexId").objectIdField().required();
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.SERVER_ID, this.serverId)
          .field(Fields.INDEX_ID, this.indexId)
          .build();
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || this.getClass() != o.getClass()) {
        return false;
      }
      IndexStatsKey entry = (IndexStatsKey) o;
      return Objects.equals(this.serverId, entry.serverId)
          && Objects.equals(this.indexId, entry.indexId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.serverId, this.indexId);
    }

    public static IndexStatsKey fromBson(DocumentParser parser) throws BsonParseException {
      return new IndexStatsKey(
          parser.getField(Fields.SERVER_ID).unwrap(), parser.getField(Fields.INDEX_ID).unwrap());
    }
  }

  public record DetailedIndexStats(
      IndexStatus status,
      IndexDefinition definition,
      Optional<Map<String, SynonymDetailedStatus>> synonymDetailedStatusMap)
      implements DocumentEncodable {

    static class Fields {
      static final Field.Required<IndexStatus> INDEX_STATUS =
          Field.builder("status").classField(IndexStatus::fromBson).allowUnknownFields().required();

      static final Field.Required<IndexDefinition> INDEX_DEFINITION =
          Field.builder("definition")
              .classField(IndexDefinition::fromBson)
              .allowUnknownFields()
              .required();

      static final Field.Optional<Map<String, SynonymDetailedStatus>> SYNONYM_DETAILED_STATUS_MAP =
          Field.builder("synonymDetailedStatusMap")
              .mapOf(
                  Value.builder()
                      .classValue(SynonymDetailedStatus::fromBson)
                      .allowUnknownFields()
                      .required())
              .optional()
              .noDefault();
    }

    public static DetailedIndexStats fromBson(DocumentParser parser) throws BsonParseException {
      return new DetailedIndexStats(
          parser.getField(Fields.INDEX_STATUS).unwrap(),
          parser.getField(Fields.INDEX_DEFINITION).unwrap(),
          parser.getField(Fields.SYNONYM_DETAILED_STATUS_MAP).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.INDEX_STATUS, this.status)
          .field(Fields.INDEX_DEFINITION, this.definition)
          .field(Fields.SYNONYM_DETAILED_STATUS_MAP, this.synonymDetailedStatusMap)
          .build();
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || this.getClass() != o.getClass()) {
        return false;
      }
      DetailedIndexStats entry = (DetailedIndexStats) o;
      return Objects.equals(this.status, entry.status)
          && Objects.equals(this.definition, entry.definition)
          && Objects.equals(this.synonymDetailedStatusMap, entry.synonymDetailedStatusMap);
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.status, this.definition, this.synonymDetailedStatusMap);
    }
  }
}
