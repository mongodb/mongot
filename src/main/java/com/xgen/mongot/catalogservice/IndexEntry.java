package com.xgen.mongot.catalogservice;

import com.mongodb.client.model.Updates;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public record IndexEntry(
    AuthoritativeIndexKey indexKey,
    ObjectId indexId,
    int schemaVersion,
    IndexDefinition definition,
    Optional<BsonDocument> customerDefinition)
    implements DocumentEncodable {

  public IndexEntry {
    Objects.requireNonNull(indexKey);
    Objects.requireNonNull(indexId);
    Objects.requireNonNull(definition);
    Objects.requireNonNull(customerDefinition);
  }

  abstract static class Fields {
    static final Field.Required<AuthoritativeIndexKey> INDEX_KEY =
        Field.builder("_id")
            .classField(AuthoritativeIndexKey::fromBson)
            .disallowUnknownFields()
            .required();

    static final Field.Required<ObjectId> ID = Field.builder("indexId").objectIdField().required();
    static final Field.Required<Integer> VERSION =
        Field.builder("schemaVersion").intField().required();

    // The serialized internal representation of the index definition
    static final Field.Required<IndexDefinition> DEFINITION =
        Field.builder("definition")
            .classField(IndexDefinition::fromBson)
            .disallowUnknownFields()
            .required();

    // The exact index definition document provided by the customer when creating/updating the
    // index. The internal definition is built using this document but we explicitly store the
    // customer provided definition so we can return it to the customer in the ListSearchIndexes
    // response.
    static final Field.Optional<BsonDocument> CUSTOMER_DEFINITION =
        Field.builder("customerDefinition").documentField().optional().noDefault();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.INDEX_KEY, this.indexKey())
        .field(Fields.ID, this.indexId())
        .field(Fields.VERSION, this.schemaVersion())
        .field(Fields.DEFINITION, this.definition())
        .field(Fields.CUSTOMER_DEFINITION, this.customerDefinition)
        .build();
  }

  public BsonDocument keyBson() {
    return keyAsBson(this.indexKey);
  }

  public static BsonDocument keyAsBson(AuthoritativeIndexKey indexKey) {
    return BsonDocumentBuilder.builder().field(Fields.INDEX_KEY, indexKey).build();
  }

  public static BsonDocument updateDefinition(IndexDefinition definition) {
    return Updates.set(Fields.DEFINITION.getName(), definition.toBson()).toBsonDocument();
  }

  public static IndexEntry fromBson(DocumentParser parser) throws BsonParseException {
    return new IndexEntry(
        parser.getField(Fields.INDEX_KEY).unwrap(),
        parser.getField(Fields.ID).unwrap(),
        parser.getField(Fields.VERSION).unwrap(),
        parser.getField(Fields.DEFINITION).unwrap(),
        parser.getField(Fields.CUSTOMER_DEFINITION).unwrap());
  }
}
