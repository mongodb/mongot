package com.xgen.mongot.catalogservice.schema;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Field.Required;
import java.util.UUID;
import org.bson.BsonDocument;

public record AuthoritativeIndexKey(UUID collectionUuid, String indexName)
    implements DocumentEncodable {

  public abstract static class Fields {
    static final Required<UUID> COLLECTION_UUID =
        Field.builder("collectionUuid").uuidField().required();
    static final Required<String> INDEX_NAME = Field.builder("indexName").stringField().required();

    private Fields() {}
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.COLLECTION_UUID, this.collectionUuid())
        .field(Fields.INDEX_NAME, this.indexName())
        .build();
  }

  public static AuthoritativeIndexKey fromBson(DocumentParser parser) throws BsonParseException {
    return new AuthoritativeIndexKey(
        parser.getField(Fields.COLLECTION_UUID).unwrap(),
        parser.getField(Fields.INDEX_NAME).unwrap());
  }

  public static AuthoritativeIndexKey from(IndexDefinition indexDefinition) {
    return new AuthoritativeIndexKey(
        indexDefinition.getCollectionUuid(), indexDefinition.getName());
  }
}
