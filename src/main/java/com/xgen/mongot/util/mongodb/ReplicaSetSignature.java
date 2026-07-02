package com.xgen.mongot.util.mongodb;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

/**
 * A unique signature per replica set that is not reused upon shard removal and recreation.
 *
 * <p>Notice that the replica set IDs are not necessarily unique in the lifetime of a cluster.
 * Therefore, we use the replica set creation date to further identify the right replica set.
 *
 * <p><b>Note</b>: If any format-breaking changes are needed in this class, we first must create a
 * backward-compatible copy of this type and use it in IndexSnapshotMetadata instead of this one.
 *
 * @param rsId replica set ID
 * @param createDate creation date string
 */
public record ReplicaSetSignature(String rsId, String createDate) implements DocumentEncodable {

  static class Fields {
    static final Field.Required<String> ID = Field.builder("id").stringField().required();
    static final Field.Required<String> CREATE_DATE =
        Field.builder("createDate").stringField().required();
  }

  public static ReplicaSetSignature fromBson(DocumentParser parser) throws BsonParseException {
    return new ReplicaSetSignature(
        parser.getField(Fields.ID).unwrap(), parser.getField(Fields.CREATE_DATE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ID, this.rsId)
        .field(Fields.CREATE_DATE, this.createDate)
        .build();
  }
}
