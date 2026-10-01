package com.xgen.mongot.index.query.sort;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import org.bson.BsonDocument;

/**
 * The array path and child-selecting operator of an {@code embeddedDocument}-scoped sort key.
 *
 * <p>Only the operator is kept as raw BSON: the {@code operators} package depends on {@code sort},
 * so interpreting it belongs to the query layer. Serialization uses the parsed path as its single
 * source of truth. A fully parsed scope is deferred to CLOUDP-441316.
 */
public record EmbeddedSortScope(FieldPath path, BsonDocument operator)
    implements DocumentEncodable {
  static class Fields {
    static final Field.Required<FieldPath> PATH =
        Field.builder("path").classField(FieldPathField::parse, FieldPathField::encode).required();

    static final Field.Required<BsonDocument> OPERATOR =
        Field.builder("operator").documentField().required();
  }

  public static EmbeddedSortScope fromBson(DocumentParser parser) throws BsonParseException {
    return new EmbeddedSortScope(
        parser.getField(Fields.PATH).unwrap(), parser.getField(Fields.OPERATOR).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.path)
        .field(Fields.OPERATOR, this.operator)
        .build();
  }
}
