package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import org.bson.BsonDocument;

/**
 * The top-level {@code indexNamespace} option of a vector index definition, e.g. {@code
 * indexNamespace: { path: "tenantID" }}. The value of the document field at {@code path} partitions
 * the index: each distinct value gets its own namespace, and queries against the index must supply
 * an {@code indexNamespace} equality selecting the namespace to search.
 */
public record IndexNamespaceDefinition(FieldPath path) implements DocumentEncodable {

  public static class Fields {
    static final Field.Required<FieldPath> PATH =
        Field.builder("path").classField(FieldPathField::parse, FieldPathField::encode).required();
  }

  public static IndexNamespaceDefinition fromBson(DocumentParser parser) throws BsonParseException {
    return new IndexNamespaceDefinition(parser.getField(Fields.PATH).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.PATH, this.path).build();
  }
}
