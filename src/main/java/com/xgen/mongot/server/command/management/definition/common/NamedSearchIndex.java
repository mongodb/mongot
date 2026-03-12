package com.xgen.mongot.server.command.management.definition.common;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public record NamedSearchIndex(
    String name,
    IndexDefinition.Type type,
    BsonDocument definitionBson,
    UserIndexDefinition definition)
    implements DocumentEncodable {

  public NamedSearchIndex(
      String name, IndexDefinition.Type type, UserIndexDefinition indexDefinition) {
    this(name, type, indexDefinition.toBson(), indexDefinition);
  }

  private static class Fields {
    static final Field.WithDefault<String> NAME =
        Field.builder("name")
            .stringField()
            .optional()
            .withDefault(CommonDefinitions.DEFAULT_INDEX_NAME);
    static final Field.WithDefault<IndexDefinition.Type> TYPE =
        Field.builder("type")
            .enumField(IndexDefinition.Type.class)
            .asCamelCase()
            .optional()
            .withDefault(IndexDefinition.Type.SEARCH);

    static final Field.Required<BsonDocument> DEFINITION =
        Field.builder("definition").documentField().required();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.NAME, this.name)
        .field(Fields.TYPE, this.type)
        .field(Fields.DEFINITION, this.definition.toBson())
        .build();
  }

  public static NamedSearchIndex fromBson(DocumentParser parser) throws BsonParseException {
    IndexDefinition.Type type = parser.getField(Fields.TYPE).unwrap();
    BsonDocument definitionBson = parser.getField(Fields.DEFINITION).unwrap();
    BsonDocumentParser definitionParser =
        BsonDocumentParser.withContext(parser.getContext(), definitionBson).build();
    UserIndexDefinition definition = UserIndexDefinition.fromBson(definitionParser, type);

    return new NamedSearchIndex(
        parser.getField(Fields.NAME).unwrap(), type, definitionBson, definition);
  }
}
