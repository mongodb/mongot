package com.xgen.mongot.server.command.management.definition.common;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * Represents the user-facing vector search index definition.
 * https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-search-type/#mongodb-vector-search-index-fields
 */
public record UserVectorIndexDefinition(
    List<VectorIndexFieldDefinition> fields,
    int numPartitions,
    Optional<StoredSourceDefinition> storedSource,
    Optional<FieldPath> nestedRoot)
    implements UserIndexDefinition {

  private static class Fields {
    private static Optional<String> fieldValidator(List<VectorIndexFieldDefinition> fields) {
      long uniquePathsCount =
          fields.stream().map(VectorIndexFieldDefinition::getPath).distinct().count();
      if (uniquePathsCount < fields.size()) {
        return Optional.of("Vector and filter fields should have distinct paths");
      } else {
        return Optional.empty();
      }
    }

    static final Field.Required<List<VectorIndexFieldDefinition>> FIELDS =
        Field.builder("fields")
            .classField(VectorIndexFieldDefinition::fromBson, VectorIndexFieldDefinition::toBson)
            .disallowUnknownFields()
            .asList()
            .validate(Fields::fieldValidator)
            .required();

    static final Field.Optional<FieldPath> NESTED_ROOT =
        Field.builder("nestedRoot")
            .classField(FieldPathField::parse, FieldPathField::encode)
            .optional()
            .noDefault();
  }

  @Override
  public BsonDocument toBson() {
    @Var var builder = BsonDocumentBuilder.builder().field(Fields.FIELDS, this.fields);
    if (this.numPartitions() != UserIndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()) {
      builder = builder.field(UserIndexDefinition.Fields.NUM_PARTITIONS, this.numPartitions());
    }
    if (this.storedSource().isPresent()) {
      builder = builder.field(UserIndexDefinition.Fields.STORED_SOURCE, this.storedSource());
    }
    if (this.nestedRoot().isPresent()) {
      builder = builder.field(Fields.NESTED_ROOT, this.nestedRoot());
    }
    return builder.build();
  }

  public static UserVectorIndexDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new UserVectorIndexDefinition(
        parser.getField(Fields.FIELDS).unwrap(),
        parser.getField(UserIndexDefinition.Fields.NUM_PARTITIONS).unwrap(),
        parser.getField(UserIndexDefinition.Fields.STORED_SOURCE).unwrap(),
        parser.getField(Fields.NESTED_ROOT).unwrap());
  }
}
