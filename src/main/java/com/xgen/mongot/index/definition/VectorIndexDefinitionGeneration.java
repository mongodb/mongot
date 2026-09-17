package com.xgen.mongot.index.definition;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.MaterializedViewGeneration;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public record VectorIndexDefinitionGeneration(
    VectorIndexDefinition definition, Generation generation) implements IndexDefinitionGeneration {

  public VectorIndexDefinitionGeneration {
    Check.checkArg(
        !(generation instanceof MaterializedViewGeneration),
        "MaterializedViewGeneration is not allowed in VectorIndexDefinitionGeneration");
  }

  public static VectorIndexDefinitionGeneration fromBson(DocumentParser parser)
      throws BsonParseException {
    VectorIndexDefinition definition = VectorIndexDefinition.fromBson(parser);
    return new VectorIndexDefinitionGeneration(
        definition, parser.getField(Fields.GENERATION).unwrap());
  }

  @Override
  public IndexDefinitionGeneration upgradeToCurrentFormatVersion() {
    checkArg(
        !hasCurrentFormatVersion(),
        "Attempting to upgrade index but this Generation is already current.");
    return new VectorIndexDefinitionGeneration(
        this.definition, this.definition.newGeneration(UserIndexVersion.FIRST));
  }

  public VectorIndexDefinitionGeneration incrementUser(VectorIndexDefinition newDefinition) {
    Generation generation = this.generation.incrementUser();
    return new VectorIndexDefinitionGeneration(newDefinition, generation);
  }

  @Override
  public VectorIndexDefinitionGeneration incrementAttempt() {
    Generation generation = this.generation.nextAttempt();
    return new VectorIndexDefinitionGeneration(this.definition, generation);
  }

  @Override
  public VectorIndexDefinition getIndexDefinition() {
    return this.definition;
  }

  @Override
  public ObjectId getIndexId() {
    return this.definition.getIndexId();
  }

  @Override
  public BsonDocument toBson() {
    var doc = this.definition.toBson();
    doc.putAll(BsonDocumentBuilder.builder().field(Fields.GENERATION, this.generation).build());
    return doc;
  }

  @Override
  public Type getType() {
    return Type.VECTOR;
  }
}
