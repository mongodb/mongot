package com.xgen.mongot.index.definition;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public record SearchIndexDefinitionGeneration(
    AnalyzerBoundSearchIndexDefinition definition, Generation generation)
    implements IndexDefinitionGeneration {

  public static SearchIndexDefinitionGeneration fromBson(DocumentParser parser)
      throws BsonParseException {
    AnalyzerBoundSearchIndexDefinition definition =
        AnalyzerBoundSearchIndexDefinition.fromBson(parser);
    return new SearchIndexDefinitionGeneration(
        definition, parser.getField(Fields.GENERATION).unwrap());
  }

  @Override
  public IndexDefinitionGeneration upgradeToCurrentFormatVersion() {
    checkArg(
        !hasCurrentFormatVersion(),
        "Attempting to upgrade index but this Generation is already current.");
    return new SearchIndexDefinitionGeneration(this.definition, Generation.CURRENT);
  }

  public SearchIndexDefinitionGeneration incrementUser(
      AnalyzerBoundSearchIndexDefinition newDefinition) {
    Generation generation = this.generation.incrementUser();
    return new SearchIndexDefinitionGeneration(newDefinition, generation);
  }

  @Override
  public SearchIndexDefinitionGeneration incrementAttempt() {
    Generation generation = this.generation.nextAttempt();
    return new SearchIndexDefinitionGeneration(this.definition, generation);
  }

  @Override
  public SearchIndexDefinition getIndexDefinition() {
    return this.definition.indexDefinition();
  }

  @Override
  public ObjectId getIndexId() {
    return this.definition.indexDefinition().getIndexId();
  }

  @Override
  public BsonDocument toBson() {
    var doc = this.definition.toBson();
    doc.putAll(BsonDocumentBuilder.builder().field(Fields.GENERATION, this.generation).build());
    return doc;
  }

  @Override
  public Type getType() {
    return Type.SEARCH;
  }
}
