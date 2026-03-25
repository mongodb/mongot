package com.xgen.mongot.index.definition;

import com.xgen.mongot.index.version.MaterializedViewGeneration;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public record MaterializedViewIndexDefinitionGeneration(
    IndexDefinition definition, MaterializedViewGeneration generation)
    implements IndexDefinitionGeneration {

  /**
   * The minimum version {@link IndexDefinition#getParsedAutoEmbeddingFeatureVersion} to support
   * Materialized View based auto-embedding feature.
   */
  public static final int MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING = 2;

  public static MaterializedViewIndexDefinitionGeneration fromBson(DocumentParser parser)
      throws BsonParseException {
    IndexDefinition definition = IndexDefinition.fromBson(parser);
    return new MaterializedViewIndexDefinitionGeneration(
        definition, new MaterializedViewGeneration(parser.getField(Fields.GENERATION).unwrap()));
  }

  @Override
  public MaterializedViewIndexDefinitionGeneration upgradeToCurrentFormatVersion() {
    throw new UnsupportedOperationException("upgradeToCurrentFormatVersion is not supported");
  }

  @Override
  public MaterializedViewIndexDefinitionGeneration incrementAttempt() {
    MaterializedViewGeneration generation = this.generation.nextAttempt();
    return new MaterializedViewIndexDefinitionGeneration(this.definition, generation);
  }

  public MaterializedViewIndexDefinitionGeneration incrementUser() {
    MaterializedViewGeneration generation = this.generation.incrementUser();
    return new MaterializedViewIndexDefinitionGeneration(this.definition, generation);
  }

  @Override
  public IndexDefinition getIndexDefinition() {
    return this.definition;
  }

  @Override
  public ObjectId getIndexId() {
    return this.definition.getIndexId();
  }

  @Override
  public MaterializedViewGenerationId getGenerationId() {
    return generation().generationId(getIndexId());
  }

  @Override
  public BsonDocument toBson() {
    var doc = this.definition.toBson();
    doc.putAll(BsonDocumentBuilder.builder().field(Fields.GENERATION, this.generation).build());
    return doc;
  }

  @Override
  public Type getType() {
    return Type.AUTO_EMBEDDING;
  }

  /**
   * Checks if an index uses the materialized view based embedding strategy.
   *
   * <p>AUTO_EMBED fields (version >= 2) use the EMBEDDING_MATERIALIZED_VIEW strategy which supports
   * partial updates. TEXT fields (version 1) use the old EMBEDDING strategy which writes directly
   * to Lucene and does not support partial updates.
   */
  public static boolean isMaterializedViewBasedIndex(IndexDefinition indexDefinition) {
    return indexDefinition.isAutoEmbeddingIndex()
        && indexDefinition.getParsedAutoEmbeddingFeatureVersion()
            >= MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING;
  }

  public static boolean isMaterializedViewBasedIndex(
      IndexDefinitionGeneration definitionGeneration) {
    return isMaterializedViewBasedIndex(definitionGeneration.getIndexDefinition());
  }
}
