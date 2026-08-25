package com.xgen.mongot.index.definition;

import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.types.ObjectId;

/**
 * Represents an index definition at a given point in time. For instance, one index may have a few
 * of these before and after we bump a backend version or a user modifies the index definition.
 */
public sealed interface IndexDefinitionGeneration extends DocumentEncodable
    permits SearchIndexDefinitionGeneration,
        VectorIndexDefinitionGeneration,
        MaterializedViewIndexDefinitionGeneration {

  class Fields {
    public static final Field.Required<Generation> GENERATION =
        Field.builder("generation")
            .classField(Generation::fromBson)
            .disallowUnknownFields()
            .required();
  }

  enum Type {
    SEARCH,
    VECTOR,
    AUTO_EMBEDDING
  }

  Generation generation();

  default GenerationId getGenerationId() {
    return this.generation().generationId(getIndexId());
  }

  IndexDefinition getIndexDefinition();

  IndexDefinitionGeneration upgradeToCurrentFormatVersion();

  IndexDefinitionGeneration incrementAttempt();

  ObjectId getIndexId();

  Type getType();

  default SearchIndexDefinitionGeneration asSearch() {
    Check.expectedType(Type.SEARCH, getType());
    return (SearchIndexDefinitionGeneration) this;
  }

  default VectorIndexDefinitionGeneration asVector() {
    Check.expectedType(Type.VECTOR, getType());
    return (VectorIndexDefinitionGeneration) this;
  }

  default MaterializedViewIndexDefinitionGeneration asMaterializedView() {
    Check.expectedType(Type.AUTO_EMBEDDING, getType());
    return (MaterializedViewIndexDefinitionGeneration) this;
  }
}
