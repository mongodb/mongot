package com.xgen.mongot.index;

import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.version.GenerationId;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/** An IndexFactory is able to create a new Index from its definition. */
public interface IndexFactory extends Closeable {

  /** Creates a new index. Can result in errors if index is already created. */
  Index getIndex(IndexDefinitionGeneration definitionGeneration)
      throws IOException, InvalidAnalyzerDefinitionException;

  /**
   * Creates a new InitializedIndex for the specified index. Can result in errors if index is
   * already initialized.
   */
  InitializedIndex getInitializedIndex(Index index, IndexDefinitionGeneration definitionGeneration)
      throws IOException;

  /**
   * Reads a generation's persisted commit user data from disk without contacting mongod. Empty when
   * no such data exists on disk or the factory is not disk-backed.
   */
  default Optional<EncodedUserData> readPersistedCommitUserData(
      IndexDefinitionGeneration definitionGeneration) {
    return Optional.empty();
  }

  /**
   * Whether {@code definitionGeneration} has everything needed to begin initialization.
   *
   * <p>Defaults to {@code true}. Implementations may override this for indexes that have additional
   * initialization prerequisites.
   */
  default boolean isReadyToInitialize(IndexDefinitionGeneration definitionGeneration) {
    return true;
  }

  /**
   * Deletes a generation's persisted data, identified only by its {@link GenerationId}.
   *
   * <p>Unlike dropping an {@link Index}, this needs no parsed index definition, so it can clean up
   * after a definition that could not be parsed.
   *
   * <p>Deliberately has no default: a factory with nothing to delete must say so, since the
   * fallback for one that does own data is silently leaking it.
   */
  void deleteGenerationData(GenerationId generationId) throws IOException;

  @Override
  void close();
}
