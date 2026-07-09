package com.xgen.mongot.index;

import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
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

  @Override
  void close();
}
