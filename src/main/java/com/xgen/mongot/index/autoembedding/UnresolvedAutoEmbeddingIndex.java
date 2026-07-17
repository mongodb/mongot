package com.xgen.mongot.index.autoembedding;

import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Auto-embedding index that could not resolve its materialized view (mongod unreachable or missing
 * sync source), kept to track status for retry. When the derived Lucene was committed to local disk
 * it carries a reconstructed derived index rebuilt offline from the persisted commit metadata:
 * status delegates to it (STALE) and {@code VectorSearchCommand} serves queries from it until
 * mongod returns. Otherwise it is a status-only stub reporting its FAILED/DOES_NOT_EXIST status.
 */
public class UnresolvedAutoEmbeddingIndex implements Index {

  private final IndexDefinition definition;
  // Used only when there is no reconstructed index to delegate to; only ConfigManager mutates it.
  private IndexStatus status;
  private final Optional<InitializedIndex> reconstructedDerivedIndex;
  private final Map<FieldPath, FieldPath> autoEmbeddingFieldsMapping;

  public UnresolvedAutoEmbeddingIndex(
      IndexDefinition definition,
      IndexStatus status,
      Optional<InitializedIndex> reconstructedDerivedIndex,
      Map<FieldPath, FieldPath> autoEmbeddingFieldsMapping) {
    this.definition = definition;
    this.status = status;
    this.reconstructedDerivedIndex = reconstructedDerivedIndex;
    this.autoEmbeddingFieldsMapping = autoEmbeddingFieldsMapping;
  }

  /** The offline-reconstructed derived Lucene index served while mongod is unreachable, if any. */
  public Optional<InitializedIndex> getReconstructedDerivedIndex() {
    return this.reconstructedDerivedIndex;
  }

  /**
   * The user-to-internal field mapping persisted with the reconstructed index, used to remap a
   * query's path to the derived Lucene path while serving offline. Empty when not serving offline.
   */
  public Map<FieldPath, FieldPath> getAutoEmbeddingFieldsMapping() {
    return this.autoEmbeddingFieldsMapping;
  }

  @Override
  public IndexDefinition getDefinition() {
    return this.definition;
  }

  @Override
  public void setStatus(IndexStatus status) {
    if (this.reconstructedDerivedIndex.isPresent()) {
      this.reconstructedDerivedIndex.get().setStatus(status);
      return;
    }
    this.status = status;
  }

  @Override
  public IndexStatus getStatus() {
    // While serving offline the reconstructed index owns the status (STALE).
    return this.reconstructedDerivedIndex.map(InitializedIndex::getStatus).orElse(this.status);
  }

  @Override
  public boolean isCompatibleWith(IndexDefinition indexDefinition) {
    return true;
  }

  @Override
  public void drop() throws IOException {
    // Delete the reconstructed derived Lucene's on-disk data so a phased-out generation does not
    // leak its directory (recovery swaps in a new attempt with its own directory).
    // TODO(CLOUDP-423170): when there is no reconstructed index, the directory from a prior healthy
    // run is not reclaimed here and leaks; clean it up for the FAILED case too.
    if (this.reconstructedDerivedIndex.isPresent()) {
      this.reconstructedDerivedIndex.get().drop();
    }
  }

  @Override
  public boolean isClosed() {
    // Always closed so that it won't be added to replication.
    return true;
  }

  @Override
  public void throwIfUnavailableForQuerying() throws IndexUnavailableException {
    // While serving offline the reconstructed index gates queryability (it is what actually serves
    // the query); a status-only stub has nothing to serve, so it stays a no-op and the query fails
    // downstream when no initialized index is found.
    if (this.reconstructedDerivedIndex.isPresent()) {
      this.reconstructedDerivedIndex.get().throwIfUnavailableForQuerying();
    }
  }

  @Override
  public void close() throws IOException {
    // Release the reconstructed reader if one was opened. Closing keeps on-disk data (drop()
    // deletes it) so a shutdown while still offline leaves the files for the next restart.
    if (this.reconstructedDerivedIndex.isPresent()) {
      this.reconstructedDerivedIndex.get().close();
    }
  }
}
