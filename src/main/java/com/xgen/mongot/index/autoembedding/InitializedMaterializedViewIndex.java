package com.xgen.mongot.index.autoembedding;

import static com.xgen.mongot.embedding.mongodb.leasing.StaticLeaderLeaseManager.DEFAULT_INDEX_DEFINITION_VERSION;
import static com.xgen.mongot.index.definition.IndexDefinitionGeneration.Type.AUTO_EMBEDDING;
import static com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration.MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING;

import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexMetrics;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.IndexWriter;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.mongodb.MaterializedViewWriter;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonArray;

public class InitializedMaterializedViewIndex implements InitializedVectorIndex {
  private static final NoOpIndexReader NO_OP_INDEX_READER = new NoOpIndexReader();
  private final VectorIndexDefinition vectorIndexDefinition;
  private final MaterializedViewGenerationId generationId;
  private final IndexMetricsUpdater indexMetricsUpdater;
  private final MaterializedViewWriter materializedViewWriter;
  private final AtomicReference<IndexStatus> statusRef;
  private final LeaseManager leaseManager;

  public InitializedMaterializedViewIndex(
      MaterializedViewIndexDefinitionGeneration matViewDefinitionGeneration,
      MaterializedViewWriter materializedViewWriter,
      IndexMetricsUpdater indexMetricsUpdater,
      AtomicReference<IndexStatus> statusRef,
      LeaseManager leaseManager) {
    this.vectorIndexDefinition = matViewDefinitionGeneration.getIndexDefinition();
    this.generationId = matViewDefinitionGeneration.getGenerationId();
    this.indexMetricsUpdater = indexMetricsUpdater;
    this.materializedViewWriter = materializedViewWriter;
    this.statusRef = statusRef;
    this.leaseManager = leaseManager;
  }

  @Override
  public VectorIndexReader getReader() {
    return NO_OP_INDEX_READER;
  }

  @Override
  public IndexWriter getWriter() {
    return this.materializedViewWriter;
  }

  @Override
  public IndexMetricsUpdater getMetricsUpdater() {
    return this.indexMetricsUpdater;
  }

  @Override
  public IndexMetrics getMetrics() {
    return this.indexMetricsUpdater.getMetrics();
  }

  @Override
  public long getIndexSize() {
    return this.indexMetricsUpdater.getIndexSize();
  }

  @Override
  public void clear(EncodedUserData dropUserData) {}

  @Override
  public MaterializedViewGenerationId getGenerationId() {
    return this.generationId;
  }

  @Override
  public IndexDefinitionGeneration.Type getType() {
    return AUTO_EMBEDDING;
  }

  @Override
  public VectorIndexDefinition getDefinition() {
    return this.vectorIndexDefinition;
  }

  @Override
  public void setStatus(IndexStatus status) {
    try {
      this.leaseManager.updateReplicationStatus(
          this.generationId,
          this.vectorIndexDefinition
              .getDefinitionVersion()
              .orElse(DEFAULT_INDEX_DEFINITION_VERSION),
          status);
    } catch (MaterializedViewNonTransientException e) {
      this.statusRef.set(IndexStatus.failed("Corrupted Lease documents for this index."));
      return;
    } catch (MaterializedViewTransientException ignored) {
      // TODO(CLOUDP-371153): Revisit this to decide how to handle committing error.
    }
    this.statusRef.set(status);
  }

  @Override
  public IndexStatus getStatus() {
    return this.statusRef.get();
  }

  @Override
  public boolean isCompatibleWith(IndexDefinition indexDefinition) {
    // TODO(CLOUDP-360523): Implement compatibility check for redefined index.
    return indexDefinition.isAutoEmbeddingIndex()
        && indexDefinition.asVectorDefinition().getParsedAutoEmbeddingFeatureVersion()
            >= MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING;
  }

  @Override
  public void drop() throws IOException {}

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public void throwIfUnavailableForQuerying() throws IndexUnavailableException {}

  @Override
  public void close() throws IOException {}

  public UUID getMaterializedViewCollectionUuid() {
    return this.materializedViewWriter.getCollectionUuid();
  }

  static class NoOpIndexReader implements VectorIndexReader {
    @Override
    public BsonArray query(MaterializedVectorSearchQuery materializedQuery)
        throws ReaderClosedException, IOException, InvalidQueryException {
      return new BsonArray();
    }

    @Override
    public void refresh() throws IOException, ReaderClosedException {}

    @Override
    public void open() {}

    @Override
    public void close() {}

    @Override
    public long getRequiredMemoryForVectorData() throws ReaderClosedException {
      return 0;
    }
  }
}
