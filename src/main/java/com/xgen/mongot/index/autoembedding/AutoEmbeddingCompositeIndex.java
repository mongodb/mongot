package com.xgen.mongot.index.autoembedding;

import static com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration.MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING;

import com.mongodb.lang.Nullable;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedVectorIndex;
import com.xgen.mongot.index.VectorIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.ResumeTokenUtils;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.commons.codec.DecoderException;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Composite Index type for AutoEmbeddingIndexGeneration used for consolidating index status from
 * both Materialized View and Lucene.
 */
public class AutoEmbeddingCompositeIndex implements VectorIndex {
  public final InitializedMaterializedViewIndex matViewIndex;
  public final VectorIndex vectorIndex;
  // Supplier to lazily fetch the initialized vector index from catalog
  private final Supplier<Optional<InitializedVectorIndex>> initializedVectorIndexSupplier;
  private static final Logger LOG = LoggerFactory.getLogger(AutoEmbeddingCompositeIndex.class);

  // Track previous status to log only on transitions
  @Nullable private StatusCode previousConsolidatedStatus = null;

  public AutoEmbeddingCompositeIndex(
      InitializedMaterializedViewIndex matViewIndex,
      VectorIndex vectorIndex,
      Supplier<Optional<InitializedVectorIndex>> initializedVectorIndexSupplier) {
    this.matViewIndex = matViewIndex;
    this.vectorIndex = vectorIndex;
    this.initializedVectorIndexSupplier = initializedVectorIndexSupplier;
  }

  public AutoEmbeddingCompositeIndex(
      InitializedMaterializedViewIndex matViewIndex, VectorIndex vectorIndex) {
    this(matViewIndex, vectorIndex, Optional::empty);
  }

  @Override
  public VectorIndexDefinition getDefinition() {
    return this.matViewIndex.getDefinition();
  }

  public VectorIndexDefinition getDerivedDefinition() {
    return this.vectorIndex.getDefinition();
  }

  @Override
  public void setStatus(IndexStatus status) {
    throw new UnsupportedOperationException(
        "Don't set status directly on AutoEmbedding composite index.");
  }

  @Override
  public IndexStatus getStatus() {
    // Effective MV status persisted by StatusResolutionUtils
    IndexStatus effectiveMvStatus = this.matViewIndex.getStatus();
    IndexStatus luceneStatus = this.vectorIndex.getStatus();

    // There are scenarios where lucene will be behind the MV. Particularly during the initial
    // sync stage. We need to properly reflect this lag as the user facing status
    IndexStatus effectiveLuceneStatus = calculateEffectiveLuceneStatus(luceneStatus);

    return consolidateStatuses(
        effectiveMvStatus.getStatusCode(), effectiveLuceneStatus.getStatusCode());
  }

  @Override
  public boolean isCompatibleWith(IndexDefinition indexDefinition) {
    if (indexDefinition.isAutoEmbeddingIndex()
        && indexDefinition.getParsedAutoEmbeddingFeatureVersion()
            >= MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING) {
      return this.matViewIndex.isCompatibleWith(indexDefinition);
    }
    return this.vectorIndex.isCompatibleWith(indexDefinition);
  }

  @Override
  public void drop() throws IOException {
    this.matViewIndex.drop();
    this.vectorIndex.drop();
  }

  @Override
  public boolean isClosed() {
    return this.matViewIndex.isClosed() && this.vectorIndex.isClosed();
  }

  @Override
  public void throwIfUnavailableForQuerying() throws IndexUnavailableException {
    // TODO(CLOUDP-356241): Implement matview::throwIfUnavailableForQuerying
    this.vectorIndex.throwIfUnavailableForQuerying();
  }

  @Override
  public void close() throws IOException {
    this.matViewIndex.close();
    this.vectorIndex.close();
  }

  public MaterializedViewSchemaMetadata getSchemaMetadata() {
    return this.matViewIndex.getSchemaMetadata();
  }

  /** Updates effective lucene status by checking if lucene has caught up to MV's steady position */
  private IndexStatus calculateEffectiveLuceneStatus(IndexStatus luceneStatus) {
    if (luceneStatus.getStatusCode() != StatusCode.STEADY) {
      return luceneStatus;
    }

    Optional<BsonTimestamp> steadyPosition = this.matViewIndex.getSteadyAsOfOplogPosition();
    // If MV hasn't recorded a steady position yet, we can't determine lag.
    if (steadyPosition.isEmpty()) {
      return luceneStatus;
    }

    Optional<BsonTimestamp> lucenePosition = extractLucenePosition();
    // Lucene is STEADY only if MV is STEADY and Lucene has caught up
    boolean caughtUp =
        lucenePosition.isPresent() && lucenePosition.get().compareTo(steadyPosition.get()) >= 0;
    return caughtUp ? luceneStatus : new IndexStatus(StatusCode.INITIAL_SYNC);
  }

  /** Extract oplog position of the lucene index */
  private Optional<BsonTimestamp> extractLucenePosition() {
    Optional<InitializedVectorIndex> luceneOpt = this.initializedVectorIndexSupplier.get();
    if (luceneOpt.isEmpty()) {
      return Optional.empty();
    }
    InitializedVectorIndex lucene = luceneOpt.get();

    EncodedUserData commitData = lucene.getWriter().getCommitUserData();
    IndexCommitUserData userData =
        IndexCommitUserData.fromEncodedData(commitData, Optional.empty());
    return userData
        .getResumeInfo()
        .map(
            info -> {
              try {
                return ResumeTokenUtils.opTimeFromResumeToken(info.getResumeToken());
              } catch (DecoderException e) {
                LOG.atWarn()
                    .addKeyValue("indexId", getDefinition().getIndexId())
                    .addKeyValue("resumeToken", info.getResumeToken())
                    .setCause(e)
                    .log("Failed to decode opTime from resume token");
                return null;
              }
            });
  }

  private IndexStatus consolidateStatuses(StatusCode mvStatus, StatusCode luceneStatus) {
    IndexStatus result = computeConsolidatedStatus(mvStatus, luceneStatus);
    StatusCode newStatus = result.getStatusCode();

    // Log on status transitions
    if (this.previousConsolidatedStatus != newStatus) {
      LOG.atInfo()
          .addKeyValue("indexId", getDefinition().getIndexId())
          .addKeyValue("previousStatus", this.previousConsolidatedStatus)
          .addKeyValue("newStatus", newStatus)
          .addKeyValue("mvStatus", mvStatus)
          .addKeyValue("luceneStatus", luceneStatus)
          .log("Status transition");
      this.previousConsolidatedStatus = newStatus;
    }

    return result;
  }

  private IndexStatus computeConsolidatedStatus(StatusCode mvStatus, StatusCode luceneStatus) {
    // MV DOES_NOT_EXIST takes precedence over everything
    if (mvStatus == StatusCode.DOES_NOT_EXIST) {
      return IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND);
    }

    // FAILED status on any component propagates
    if (mvStatus == StatusCode.FAILED || luceneStatus == StatusCode.FAILED) {
      return IndexStatus.failed("Index failed");
    }

    // STALE status on any component propagates
    if (mvStatus == StatusCode.STALE || luceneStatus == StatusCode.STALE) {
      return new IndexStatus(StatusCode.STALE);
    }

    // DOES_NOT_EXIST on any component propagates
    if (luceneStatus == StatusCode.DOES_NOT_EXIST) {
      return IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND);
    }

    // INITIAL_SYNC on any component propagates
    if (mvStatus == StatusCode.INITIAL_SYNC || luceneStatus == StatusCode.INITIAL_SYNC) {
      return new IndexStatus(StatusCode.INITIAL_SYNC);
    }

    // UNKNOWN or NOT_STARTED on any component propagates
    if (mvStatus == StatusCode.UNKNOWN || mvStatus == StatusCode.NOT_STARTED) {
      return new IndexStatus(mvStatus);
    }
    if (luceneStatus == StatusCode.UNKNOWN || luceneStatus == StatusCode.NOT_STARTED) {
      return new IndexStatus(luceneStatus);
    }

    // RECOVERING_TRANSIENT or RECOVERING_NON_TRANSIENT propagates
    if (mvStatus == StatusCode.RECOVERING_TRANSIENT
        || mvStatus == StatusCode.RECOVERING_NON_TRANSIENT) {
      return new IndexStatus(mvStatus);
    }
    if (luceneStatus == StatusCode.RECOVERING_TRANSIENT
        || luceneStatus == StatusCode.RECOVERING_NON_TRANSIENT) {
      return new IndexStatus(luceneStatus);
    }

    // Both STEADY
    return IndexStatus.steady();
  }
}
