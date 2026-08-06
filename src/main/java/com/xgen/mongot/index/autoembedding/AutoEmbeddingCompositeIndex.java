package com.xgen.mongot.index.autoembedding;

import static com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration.MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING;

import com.google.errorprone.annotations.Var;
import com.mongodb.lang.Nullable;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.status.IndexStatus.StatusCode;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.ResumeTokenUtils;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.apache.commons.codec.DecoderException;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Composite Index type for AutoEmbeddingIndexGeneration used for consolidating index status from
 * both Materialized View and Lucene. Supports both vector and search derived indexes.
 */
public class AutoEmbeddingCompositeIndex implements Index {
  public final InitializedMaterializedViewIndex matViewIndex;
  public final Index derivedIndex;
  // Supplier to lazily fetch the initialized derived index from catalog
  private final Supplier<Optional<InitializedIndex>> initializedIndexSupplier;
  private static final Logger LOG = LoggerFactory.getLogger(AutoEmbeddingCompositeIndex.class);

  // Track previous status to log only on transitions
  @Nullable private StatusCode previousConsolidatedStatus = null;
  // One-way ratchet: set to true at runtime only when the MV itself is STEADY and the
  // composite canServiceQueries(). Also pre-initialized from the persisted lease at
  // construction, so a previously-queryable index is not regressed by an MV initialization
  // gap on restart (e.g. forceDisableReplication) — the path where MV is UNKNOWN but
  // Lucene is STEADY does not flip this ratchet; the lease handles it instead.
  private final AtomicBoolean compositeWasQueryable;

  // Live check for whether replication is currently stopped (cluster pause / disk-full /
  // forceDisableReplication). Gates the (UNKNOWN/NOT_STARTED MV, STEADY Lucene) → synthetic STEADY
  // fallthrough in computeConsolidatedStatus: without this gate the fallthrough also fires for
  // followers at startup (whose MV is legitimately UNKNOWN because the leader owns it), which
  // breaks host-failover takeover by reporting STEADY before the follower has any MV state.
  private final BooleanSupplier isReplicationStoppedSupplier;
  private final Optional<AutoEmbeddingGenerationMetrics> generationMetrics;

  public AutoEmbeddingCompositeIndex(
      InitializedMaterializedViewIndex matViewIndex,
      Index derivedIndex,
      Supplier<Optional<InitializedIndex>> initializedIndexSupplier,
      BooleanSupplier isReplicationStoppedSupplier) {
    this(
        matViewIndex,
        derivedIndex,
        initializedIndexSupplier,
        isReplicationStoppedSupplier,
        Optional.empty());
  }

  AutoEmbeddingCompositeIndex(
      InitializedMaterializedViewIndex matViewIndex,
      Index derivedIndex,
      Supplier<Optional<InitializedIndex>> initializedIndexSupplier,
      BooleanSupplier isReplicationStoppedSupplier,
      AutoEmbeddingGenerationMetrics generationMetrics) {
    this(
        matViewIndex,
        derivedIndex,
        initializedIndexSupplier,
        isReplicationStoppedSupplier,
        Optional.of(generationMetrics));
  }

  private AutoEmbeddingCompositeIndex(
      InitializedMaterializedViewIndex matViewIndex,
      Index derivedIndex,
      Supplier<Optional<InitializedIndex>> initializedIndexSupplier,
      BooleanSupplier isReplicationStoppedSupplier,
      Optional<AutoEmbeddingGenerationMetrics> generationMetrics) {
    this.matViewIndex = matViewIndex;
    this.derivedIndex = derivedIndex;
    this.initializedIndexSupplier = initializedIndexSupplier;
    this.isReplicationStoppedSupplier = isReplicationStoppedSupplier;
    this.generationMetrics = generationMetrics;
    this.compositeWasQueryable =
        new AtomicBoolean(matViewIndex.isCurrentVersionQueryablePerLease());
  }

  public AutoEmbeddingCompositeIndex(
      InitializedMaterializedViewIndex matViewIndex,
      Index derivedIndex,
      Supplier<Optional<InitializedIndex>> initializedIndexSupplier) {
    this(matViewIndex, derivedIndex, initializedIndexSupplier, () -> false);
  }

  public AutoEmbeddingCompositeIndex(
      InitializedMaterializedViewIndex matViewIndex, Index derivedIndex) {
    this(matViewIndex, derivedIndex, Optional::empty, () -> false);
  }

  @Override
  public IndexDefinition getDefinition() {
    return this.matViewIndex.getDefinition();
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
    IndexStatus luceneStatus = this.derivedIndex.getStatus();

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
    return this.derivedIndex.isCompatibleWith(indexDefinition);
  }

  @Override
  public void drop() throws IOException {
    this.matViewIndex.drop();
    this.derivedIndex.drop();
  }

  @Override
  public boolean isClosed() {
    return this.matViewIndex.isClosed() && this.derivedIndex.isClosed();
  }

  @Override
  public void throwIfUnavailableForQuerying() throws IndexUnavailableException {
    // TODO(CLOUDP-356241): Implement matview::throwIfUnavailableForQuerying
    this.derivedIndex.throwIfUnavailableForQuerying();
  }

  @Override
  public void close() throws IOException {
    try {
      @Var Exception first = null;
      // Try each index independently so one close failure does not skip the other.
      try {
        this.matViewIndex.close();
      } catch (IOException | RuntimeException e) {
        first = e;
      }
      try {
        this.derivedIndex.close();
      } catch (IOException | RuntimeException e) {
        if (first == null) {
          first = e;
        } else {
          first.addSuppressed(e);
        }
      }
      if (first instanceof IOException ioException) {
        throw ioException;
      }
      if (first instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
    } finally {
      this.generationMetrics.ifPresent(AutoEmbeddingGenerationMetrics::close);
    }
  }

  public MaterializedViewSchemaMetadata getSchemaMetadata() {
    return this.matViewIndex.getSchemaMetadata();
  }

  /** Updates effective lucene status by checking if lucene has caught up to MV's steady position */
  private IndexStatus calculateEffectiveLuceneStatus(IndexStatus luceneStatus) {
    if (luceneStatus.getStatusCode() != StatusCode.STEADY) {
      return luceneStatus;
    }

    // If the composite was previously queryable, Lucene is still serving queries correctly for the
    // existing definition. Skip the lag check to avoid regressing a live index to INITIAL_SYNC
    // (e.g. after a definition change clears steadyAsOfOplogPosition or advances it past Lucene).
    if (this.compositeWasQueryable.get()) {
      return luceneStatus;
    }

    // First build: verify Lucene has caught up to the position where MV first became STEADY.
    Optional<BsonTimestamp> steadyPosition = this.matViewIndex.getSteadyAsOfOplogPosition();
    if (steadyPosition.isEmpty()) {
      // No position recorded yet (e.g. MV just became STEADY but highWaterMark not yet set);
      // can't determine lag so pass through Lucene's status.
      return luceneStatus;
    }

    Optional<BsonTimestamp> lucenePosition = extractLucenePosition();
    boolean caughtUp =
        lucenePosition.isPresent() && lucenePosition.get().compareTo(steadyPosition.get()) >= 0;
    return caughtUp ? luceneStatus : new IndexStatus(StatusCode.INITIAL_SYNC);
  }

  /** Extract oplog position of the lucene index */
  private Optional<BsonTimestamp> extractLucenePosition() {
    Optional<InitializedIndex> luceneOpt = this.initializedIndexSupplier.get();
    if (luceneOpt.isEmpty()) {
      return Optional.empty();
    }
    InitializedIndex lucene = luceneOpt.get();

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

    // Only set compositeWasQueryable when both MV and Lucene statuses are STEADY. The
    // (UNKNOWN/NOT_STARTED, STEADY) fallthrough in computeConsolidatedStatus() returns a
    // synthetic STEADY so that processReplicationStatus() can apply the STEADY→STALE transition.
    if (result.canServiceQueries() && mvStatus == StatusCode.STEADY) {
      this.compositeWasQueryable.set(true);
    }

    // Log on status transitions
    if (this.previousConsolidatedStatus != newStatus) {
      LOG.atInfo()
          .addKeyValue("generationId", this.matViewIndex.getGenerationId())
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

    // FAILED on a component propagates with a fixed, customer-safe message naming the failing stage
    // (MV takes precedence). The underlying component error is intentionally not surfaced here; it
    // remains on the component index's own status for internal diagnostics.
    if (mvStatus == StatusCode.FAILED) {
      return IndexStatus.failed(
          AutoEmbeddingFailureMessages.withFailurePrefix("embedding generation failed"));
    }
    if (luceneStatus == StatusCode.FAILED) {
      return IndexStatus.failed(
          AutoEmbeddingFailureMessages.withFailurePrefix("index build failed"));
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

    // UNKNOWN or NOT_STARTED on the MV component propagates, except when Lucene is STEADY AND
    // replication is currently stopped. When Lucene is STEADY but MV hasn't (re-)initialized
    // because replication is disabled (e.g. forceDisableReplication on restart), returning UNKNOWN
    // causes isTransient()=true in MMS, which filters the index before processReplicationStatus()
    // can apply the STEADY→STALE transition. Falling through to STEADY in that specific case lets
    // MMS correctly mark the index STALE. The replication-stopped gate is required to avoid the
    // same fallthrough firing for followers at normal startup (MV legitimately UNKNOWN because the
    // leader owns it) — without the gate, followers would report a synthetic STEADY before they
    // had any MV state, breaking host-failover takeover (CLOUDP-405298).
    if (mvStatus == StatusCode.UNKNOWN || mvStatus == StatusCode.NOT_STARTED) {
      if (luceneStatus != StatusCode.STEADY || !this.isReplicationStoppedSupplier.getAsBoolean()) {
        return new IndexStatus(mvStatus);
      }
      LOG.atInfo()
          .addKeyValue("generationId", this.matViewIndex.getGenerationId())
          .addKeyValue("mvStatus", mvStatus)
          .addKeyValue("luceneStatus", luceneStatus)
          .log(
              "Status fallthrough: returning synthetic STEADY because replication is stopped;"
                  + " lets MMS apply the STEADY→STALE transition");
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
