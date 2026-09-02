package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.ExceededLimitsException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.util.Crash;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class DefaultDocumentIndexer implements DocumentIndexer {

  private final DefaultKeyValueLogger logger;
  private final InitializedIndex index;
  private final IndexDefinition indexDefinition;
  // Either a constant holding the last staged blob or, for writers that assemble their position
  // from live state, a supplier read as the commit runs. Last write wins between the two.
  private final AtomicReference<Supplier<IndexCommitUserData>> commitUserDataSupplier;
  // Present only for the derived index of an auto-embedding composite. When present, each commit
  // snapshots the auto-embedding metadata (e.g. the lease's per-definition-version queryable
  // ratchet) so a restart that cannot reach mongod can read it back to report STALE rather than
  // FAILED. Kept opaque as a whole-metadata supplier so evolving the metadata schema does not touch
  // this class.
  private final Optional<Supplier<AutoEmbeddingCommitMetadata>> autoEmbeddingMetadataSupplier;

  private DefaultDocumentIndexer(
      InitializedIndex index,
      IndexDefinition indexDefinition,
      Optional<Supplier<AutoEmbeddingCommitMetadata>> autoEmbeddingMetadataSupplier) {
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", indexDefinition.getIndexId());
    this.logger = DefaultKeyValueLogger.getLogger(DefaultDocumentIndexer.class, defaultKeyValues);
    this.index = index;
    this.indexDefinition = indexDefinition;
    IndexCommitUserData persisted =
        IndexCommitUserData.fromEncodedData(
            index.getWriter().getCommitUserData(), Optional.of(index.getGenerationId()));
    this.commitUserDataSupplier = new AtomicReference<>(() -> persisted);
    this.autoEmbeddingMetadataSupplier = autoEmbeddingMetadataSupplier;
  }

  public static DefaultDocumentIndexer create(InitializedIndex index) {
    return new DefaultDocumentIndexer(index, index.getDefinition(), Optional.empty());
  }

  /**
   * Creates an indexer for the derived index of an auto-embedding composite. The supplier is read
   * on every commit to snapshot the current auto-embedding metadata into the commit user data.
   */
  public static DefaultDocumentIndexer create(
      InitializedIndex index, Supplier<AutoEmbeddingCommitMetadata> autoEmbeddingMetadataSupplier) {
    return new DefaultDocumentIndexer(
        index, index.getDefinition(), Optional.of(autoEmbeddingMetadataSupplier));
  }

  @Override
  public void indexDocumentEvent(DocumentEvent event) throws FieldExceededLimitsException {

    try {
      this.index.getWriter().updateIndex(event);

    } catch (IOException e) {
      Crash.because(
              String.format(
                  "index %s failed to update index",
                  this.indexDefinition.getIndexId().toHexString()))
          .withThrowable(e)
          .now();
    }
  }

  @Override
  public List<DocumentEvent> prepareBatch(List<DocumentEvent> events) {
    try {
      return this.index.getWriter().prepareBatch(events);
    } catch (IOException e) {
      Crash.because(
              String.format(
                  "index %s failed to prepare batch",
                  this.indexDefinition.getIndexId().toHexString()))
          .withThrowable(e)
          .now();
      return events;
    }
  }

  @Override
  public void updateCommitUserData(IndexCommitUserData commitUserData) {
    this.commitUserDataSupplier.set(() -> commitUserData);
  }

  @Override
  public void updateCommitUserDataSupplier(Supplier<IndexCommitUserData> commitUserDataSupplier) {
    this.commitUserDataSupplier.set(commitUserDataSupplier);
  }

  @Override
  public synchronized void commit() throws IOException {
    IndexCommitUserData base = this.commitUserDataSupplier.get().get();
    IndexCommitUserData userData =
        this.autoEmbeddingMetadataSupplier
            .map(supplier -> base.withAutoEmbeddingMetadata(supplier.get()))
            .orElse(base);
    this.index.getWriter().commit(userData.toEncodedData());
  }

  @Override
  public synchronized void clearIndex(IndexCommitUserData commitUserData) {
    this.index.clear(commitUserData.toEncodedData());
    updateCommitUserData(commitUserData);
  }

  @Override
  public Optional<ExceededLimitsException> exceededLimits() {
    Optional<ExceededLimitsException> maybeExceeded = this.index.getWriter().exceededLimits();
    maybeExceeded.ifPresent(
        exceeded -> this.logger.info("Index exceeded limits: {}", exceeded.getMessage()));
    return maybeExceeded;
  }

  @Override
  public IndexDefinition getIndexDefinition() {
    return this.indexDefinition;
  }
}
