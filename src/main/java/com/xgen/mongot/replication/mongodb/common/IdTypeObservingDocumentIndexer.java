package com.xgen.mongot.replication.mongodb.common;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.ExceededLimitsException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.definition.IndexDefinition;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DocumentIndexer} decorator that observes the {@code _id} BSON type of the first
 * document indexed and reports it via a provided callback. The callback fires at most once.
 */
public final class IdTypeObservingDocumentIndexer implements DocumentIndexer {

  private static final Logger LOG =
      LoggerFactory.getLogger(IdTypeObservingDocumentIndexer.class);

  private final DocumentIndexer delegate;
  private final Consumer<String> onFirstIdType;
  private final AtomicBoolean reported = new AtomicBoolean(false);

  private IdTypeObservingDocumentIndexer(
      DocumentIndexer delegate, Consumer<String> onFirstIdType) {
    this.delegate = delegate;
    this.onFirstIdType = onFirstIdType;
  }

  public static IdTypeObservingDocumentIndexer wrap(
      DocumentIndexer delegate, Consumer<String> onFirstIdType) {
    return new IdTypeObservingDocumentIndexer(delegate, onFirstIdType);
  }

  @VisibleForTesting
  public DocumentIndexer getDelegate() {
    return this.delegate;
  }

  @Override
  public void indexDocumentEvent(DocumentEvent event) throws FieldExceededLimitsException {
    if (this.reported.compareAndSet(false, true)) {
      try {
        BsonValue documentId = event.getDocumentId();
        String typeName = documentId.getBsonType().name();
        this.onFirstIdType.accept(typeName);
      } catch (Exception e) {
        this.reported.set(false);
        LOG.warn("Failed to observe _id field type from document, ignoring.", e);
      }
    }
    this.delegate.indexDocumentEvent(event);
  }

  @Override
  public List<DocumentEvent> prepareBatch(List<DocumentEvent> events) {
    return this.delegate.prepareBatch(events);
  }

  @Override
  public void updateCommitUserData(IndexCommitUserData commitUserData) {
    this.delegate.updateCommitUserData(commitUserData);
  }

  @Override
  public void commit() throws IOException {
    this.delegate.commit();
  }

  @Override
  public void clearIndex(IndexCommitUserData commitUserData) throws IOException {
    this.delegate.clearIndex(commitUserData);
  }

  @Override
  public Optional<ExceededLimitsException> exceededLimits() {
    return this.delegate.exceededLimits();
  }

  @Override
  public IndexDefinition getIndexDefinition() {
    return this.delegate.getIndexDefinition();
  }
}
