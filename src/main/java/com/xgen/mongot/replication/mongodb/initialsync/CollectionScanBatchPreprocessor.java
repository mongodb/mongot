package com.xgen.mongot.replication.mongodb.initialsync;

import com.google.common.base.Stopwatch;
import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.bson.RawBsonDocument;

/**
 * Preprocesses the raw documents of a collection scan batch into the events to index. Shared by the
 * single-cursor and parallel scanners so that both unwrap views and skip malformed documents
 * identically.
 */
final class CollectionScanBatchPreprocessor {

  static final String SKIPPED_DOCUMENTS_WITHOUT_METADATA_NAMESPACE =
      "skippedDocumentsWithoutMetadataNamespace";

  static final String PREPROCESSING_BATCH_DURATIONS = "collectionScanPreprocessingBatchDurations";

  private static final FluentLogger LOG = FluentLogger.forEnclosingClass();

  private final InitialSyncContext context;
  private final Counter skippedDocumentsWithoutMetadataNamespaceCounter;
  private final Timer preprocessingBatchTimer;

  CollectionScanBatchPreprocessor(InitialSyncContext context, MetricsFactory metricsFactory) {
    this.context = context;
    this.skippedDocumentsWithoutMetadataNamespaceCounter =
        metricsFactory.counter(SKIPPED_DOCUMENTS_WITHOUT_METADATA_NAMESPACE);
    this.preprocessingBatchTimer = metricsFactory.timerSec(PREPROCESSING_BATCH_DURATIONS);
  }

  List<DocumentEvent> preprocess(List<RawBsonDocument> batch) {
    var preprocessingBatchTimer = Stopwatch.createStarted();
    List<DocumentEvent> documentEvents =
        batch.stream()
            .map(
                (doc) -> {
                  // for views, we use the metadata namespace to get the deleted flag and unmodified
                  // _id
                  if (this.context.getIndexDefinition().getView().isPresent()) {
                    DocumentMetadata metadata =
                        DocumentMetadata.fromMetadataNamespace(
                            Optional.ofNullable(doc), this.context.getIndexId());

                    // missing _id means mongod did not properly $project it, likely due to
                    // undefined behavior with duplicated fields (HELP-60413). other fields can also
                    // be corrupted, depending on the number of duplicates, we do not index such
                    // documents.
                    if (metadata.getId().isEmpty()) {
                      this.skippedDocumentsWithoutMetadataNamespaceCounter.increment();
                      LOG.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
                          "Document %s did not contain metadata namespace, likely due to "
                              + "presence of duplicate fields, skipping indexing this document",
                          Optional.ofNullable(doc).map(DocumentMetadata::extractId));
                      return Optional.<DocumentEvent>empty();
                    }

                    return Optional.of(DocumentEvent.createInsert(metadata, doc));
                  }

                  // for collections, we only need _id from the root of the document
                  DocumentMetadata metadata =
                      DocumentMetadata.fromOriginalDocument(Optional.ofNullable(doc));
                  return Optional.of(DocumentEvent.createInsert(metadata, doc));
                })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    this.preprocessingBatchTimer.record(preprocessingBatchTimer.stop().elapsed());

    return documentEvents;
  }
}
