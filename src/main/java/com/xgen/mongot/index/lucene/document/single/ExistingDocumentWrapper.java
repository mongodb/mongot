package com.xgen.mongot.index.lucene.document.single;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.version.IndexCapabilities;
import org.apache.lucene.document.Document;

/**
 * A {@link ExistingDocumentWrapper} extends {@link AbstractDocumentWrapper} is a container that
 * contains a pre-existing Lucene {@code Document} and other information used for creating indexable
 * fields which are inserted into the existing Lucene Document.
 */
public class ExistingDocumentWrapper extends AbstractDocumentWrapper {

  ExistingDocumentWrapper(
      Document luceneDocument,
      IndexCapabilities indexCapabilities,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater) {
    super(luceneDocument, indexCapabilities, indexingMetricsUpdater, FeatureFlags.getDefault());
  }

  /**
   * Creates a new {@link ExistingDocumentWrapper}.
   *
   * @param luceneDocument - the Lucene document to wrap
   * @param indexCapabilities - the index capabilities of the document
   * @param indexingMetricsUpdater - the indexing metrics updater
   */
  public static ExistingDocumentWrapper create(
      Document luceneDocument,
      IndexCapabilities indexCapabilities,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater) {
    return new ExistingDocumentWrapper(luceneDocument, indexCapabilities, indexingMetricsUpdater);
  }
}
