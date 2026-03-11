package com.xgen.mongot.index.lucene.document.single;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.document.context.IndexingPolicyBuilderContext;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.document.Document;

/**
 * A {@link VectorIndexDocumentWrapper} extends {@link AbstractDocumentWrapper} and is a container
 * that contains a Lucene {@code Document} and other information used for creating indexable fields
 * for vector indexes.
 */
public class VectorIndexDocumentWrapper extends AbstractDocumentWrapper {

  private final Optional<FieldPath> embeddedRoot;

  VectorIndexDocumentWrapper(
      Document luceneDocument,
      IndexCapabilities indexCapabilities,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater,
      Optional<FieldPath> embeddedRoot) {
    super(luceneDocument, indexCapabilities, indexingMetricsUpdater);
    this.embeddedRoot = embeddedRoot;
  }

  public static VectorIndexDocumentWrapper createRoot(
      byte[] id,
      IndexCapabilities indexCapabilities,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater,
      IndexingPolicyBuilderContext context) {
    Document luceneDocument = new Document();
    VectorIndexDocumentWrapper wrapper =
        new VectorIndexDocumentWrapper(
            luceneDocument, indexCapabilities, indexingMetricsUpdater, Optional.empty());

    if (context.customVectorEngineId().isPresent()) {
      long customVectorEngineId = context.customVectorEngineId().get();
      IndexableFieldFactory.addCustomVectorEngineIdField(wrapper, customVectorEngineId);
      IndexableFieldFactory.addDocumentIdField(wrapper, id, true);
    } else {
      IndexableFieldFactory.addDocumentIdField(wrapper, id, false);
    }

    IndexableFieldFactory.addEmbeddedRootField(wrapper);
    return wrapper;
  }

  /**
   * Create a {@link VectorIndexDocumentWrapper} for an embedded Lucene document within a vector
   * index. This is used when indexing vectors inside array subdocuments.
   *
   * <p>Note: The document ID field must have the same structure (with or without doc values) across
   * all documents in the block (root and embedded). Vector indexes do not support sorting on _id,
   * so we use false for includeDocValue to match the root document.
   *
   * @param id The document ID (shared across all documents in the block)
   * @param embeddedRoot The field path to the embedded document root
   * @param indexCapabilities Index capabilities
   * @param indexingMetricsUpdater Metrics updater for indexing operations
   * @return A new VectorIndexDocumentWrapper for an embedded document
   */
  public static VectorIndexDocumentWrapper createEmbedded(
      byte[] id,
      FieldPath embeddedRoot,
      IndexCapabilities indexCapabilities,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater) {
    Document luceneDocument = new Document();
    VectorIndexDocumentWrapper wrapper =
        new VectorIndexDocumentWrapper(
            luceneDocument, indexCapabilities, indexingMetricsUpdater, Optional.of(embeddedRoot));
    // Use false for includeDocValue to match the root document structure
    IndexableFieldFactory.addDocumentIdField(wrapper, id, false);
    IndexableFieldFactory.addEmbeddedPathField(wrapper);
    return wrapper;
  }

  @Override
  Optional<FieldPath> getEmbeddedRoot() {
    return this.embeddedRoot;
  }
}
