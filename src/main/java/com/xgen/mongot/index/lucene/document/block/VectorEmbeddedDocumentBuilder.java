package com.xgen.mongot.index.lucene.document.block;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater;
import com.xgen.mongot.index.definition.VectorEmbeddedDocumentsFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.index.lucene.document.builder.DocumentBlockBuilder;
import com.xgen.mongot.index.lucene.document.builder.DocumentBuilder;
import com.xgen.mongot.index.lucene.document.context.IndexingPolicyBuilderContext;
import com.xgen.mongot.index.lucene.document.single.LuceneVectorIndexDocumentBuilder;
import com.xgen.mongot.index.lucene.document.single.VectorIndexDocumentWrapper;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.jetbrains.annotations.Nullable;

/**
 * {@link VectorEmbeddedDocumentBuilder} is a {@link DocumentBlockBuilder} responsible for building
 * embedded Lucene documents for vector indexes. It wraps a {@link LuceneVectorIndexDocumentBuilder}
 * and manages the document block structure for embedded vectors.
 *
 * <p>This class is used when indexing vectors inside array subdocuments, creating child documents
 * in a Lucene block structure where each array element becomes a separate Lucene document.
 */
public class VectorEmbeddedDocumentBuilder implements DocumentBlockBuilder {

  private final DocumentHandler wrappedDocumentBuilder;
  private final DocumentBlock documentBlock;
  @Nullable private final VectorEmbeddedDocumentsFieldDefinition fieldDefinition;
  private final VectorIndexFieldMapping mapping;
  private final byte[] id;
  private final Optional<FieldPath> path;
  private final IndexingMetricsUpdater indexingMetricsUpdater;
  private final IndexCapabilities indexCapabilities;
  private final ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings;
  private final FeatureFlags featureFlags;

  private VectorEmbeddedDocumentBuilder(
      DocumentHandler wrappedDocumentBuilder,
      DocumentBlock documentBlock,
      @Nullable VectorEmbeddedDocumentsFieldDefinition fieldDefinition,
      VectorIndexFieldMapping mapping,
      byte[] id,
      Optional<FieldPath> path,
      IndexingMetricsUpdater indexingMetricsUpdater,
      IndexCapabilities indexCapabilities,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings,
      FeatureFlags featureFlags) {
    this.wrappedDocumentBuilder = wrappedDocumentBuilder;
    this.documentBlock = documentBlock;
    this.fieldDefinition = fieldDefinition;
    this.mapping = mapping;
    this.id = id;
    this.path = path;
    this.indexingMetricsUpdater = indexingMetricsUpdater;
    this.indexCapabilities = indexCapabilities;
    this.autoEmbeddings = autoEmbeddings;
    this.featureFlags = featureFlags;
  }

  /**
   * Create a root {@link VectorEmbeddedDocumentBuilder} that wraps an existing document builder.
   * This is used at the top level when the index has embedded vector fields.
   *
   * @param wrappedDocumentBuilder The wrapped document builder
   * @param mapping The vector index field mapping
   * @param id The document ID
   * @param indexingMetricsUpdater Metrics updater for indexing operations
   * @param indexCapabilities Index capabilities
   * @param autoEmbeddings Auto-generated embeddings map
   * @param featureFlags Feature flags for this index
   * @return A new VectorEmbeddedDocumentBuilder instance for the root
   */
  public static VectorEmbeddedDocumentBuilder createRoot(
      DocumentBuilder wrappedDocumentBuilder,
      VectorIndexFieldMapping mapping,
      byte[] id,
      IndexingMetricsUpdater indexingMetricsUpdater,
      IndexCapabilities indexCapabilities,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings,
      FeatureFlags featureFlags) {

    // Create a root block for this document
    DocumentBlock rootBlock = RootBlock.create(wrappedDocumentBuilder);

    return new VectorEmbeddedDocumentBuilder(
        wrappedDocumentBuilder,
        rootBlock,
        null, // No specific embedded definition at root level
        mapping,
        id,
        Optional.empty(),
        indexingMetricsUpdater,
        indexCapabilities,
        autoEmbeddings,
        featureFlags);
  }

  /**
   * Create a {@link VectorEmbeddedDocumentBuilder} for an embedded Lucene document within a vector
   * index. This builder creates child documents for array elements containing vectors.
   *
   * @param parentBlock The parent document block
   * @param embeddedDefinition The embedded documents field definition
   * @param mapping The vector index field mapping
   * @param id The document ID (shared across all documents in the block)
   * @param path The field path to the embedded document
   * @param indexingMetricsUpdater Metrics updater for indexing operations
   * @param indexCapabilities Index capabilities
   * @param autoEmbeddings Auto-generated embeddings map
   * @param featureFlags Feature flags for this index
   * @return A new VectorEmbeddedDocumentBuilder instance
   */
  public static VectorEmbeddedDocumentBuilder create(
      DocumentBlock parentBlock,
      VectorEmbeddedDocumentsFieldDefinition embeddedDefinition,
      VectorIndexFieldMapping mapping,
      byte[] id,
      FieldPath path,
      IndexingMetricsUpdater indexingMetricsUpdater,
      IndexCapabilities indexCapabilities,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings,
      FeatureFlags featureFlags) {

    // Create a new child document block for this embedded document
    VectorIndexDocumentWrapper documentWrapper =
        VectorIndexDocumentWrapper.createEmbedded(
            id, path, indexCapabilities, indexingMetricsUpdater, featureFlags);

    DocumentBlock childBlock = parentBlock.newChild(documentWrapper, () -> Optional.empty());

    LuceneVectorIndexDocumentBuilder documentBuilder =
        new LuceneVectorIndexDocumentBuilder(
            documentWrapper,
            mapping,
            Optional.of(path),
            IndexingPolicyBuilderContext.builder().autoEmbeddings(autoEmbeddings).build());

    return new VectorEmbeddedDocumentBuilder(
        documentBuilder,
        childBlock,
        embeddedDefinition,
        mapping,
        id,
        Optional.of(path),
        indexingMetricsUpdater,
        indexCapabilities,
        autoEmbeddings,
        featureFlags);
  }

  /**
   * Create a {@link FieldValueHandler} for a value at a particular leaf path.
   *
   * <p>This method delegates to {@link VectorEmbeddedDocumentHandler#valueHandler} to create
   * handlers that can process both regular fields and nested embedded documents.
   */
  @Override
  public Optional<FieldValueHandler> valueHandler(String leafPath) {
    return VectorEmbeddedDocumentHandler.valueHandler(
        this.wrappedDocumentBuilder,
        this.documentBlock,
        this.fieldDefinition,
        this.mapping,
        this.indexingMetricsUpdater,
        this.indexCapabilities,
        this.path.map(path -> path.newChild(leafPath)).orElseGet(() -> FieldPath.newRoot(leafPath)),
        this.id,
        this.autoEmbeddings,
        this.featureFlags);
  }

  @Override
  public List<Document> buildBlock() throws IOException {
    return this.documentBlock.build();
  }
}
