package com.xgen.mongot.index.lucene.document.single;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.EmbeddedDocumentsFieldDefinition;
import com.xgen.mongot.index.definition.HierarchicalFieldDefinition;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.index.lucene.document.builder.DocumentBuilder;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;

/**
 * {@link LuceneSearchIndexDocumentBuilder} is a {@link DocumentHandler} responsible for creating
 * {@link FieldValueHandler}s for fields at the root level of an indexed document.
 *
 * <h2>Root Non-Embedded Documents</h2>
 *
 * <p>When building the root, "non-embedded" Lucene document, {@link
 * LuceneSearchIndexDocumentBuilder} is configured with:
 *
 * <ul>
 *   <li>The {@link DocumentFieldDefinition} (e.g. mappings) for a particular index.
 *   <li>A {@link DocumentWrapper} that contains a Lucene document:
 *       <ul>
 *         <li>With a {@code $meta/_id} field. All documents contain a field with the encoded _id of
 *             the MongoDB source document they originated from.
 *         <li>Without a {@code $meta/embeddedRoot} field. This document is not the root document of
 *             an embedded document block.
 *         <li>Without a {@code $meta/embeddedPath} field. This document is not a child document
 *             that is part of an embedded document block.
 *       </ul>
 * </ul>
 *
 * <h2>Root Embedded Documents</h2>
 *
 * <p>When building the root, embedded Lucene document, {@link LuceneSearchIndexDocumentBuilder} is
 * configured with:
 *
 * <ul>
 *   <li>The {@link DocumentFieldDefinition} (e.g. mappings) for a particular index.
 *   <li>A {@link DocumentWrapper} that contains a Lucene document:
 *       <ul>
 *         <li>With a {@code $meta/_id} field. All documents contain a field with the encoded _id of
 *             the MongoDB source document they originated from.
 *         <li>With a {@code $meta/embeddedRoot} field. This document is the root document of an
 *             embedded document block.
 *         <li>Without a {@code $meta/embeddedPath} field. This document is not a child document
 *             that is part of an embedded document block.
 *       </ul>
 * </ul>
 *
 * <h2>Non-Root Embedded Documents</h2>
 *
 * <p>When building an embedded Lucene document, {@link LuceneSearchIndexDocumentBuilder} is
 * configured with:
 *
 * <ul>
 *   <li>The EmbeddedDocumentsFieldDefinition configuring the root of that embedded document.
 *   <li>A {@link DocumentWrapper} that contains a Lucene document:
 *       <ul>
 *         <li>With a {@code $meta/_id} field. All documents contain a field with the encoded _id of
 *             the MongoDB source document they originated from.
 *         <li>Without a {@code $meta/embeddedRoot} field. This document is not the root document of
 *             an embedded document block.
 *         <li>With a {@code $meta/embeddedPath} field. This document is a child document that is
 *             part of an embedded document block.
 *       </ul>
 * </ul>
 *
 * <h2>Creating {@link FieldValueHandler}s.</h2>
 *
 * <p>After instantiation, {@link LuceneSearchIndexDocumentBuilder} is ignorant of its
 * "embedded"-ness. It may create a {@link LuceneSearchIndexFieldValueHandler}s for a field, if that
 * field is configured to be indexed statically or dynamically.
 */
public class LuceneSearchIndexDocumentBuilder implements DocumentBuilder {
  final DocumentWrapper documentWrapper;
  final HierarchicalFieldDefinition fieldDefinition;
  final Optional<FieldPath> path;

  private LuceneSearchIndexDocumentBuilder(
      DocumentWrapper documentWrapper,
      HierarchicalFieldDefinition fieldDefinition,
      Optional<FieldPath> path) {
    this.documentWrapper = documentWrapper;
    this.fieldDefinition = fieldDefinition;
    this.path = path;
  }

  /**
   * Create a {@link LuceneSearchIndexDocumentBuilder} for a standalone Lucene document to be
   * indexed. Configures the to-be-built Lucene document with the correct _id field. Does not
   * populate embedded-specific fields to mark that document as the "root" of an embedded document
   * block.
   */
  public static LuceneSearchIndexDocumentBuilder createRoot(
      byte[] id,
      DocumentFieldDefinition mappings,
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater,
      FeatureFlags featureFlags) {
    return new LuceneSearchIndexDocumentBuilder(
        DocumentWrapper.createRootStandalone(
            id, indexAnalyzer, resolver, indexingMetricsUpdater, featureFlags),
        mappings,
        Optional.empty());
  }

  /**
   * Create a {@link LuceneSearchIndexDocumentBuilder} for a Lucene document that will be the root
   * of an embedded document block. Configures the to-be-built Lucene document with the correct _id
   * field, as well as with embedded-specific fields to mark that document as the "root" of an
   * embedded document block.
   */
  public static LuceneSearchIndexDocumentBuilder createEmbeddedRoot(
      byte[] id,
      DocumentFieldDefinition fieldDefinition,
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater,
      FeatureFlags featureFlags) {
    return new LuceneSearchIndexDocumentBuilder(
        DocumentWrapper.createEmbeddedRoot(
            id, indexAnalyzer, resolver, indexingMetricsUpdater, featureFlags),
        fieldDefinition,
        Optional.empty());
  }

  /**
   * Create a {@link LuceneSearchIndexDocumentBuilder} for an embedded Lucene document that is not
   * the root document of its embedded document block. Configures the to-be-built Lucene document
   * with the correct _id field, as well as with embedded-specific fields to mark what path this
   * embedded document considers as the "embedded root path" of its document.
   */
  public static LuceneSearchIndexDocumentBuilder createEmbedded(
      byte[] id,
      FieldPath path,
      EmbeddedDocumentsFieldDefinition fieldDefinition,
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater) {
    return new LuceneSearchIndexDocumentBuilder(
        // explicitly set isNumberAndDateSortable for embeddedDocs to false as we don't support sort
        DocumentWrapper.createEmbedded(id, path, indexAnalyzer, resolver, indexingMetricsUpdater),
        fieldDefinition,
        Optional.of(path));
  }

  @Override
  public Optional<FieldValueHandler> valueHandler(String leafPath) {
    return this.fieldDefinition
        .getField(leafPath)
        .map(
            field ->
                LuceneSearchIndexFieldValueHandler.create(
                    this.documentWrapper, field, childPath(leafPath), false))
        .or(
            () ->
                this.documentWrapper
                    .getResolver()
                    .getDynamicFieldDefinition(this.fieldDefinition)
                    .map(
                        dynamicFieldDef ->
                            LuceneSearchIndexFieldValueHandler.create(
                                this.documentWrapper,
                                dynamicFieldDef,
                                childPath(leafPath),
                                false)));
  }

  private FieldPath childPath(String leafPath) {
    return this.path
        .map(path -> path.newChild(leafPath))
        .orElseGet(() -> FieldPath.newRoot(leafPath));
  }

  public Document backingDocument() {
    return this.documentWrapper.luceneDocument;
  }

  @Override
  public Document build() {
    return this.documentWrapper.luceneDocument;
  }
}
