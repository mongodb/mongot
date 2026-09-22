package com.xgen.mongot.embedding;

import com.xgen.mongot.index.definition.VectorFieldAutoEmbeddingSpecification;
import com.xgen.mongot.util.FieldPath;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Index-neutral field mapping for auto-embedding document processing.
 *
 * <p>Classifies indexed field paths into two roles:
 *
 * <ul>
 *   <li>{@link AutoEmbedField.EmbedField} — field needs embedding generation
 *   <li>{@link AutoEmbedField.PassthroughField} — field should be copied as-is to the materialized
 *       view
 * </ul>
 *
 * <p>Provides tree-traversal predicates ({@link #childPathExists}, {@link #subDocumentExists}) for
 * efficient BSON document processing, and convenience accessors for embed-only fields used by
 * spec-level consumers (hash computation, byte estimation).
 *
 * <p>Implementations handle index-type-specific field models:
 *
 * <ul>
 *   <li>{@link VectorAutoEmbedFieldMapping} — vector indexes with a fixed set of fields
 *   <li>{@link SearchIndexAutoEmbedFieldMapping} — search indexes, with dynamic-mapping support
 * </ul>
 */
public sealed interface AutoEmbedFieldMapping
    permits VectorAutoEmbedFieldMapping, SearchIndexAutoEmbedFieldMapping {

  /** A field in the auto-embed mapping, classified by its role. */
  sealed interface AutoEmbedField {
    FieldPath path();

    /** A field that needs embedding generation (AUTO_EMBED or legacy TEXT). */
    record EmbedField(FieldPath path, VectorFieldAutoEmbeddingSpecification specification)
        implements AutoEmbedField {}

    /** A field that should be copied as-is to the materialized view (e.g., FILTER). */
    record PassthroughField(FieldPath path) implements AutoEmbedField {}
  }

  /** Returns true if {@code path} is a field in the mapping or an ancestor of one. */
  boolean childPathExists(FieldPath path);

  /** Returns true if {@code path} is an ancestor of at least one field in the mapping. */
  boolean subDocumentExists(FieldPath path);

  Optional<AutoEmbedField> getField(FieldPath path);

  boolean isEmbed(FieldPath path);

  boolean isPassthrough(FieldPath path);

  /** Returns the declared field map. */
  Map<FieldPath, AutoEmbedField> fieldMap();

  /**
   * Returns whether {@link #fieldMap()} enumerates every indexed source path. Partial updates rely
   * on this property to detect destructive changes that could otherwise leave stale MV values.
   *
   * <p>For example, this static Search definition is enumerable: the source-side mapping contains
   * the declared {@code title} path and the auto-embed source path {@code body}.
   *
   * <pre>{@code
   * {
   *   "mappings": {
   *     "dynamic": false,
   *     "fields": {
   *       "title": { "type": "string" },
   *       "embedding": {
   *         "type": "autoEmbed",
   *         "model": "voyage-4-lite",
   *         "path": "body"
   *       }
   *     }
   *   }
   * }
   * }</pre>
   *
   * <p>It is not enumerable if the root or any nested document enables dynamic indexing. For
   * example, with {@code "metadata": {"type": "document", "dynamic": true}}, an undeclared path
   * such as {@code metadata.color} may be indexed even though it is absent from {@link
   * #fieldMap()}. Vector index mappings are always enumerable because all indexed paths are
   * explicitly declared.
   */
  boolean enumeratesAllIndexedPaths();

  /** Returns only the embed fields with their specifications. Used by spec-level consumers. */
  Map<FieldPath, AutoEmbedField.EmbedField> embedFields();

  /** Returns the precomputed ancestor paths for passthrough fields only. */
  Set<FieldPath> passthroughAncestorPaths();

  /**
   * Returns the predicate to test whether a path should be descended into when collecting
   * passthrough field values.
   */
  Predicate<FieldPath> passthroughDescendPredicate();
}
