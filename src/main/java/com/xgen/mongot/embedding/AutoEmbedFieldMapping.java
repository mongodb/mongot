package com.xgen.mongot.embedding;

import com.xgen.mongot.index.definition.VectorFieldAutoEmbeddingSpecification;
import com.xgen.mongot.util.FieldPath;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
 * </ul>
 */
public sealed interface AutoEmbedFieldMapping permits VectorAutoEmbedFieldMapping {

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

  /** Returns only the embed fields with their specifications. Used by spec-level consumers. */
  Map<FieldPath, AutoEmbedField.EmbedField> embedFields();

  /** Returns the precomputed ancestor paths for passthrough fields only. */
  Set<FieldPath> passthroughAncestorPaths();
}
