package com.xgen.mongot.embedding;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping.AutoEmbedField;
import com.xgen.mongot.index.definition.DynamicDefinition;
import com.xgen.mongot.index.definition.VectorAutoEmbedFieldSpecification;
import com.xgen.mongot.util.FieldPath;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Auto-embed field mapping for search indexes.
 *
 * <p>Search indexes carry a {@link DynamicDefinition} on their top-level mapping. Whether a source
 * field should be copied to the materialized view depends on that mode:
 *
 * <ul>
 *   <li><b>Dynamic mode</b> ({@code dynamic} enabled): the derived search index on the MV
 *       collection performs dynamic mapping too, so every non-embed source field must be copied.
 *       {@link #isPassthrough} treats anything outside {@link #embedFields} as passthrough (plus
 *       embed sources that are also explicitly declared), and {@link
 *       #passthroughDescendPredicate()} descends into every subtree.
 *   <li><b>Static mode</b> ({@code dynamic} disabled): only explicitly declared non-embed fields
 *       and the ancestor paths needed to reach them are passthrough. Document ancestors are kept as
 *       scalar fallbacks for ancestor-overwrite updates, but subdocument traversal copies only
 *       explicitly declared children.
 * </ul>
 *
 * <p>Embed-field keys are the auto-embed source fields (as declared by {@code
 * SearchAutoEmbedFieldDefinition.path()}), so they align with the paths seen in a source document
 * during ingestion.
 */
public record SearchIndexAutoEmbedFieldMapping(
    DynamicDefinition dynamic,
    ImmutableMap<FieldPath, AutoEmbedField.EmbedField> embedFields,
    ImmutableSet<FieldPath> explicitPassthroughPaths,
    ImmutableSet<FieldPath> explicitPassthroughAncestorPaths,
    ImmutableSet<FieldPath> embedAncestorPaths,
    boolean enumeratesAllIndexedPaths)
    implements AutoEmbedFieldMapping {

  /**
   * Convenience constructor that precomputes ancestor-path sets used in static mode for descent
   * decisions in the materialized-view writer.
   */
  public SearchIndexAutoEmbedFieldMapping(
      DynamicDefinition dynamic,
      ImmutableMap<FieldPath, AutoEmbedField.EmbedField> embedFields,
      ImmutableSet<FieldPath> explicitPassthroughPaths) {
    this(dynamic, embedFields, explicitPassthroughPaths, !dynamic.isEnabled());
  }

  /**
   * Creates a mapping with an explicit completeness classification. A static root can still be
   * non-enumerable when a nested document or embeddedDocuments mapping enables dynamic indexing.
   */
  public SearchIndexAutoEmbedFieldMapping(
      DynamicDefinition dynamic,
      ImmutableMap<FieldPath, AutoEmbedField.EmbedField> embedFields,
      ImmutableSet<FieldPath> explicitPassthroughPaths,
      boolean enumeratesAllIndexedPaths) {
    this(
        dynamic,
        embedFields,
        explicitPassthroughPaths,
        computeAncestorPaths(explicitPassthroughPaths),
        computeAncestorPaths(embedFields.keySet()),
        enumeratesAllIndexedPaths);
  }

  @Override
  public boolean childPathExists(FieldPath path) {
    if (this.dynamic.isEnabled()) {
      return true;
    }
    return this.embedFields.containsKey(path)
        || this.explicitPassthroughPaths.contains(path)
        || subDocumentExists(path);
  }

  @Override
  public boolean subDocumentExists(FieldPath path) {
    if (this.dynamic.isEnabled()) {
      return true;
    }
    return this.explicitPassthroughAncestorPaths.contains(path)
        || this.embedAncestorPaths.contains(path);
  }

  @Override
  public Optional<AutoEmbedField> getField(FieldPath path) {
    AutoEmbedField.EmbedField embed = this.embedFields.get(path);
    if (embed != null) {
      return Optional.of(embed);
    }
    if (this.explicitPassthroughPaths.contains(path)) {
      return Optional.of(new AutoEmbedField.PassthroughField(path));
    }
    return Optional.empty();
  }

  @Override
  public boolean isEmbed(FieldPath path) {
    return this.embedFields.containsKey(path);
  }

  @Override
  public boolean isPassthrough(FieldPath path) {
    if (this.explicitPassthroughPaths.contains(path)) {
      // Dual role: an embed source that is also explicitly declared as a non-embed field is copied
      // to the MV so the declared mapping is populated, alongside the generated vector, e.g.
      // "description": {"type": "string"} and "desc_embedding": {"type": "autoEmbed", "path":
      // "description", "model": "voyage-4-lite", "modality": "text"}. Explicit declarations also
      // win under an embed source, so mixed-shape sources (a field that is a string in one
      // document and an object with declared children in another) keep their lexical mappings
      // populated.
      return true;
    }
    if (hasEmbedAncestor(path) || isEmbed(path)) {
      // Without an explicit declaration, an embed source and its subtree stay consumed by the
      // embedding pipeline; the generated vector lives at the materialized-view path.
      return false;
    }
    if (this.dynamic.isEnabled()) {
      return true;
    }
    // Ancestor paths of declared passthrough fields are retained as scalar fallbacks: if a
    // document-valued mapping is overwritten with a scalar, the raw scalar must still reach the MV
    // (the ancestor-overwrite fallback path). Subdocument traversal continues to copy only
    // explicitly declared children.
    return matchesPassthrough(path);
  }

  @Override
  public boolean hasSourcePassthroughFields() {
    return this.dynamic.isEnabled() || !this.explicitPassthroughPaths.isEmpty();
  }

  /**
   * Returns embed entries plus a synthetic {@link AutoEmbedField.PassthroughField} for each
   * explicit passthrough path. Keeping the map symmetric with the vector mapping so callers like
   * {@code AutoEmbeddingDocumentUtils} that iterate {@code fieldMap()} for stale-MV detection see
   * both embed and passthrough declarations.
   *
   * <p>When an explicit passthrough path overlaps an embed source field (e.g. a regular text field
   * named "content" alongside an auto-embed field whose {@code path} is also "content"), the embed
   * entry wins in this map. The passthrough role is still reported by {@link
   * #isPassthrough(FieldPath)} so the raw value is copied to the MV alongside the generated vector.
   *
   * <p>Dynamic-mode passthroughs are open-ended (anything not embed is passthrough) and cannot be
   * enumerated here, so dynamic mode returns embed-only. See TODO(CLOUDP-384026).
   */
  @Override
  public ImmutableMap<FieldPath, AutoEmbedField> fieldMap() {
    ImmutableMap.Builder<FieldPath, AutoEmbedField> builder = ImmutableMap.builder();
    this.embedFields.forEach(builder::put);
    this.explicitPassthroughPaths.stream()
        .filter(path -> !this.embedFields.containsKey(path))
        .forEach(path -> builder.put(path, new AutoEmbedField.PassthroughField(path)));
    return builder.build();
  }

  @Override
  public ImmutableSet<FieldPath> passthroughAncestorPaths() {
    return this.explicitPassthroughAncestorPaths;
  }

  @Override
  public Predicate<FieldPath> passthroughDescendPredicate() {
    if (this.dynamic.isEnabled()) {
      // Dynamic roots copy every non-embed subtree, but embed-source subtrees stay consumed
      // except where explicit declarations below the source must be reached (for example a
      // static document hierarchy declaring children under the source path). The source itself is
      // skipped so an object value there leaves no empty container in the MV.
      return path ->
          (!hasEmbedAncestor(path) && !isEmbed(path))
              || this.explicitPassthroughAncestorPaths.contains(path);
    }
    return this::matchesPassthrough;
  }

  /**
   * Matches exact passthrough paths, ancestors of passthrough paths for scalar fallback, and
   * descendants of explicit passthrough paths.
   */
  private boolean matchesPassthrough(FieldPath path) {
    return this.explicitPassthroughPaths.contains(path)
        || this.explicitPassthroughAncestorPaths.contains(path)
        || hasPassthroughAncestor(path);
  }

  /**
   * True when the two mappings produce identical materialized-view content for every source
   * document. Embed fields are compared on embedding-affecting properties only. Similarity,
   * indexing method, and HNSW options affect the derived Lucene index, not the vectors stored in
   * the materialized view, so changing them must not force re-embedding. Quantization remains part
   * of the comparison because it can affect the embedding generated by the provider. In dynamic
   * mode every non-embed path is passthrough, so the enumerated explicit passthrough paths are
   * irrelevant to MV content — except for paths declared at or under an embed source, whose raw
   * value is copied; those are compared in every mode.
   */
  public boolean producesSameMatViewContent(SearchIndexAutoEmbedFieldMapping other) {
    if (this.dynamic.isEnabled() != other.dynamic.isEnabled()) {
      return false;
    }
    if (!this.dynamic.isEnabled()
        && !this.explicitPassthroughPaths.equals(other.explicitPassthroughPaths)) {
      return false;
    }
    if (!declaredAtOrUnderEmbedSourcePaths().equals(other.declaredAtOrUnderEmbedSourcePaths())) {
      return false;
    }
    if (!this.embedFields.keySet().equals(other.embedFields.keySet())) {
      return false;
    }
    for (Map.Entry<FieldPath, AutoEmbedField.EmbedField> entry : this.embedFields.entrySet()) {
      if (!sameMatViewEmbedding(entry.getValue(), other.embedFields.get(entry.getKey()))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Explicitly declared paths whose raw value must reach the MV because they are an embed source
   * (dual role) or lie under one. In dynamic mode every other path is copied regardless of
   * declaration, so this is the only declaration difference that changes MV content (static mode
   * compares the full explicit set above).
   */
  private ImmutableSet<FieldPath> declaredAtOrUnderEmbedSourcePaths() {
    return this.explicitPassthroughPaths.stream()
        .filter(path -> isEmbed(path) || hasEmbedAncestor(path))
        .collect(ImmutableSet.toImmutableSet());
  }

  private static boolean sameMatViewEmbedding(
      AutoEmbedField.EmbedField left, AutoEmbedField.EmbedField right) {
    if (!(left.specification() instanceof VectorAutoEmbedFieldSpecification leftSpec)
        || !(right.specification() instanceof VectorAutoEmbedFieldSpecification rightSpec)) {
      return left.specification().equals(right.specification());
    }
    return leftSpec.materializedViewIdentity().equals(rightSpec.materializedViewIdentity());
  }

  private boolean hasPassthroughAncestor(FieldPath path) {
    return path.ancestorPaths().anyMatch(this.explicitPassthroughPaths::contains);
  }

  private boolean hasEmbedAncestor(FieldPath path) {
    return path.ancestorPaths().anyMatch(this.embedFields::containsKey);
  }

  private static ImmutableSet<FieldPath> computeAncestorPaths(Set<FieldPath> paths) {
    return paths.stream().flatMap(FieldPath::ancestorPaths).collect(ImmutableSet.toImmutableSet());
  }
}
