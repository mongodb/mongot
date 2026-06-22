package com.xgen.mongot.embedding;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.xgen.mongot.embedding.AutoEmbedFieldMapping.AutoEmbedField;
import com.xgen.mongot.index.definition.DynamicDefinition;
import com.xgen.mongot.util.FieldPath;
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
 *       {@link #isPassthrough} treats anything outside {@link #embedFields} as passthrough, and
 *       {@link #passthroughDescendPredicate()} descends into every subtree.
 *   <li><b>Static mode</b> ({@code dynamic} disabled): only explicitly declared non-embed fields
 *       are passthrough. Ancestor paths are precomputed like vector indexes.
 * </ul>
 *
 * <p>Embed-field keys are the auto-embed source fields (as declared by {@code
 * SearchAutoEmbedFieldDefinition.sourceField()}), so they align with the paths seen in a source
 * document during ingestion.
 */
public record SearchIndexAutoEmbedFieldMapping(
    DynamicDefinition dynamic,
    ImmutableMap<FieldPath, AutoEmbedField.EmbedField> embedFields,
    ImmutableSet<FieldPath> explicitPassthroughPaths,
    ImmutableSet<FieldPath> explicitPassthroughAncestorPaths,
    ImmutableSet<FieldPath> embedAncestorPaths)
    implements AutoEmbedFieldMapping {

  /**
   * Convenience constructor that precomputes ancestor-path sets used in static mode for descent
   * decisions in the materialized-view writer.
   */
  public SearchIndexAutoEmbedFieldMapping(
      DynamicDefinition dynamic,
      ImmutableMap<FieldPath, AutoEmbedField.EmbedField> embedFields,
      ImmutableSet<FieldPath> explicitPassthroughPaths) {
    this(
        dynamic,
        embedFields,
        explicitPassthroughPaths,
        computeAncestorPaths(explicitPassthroughPaths),
        computeAncestorPaths(embedFields.keySet()));
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
    if (isEmbed(path) || hasEmbedAncestor(path)) {
      return false;
    }
    if (this.dynamic.isEnabled()) {
      return true;
    }
    return this.explicitPassthroughPaths.contains(path) || hasPassthroughAncestor(path);
  }

  /**
   * Returns embed entries plus a synthetic {@link AutoEmbedField.PassthroughField} for each
   * explicit passthrough path. Keeping the map symmetric with the vector mapping so callers like
   * {@code AutoEmbeddingDocumentUtils} that iterate {@code fieldMap()} for stale-MV detection see
   * both embed and passthrough declarations.
   *
   * <p>When an explicit passthrough path overlaps an embed source field (e.g. a regular text field
   * named "content" alongside an auto-embed field whose {@code sourceField} is also "content"), the
   * embed entry wins — the path is owned by the embed pipeline.
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
      return path -> !hasEmbedAncestor(path);
    }
    return path ->
        this.explicitPassthroughAncestorPaths.contains(path)
            || this.explicitPassthroughPaths.contains(path)
            || hasPassthroughAncestor(path);
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
