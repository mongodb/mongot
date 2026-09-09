package com.xgen.mongot.replication.mongodb.common;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.FluentLogger;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.index.definition.ViewDefinition.SupportedStage;
import com.xgen.mongot.util.FieldPath;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;

/**
 * Base-collection field dependencies statically derived from a view's effective pipeline.
 *
 * <p>For an index built on a view, a change-stream {@code updateDescription} carries
 * base-collection field names while the indexed document is the view-resolved output. This class
 * derives the set of base fields whose change forces full (re-embed + upsert) processing: fields
 * referenced by any {@code $match.$expr} (a change may move the document into the view) and, per
 * auto-embed path, the base fields its value derives from, resolved transitively through earlier
 * {@code $set}/{@code $addFields} stages (a change may alter the embedded text).
 *
 * <p>Extraction is conservative: any shape it cannot interpret (bare {@code $$ROOT}/{@code
 * $$CURRENT}, unsupported stages, malformed expressions) yields an instance that forces every
 * update to take the full re-embed + upsert path. Over-collection costs an extra embedding call;
 * under-collection would silently serve stale results, so the bias is always toward
 * over-collection.
 */
public final class ViewFieldDependencies {

  private static final FluentLogger LOG = FluentLogger.forEnclosingClass();

  private static final String LITERAL_OPERATOR = "$literal";

  /** Operators that carry field names as plain strings the collector cannot interpret. */
  private static final Set<String> FIELD_NAME_OPERATORS =
      Set.of("$getField", "$setField", "$unsetField");

  private static final String EXPRESSION_PREFIX = "$";
  private static final String VARIABLE_PREFIX = "$$";
  private static final String CURRENT_FIELD_PREFIX = "$$CURRENT.";
  private static final String ROOT_FIELD_PREFIX = "$$ROOT.";
  private static final Set<String> WHOLE_DOCUMENT_VARIABLES = Set.of("$$CURRENT", "$$ROOT");

  /**
   * Base fields whose change forces full processing: match-referenced ∪ computed-filter feeders.
   */
  private final ImmutableSet<FieldPath> nonEmbedTriggers;

  /** Per embed source, the base fields its (possibly computed) value depends on. */
  private final ImmutableMap<FieldPath, ImmutableSet<FieldPath>> embedFeeders;

  /**
   * Containers of exact nested computed embed paths. An update that replaces one of these
   * containers (or an ancestor) must use a full write because a dotted $set cannot clear a missing
   * embed value from the materialized view.
   *
   * <p>For example, if a view computes {@code items.text} with {@code $set: {"items.text":
   * "$seed"}}, replacing {@code items} with {@code []} removes {@code items.text} from the view. A
   * full write then removes its obsolete embedding and hash.
   */
  private final ImmutableSet<FieldPath> nestedComputedEmbedContainers;

  /** Union of nonEmbedTriggers and all embed feeders; any change here needs embedding/full work. */
  private final ImmutableSet<FieldPath> triggerFields;

  /**
   * Present when extraction could not bound the dependency set (carrying why): every update is
   * forced to the full re-embed + upsert path regardless of which base fields it touched.
   */
  private final Optional<ViewDerivationFallbackReason> derivationFallbackReason;

  private ViewFieldDependencies(
      ImmutableSet<FieldPath> nonEmbedTriggers,
      ImmutableMap<FieldPath, ImmutableSet<FieldPath>> embedFeeders,
      ImmutableSet<FieldPath> nestedComputedEmbedContainers,
      Optional<ViewDerivationFallbackReason> derivationFallbackReason) {
    this.nonEmbedTriggers = nonEmbedTriggers;
    this.embedFeeders = embedFeeders;
    this.nestedComputedEmbedContainers = nestedComputedEmbedContainers;
    this.triggerFields =
        ImmutableSet.<FieldPath>builder()
            .addAll(nonEmbedTriggers)
            .addAll(embedFeeders.values().stream().flatMap(Set::stream).iterator())
            .build();
    this.derivationFallbackReason = derivationFallbackReason;
  }

  /** A dependency set that couldn't be bounded, tagged with why, forcing all updates full. */
  private static ViewFieldDependencies fullProcessing(ViewDerivationFallbackReason reason) {
    return new ViewFieldDependencies(
        ImmutableSet.of(), ImmutableMap.of(), ImmutableSet.of(), Optional.of(reason));
  }

  /** A dependency set for an index shape whose fields cannot be statically enumerated. */
  static ViewFieldDependencies fullProcessingForUnsupportedShape() {
    return fullProcessing(ViewDerivationFallbackReason.UNSUPPORTED_SHAPE);
  }

  /**
   * Convenience overload for indexes with no declared filter fields. Match-field and embed-feeder
   * triggers are still extracted from the pipeline; only the computed-filter-feeder pass is skipped
   * (it has nothing to operate on).
   */
  public static ViewFieldDependencies derive(
      List<BsonDocument> effectivePipeline, Set<FieldPath> embedPaths) {
    return derive(effectivePipeline, embedPaths, Set.of());
  }

  /**
   * Derives dependencies from the flattened effective pipeline, the index's embed source paths, and
   * its declared filter-field paths. Never throws: any extraction failure yields an instance that
   * depends on everything.
   */
  public static ViewFieldDependencies derive(
      List<BsonDocument> effectivePipeline, Set<FieldPath> embedPaths, Set<FieldPath> filterPaths) {
    try {
      return doDerive(effectivePipeline, embedPaths, filterPaths);
    } catch (RuntimeException e) {
      // Exception-driven fallback; counted distinctly from unanalyzable-shape fallbacks.
      LOG.atWarning().withCause(e).log(
          "Failed to derive view field dependencies; falling back to full processing");
      return fullProcessing(ViewDerivationFallbackReason.EXCEPTION);
    }
  }

  private static ViewFieldDependencies doDerive(
      List<BsonDocument> effectivePipeline, Set<FieldPath> embedPaths, Set<FieldPath> filterPaths) {

    // Fields defined by $set/$addFields so far, mapped to their resolved base-field feeders.
    Map<FieldPath, ImmutableSet<FieldPath>> computed = new HashMap<>();
    Set<FieldPath> triggers = new HashSet<>();

    for (BsonDocument stage : effectivePipeline) {
      if (stage.keySet().size() != 1) {
        return fullProcessing(ViewDerivationFallbackReason.UNSUPPORTED_SHAPE);
      }
      String stageName = stage.keySet().iterator().next();
      Optional<SupportedStage> stageType = SupportedStage.byStageName(stageName);
      if (stageType.isEmpty() || !stage.get(stageName).isDocument()) {
        return fullProcessing(ViewDerivationFallbackReason.UNSUPPORTED_SHAPE);
      }
      BsonDocument stageBody = stage.getDocument(stageName);

      switch (stageType.get()) {
        case ADD_FIELDS, SET -> {
          // All fields of one stage evaluate against the stage INPUT document, so resolve them
          // against a snapshot and merge afterwards.
          Map<FieldPath, ImmutableSet<FieldPath>> stageInput = Map.copyOf(computed);
          Map<FieldPath, ImmutableSet<FieldPath>> stageOutputs = new HashMap<>();
          for (var entry : stageBody.entrySet()) {
            RefCollector collector = new RefCollector();
            collector.collect(entry.getValue());
            if (collector.wholeDocument) {
              return fullProcessing(ViewDerivationFallbackReason.UNSUPPORTED_SHAPE);
            }
            stageOutputs.put(
                FieldPath.parse(entry.getKey()), resolveRefs(collector.refs, stageInput));
          }
          // A (re)defined path wholesale replaces descendants computed by EARLIER stages, but a
          // descendant defined in THIS same stage must be kept (its feeders refine the parent).
          // Compute removals against prior state only, then apply all outputs together — so the
          // result does not depend on stageOutputs iteration order.
          computed
              .keySet()
              .removeIf(
                  key ->
                      !stageOutputs.containsKey(key)
                          && stageOutputs.keySet().stream().anyMatch(key::isChildOf));
          computed.putAll(stageOutputs);
        }
        case MATCH -> {
          RefCollector collector = new RefCollector();
          collector.collect(stageBody);
          if (collector.wholeDocument) {
            return fullProcessing(ViewDerivationFallbackReason.UNSUPPORTED_SHAPE);
          }
          triggers.addAll(resolveRefs(collector.refs, computed));
        }
          // No default: errorprone (MissingCasesInEnumSwitch) enforces at compile time that any
          // future SupportedStage value is handled here rather than silently ignored.
      }
    }

    // Per embed source, the base fields its (possibly computed) value depends on. Kept per-embed
    // (not folded into triggers) so a change can re-embed only the affected sources.
    ImmutableMap<FieldPath, ImmutableSet<FieldPath>> embedFeeders =
        embedPaths.stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    path -> path, path -> resolveRefs(Set.of(path), computed)));
    ImmutableSet<FieldPath> nestedComputedEmbedContainers =
        embedPaths.stream()
            .filter(computed::containsKey)
            .map(FieldPath::getParent)
            .flatMap(Optional::stream)
            .collect(ImmutableSet.toImmutableSet());

    // A filter field computed by the view, or containing a computed descendant, contributes those
    // computed paths' feeders: the filter-only partial update re-extracts present filter values but
    // cannot delete one that the view drops (e.g. $cond -> $$REMOVE), so a change to such a feeder
    // must take the full path. Add only the computed feeders, not a passthrough ancestor itself, so
    // unrelated updates under that ancestor remain partial-eligible.
    for (FieldPath path : filterPaths) {
      if (isComputed(path, computed)) {
        // Preserve shadowing between computed ancestors and the filter path itself.
        triggers.addAll(resolveRefs(Set.of(path), computed));
      } else {
        // A passthrough ancestor contributes only the feeders of its computed descendants.
        computed.entrySet().stream()
            .filter(entry -> entry.getKey().isChildOf(path))
            .forEach(entry -> triggers.addAll(entry.getValue()));
      }
    }

    // triggers now holds match-referenced ∪ computed-filter feeders — the non-embed triggers.
    return new ViewFieldDependencies(
        canonicalize(triggers), embedFeeders, nestedComputedEmbedContainers, Optional.empty());
  }

  /** True if the path (or an ancestor) is produced by a $set/$addFields stage. */
  private static boolean isComputed(
      FieldPath path, Map<FieldPath, ImmutableSet<FieldPath>> computed) {
    return computed.containsKey(path) || computed.keySet().stream().anyMatch(path::isChildOf);
  }

  /**
   * Resolves referenced paths against the fields computed by earlier stages, returning
   * base-collection paths. A reference covered by a computed field inherits its feeders; a
   * reference that is an ancestor of computed fields contributes both their feeders and itself (its
   * remaining subfields still come from the base document).
   */
  private static ImmutableSet<FieldPath> resolveRefs(
      Set<FieldPath> refs, Map<FieldPath, ImmutableSet<FieldPath>> computed) {
    Set<FieldPath> resolved = new HashSet<>();
    for (FieldPath ref : refs) {
      Optional<FieldPath> covering =
          computed.keySet().stream()
              .filter(key -> ref.equals(key) || ref.isChildOf(key))
              .max(Comparator.comparingInt(key -> key.getPathHierarchy().size()));
      if (covering.isPresent()) {
        resolved.addAll(computed.get(covering.get()));
      } else {
        resolved.add(ref);
      }
      // Computed subfields under the reference refine its value and feed it as well.
      computed.entrySet().stream()
          .filter(entry -> entry.getKey().isChildOf(ref))
          .forEach(entry -> resolved.addAll(entry.getValue()));
    }
    return canonicalize(resolved);
  }

  /** Drops paths that have a strict ancestor in the set; matching is ancestor-aware anyway. */
  private static ImmutableSet<FieldPath> canonicalize(Set<FieldPath> paths) {
    return paths.stream()
        .filter(path -> paths.stream().noneMatch(path::isChildOf))
        .collect(ImmutableSet.toImmutableSet());
  }

  /** Base fields whose change forces full processing. Exposed for testing. */
  ImmutableSet<FieldPath> getTriggerFields() {
    return this.triggerFields;
  }

  /** Per embed source, the base fields its value depends on. Exposed for testing. */
  ImmutableMap<FieldPath, ImmutableSet<FieldPath>> getEmbedFeeders() {
    return this.embedFeeders;
  }

  /** True if extraction could not bound the dependencies; every update takes the full path. */
  public boolean dependsOnEverything() {
    return this.derivationFallbackReason.isPresent();
  }

  /** Present (with why) when derivation fell back to full processing; empty when bounded. */
  public Optional<ViewDerivationFallbackReason> derivationFallbackReason() {
    return this.derivationFallbackReason;
  }

  /**
   * Returns true if the update may affect view membership or an embed source, i.e. the event must
   * take the embedding/full path rather than a pure filter-only partial update. Coarse gate used by
   * the batch-dedup pre-pass; {@link #classifyViewUpdate} makes the finer partial-embed decision.
   */
  public boolean requiresFullProcessing(@Nullable UpdateDescription updateDescription) {
    if (this.derivationFallbackReason.isPresent()
        || updateDescription == null
        || forcesFull(updateDescription)) {
      return true;
    }
    Set<FieldPath> changed = changedPaths(updateDescription);
    // An empty update can't prove the indexed document is unaffected — force full.
    return changed.isEmpty()
        || hitsAny(changed, this.triggerFields)
        || overwritesNestedComputedEmbedContainer(changed, this.nestedComputedEmbedContainers);
  }

  /**
   * Classifies an UPDATE on a view-based auto-embed index: whether it must take the full path, why,
   * and if not, which embed sources need re-embedding (empty ⇒ filter-only partial update).
   */
  public ViewUpdateClassification classifyViewUpdate(
      @Nullable UpdateDescription updateDescription) {
    if (this.derivationFallbackReason.isPresent()) {
      return ViewUpdateClassification.full(ViewFullProcessingReason.DEPENDS_ON_EVERYTHING);
    }
    if (updateDescription == null) {
      return ViewUpdateClassification.full(ViewFullProcessingReason.NO_CHANGED_PATHS);
    }
    if (forcesFull(updateDescription)) {
      // A removal or truncation can't be applied through the $set-only partial path.
      return ViewUpdateClassification.full(ViewFullProcessingReason.REMOVED_OR_TRUNCATED_FIELDS);
    }
    Set<FieldPath> changed = changedPaths(updateDescription);
    // Empty update, membership/computed-filter feeder changes, and nested computed-embed container
    // overwrites can't take a partial $set.
    if (changed.isEmpty()) {
      return ViewUpdateClassification.full(ViewFullProcessingReason.NO_CHANGED_PATHS);
    }
    if (overwritesNestedComputedEmbedContainer(changed, this.nestedComputedEmbedContainers)) {
      return ViewUpdateClassification.full(
          ViewFullProcessingReason.COMPUTED_EMBED_CONTAINER_OVERWRITE);
    }
    if (hitsAny(changed, this.nonEmbedTriggers)) {
      return ViewUpdateClassification.full(ViewFullProcessingReason.TRIGGER_HIT);
    }
    // Otherwise re-embed only the embed sources whose feeders were touched (may be none ⇒ filter-
    // only).
    ImmutableSet<FieldPath> changedEmbedPaths =
        this.embedFeeders.entrySet().stream()
            .filter(entry -> hitsAny(changed, entry.getValue()))
            .map(Map.Entry::getKey)
            .collect(ImmutableSet.toImmutableSet());
    return ViewUpdateClassification.partial(changedEmbedPaths);
  }

  /** True if a removal or array truncation can't be applied through the $set-only partial path. */
  private static boolean forcesFull(UpdateDescription updateDescription) {
    return !Optional.ofNullable(updateDescription.getRemovedFields())
            .orElseGet(Collections::emptyList)
            .isEmpty()
        || !Optional.ofNullable(updateDescription.getTruncatedArrays())
            .orElseGet(Collections::emptyList)
            .isEmpty();
  }

  /** Normalized base-collection paths a change updates. */
  private static Set<FieldPath> changedPaths(UpdateDescription updateDescription) {
    Set<String> updatedFields =
        Optional.ofNullable(updateDescription.getUpdatedFields())
            .map(BsonDocument::keySet)
            .orElseGet(Collections::emptySet);
    return updatedFields.stream()
        .map(FieldPath::parse)
        .map(ChangeStreams::filterFieldPath)
        .collect(Collectors.toSet());
  }

  /** True if any changed path is a direct relation of any field in {@code targets}. */
  private static boolean hitsAny(Set<FieldPath> changed, Set<FieldPath> targets) {
    return changed.stream().anyMatch(c -> targets.stream().anyMatch(c::isDirectRelation));
  }

  /**
   * True when an updated path is an exact container or an ancestor of it, but not a descendant.
   *
   * <p>For a computed {@code items.text} embed source, replacing {@code items} can make the
   * resolved embed text disappear. Updating {@code items.text} or {@code items.other} cannot: the
   * view overwrites the former and leaves the latter outside the embed source.
   */
  private static boolean overwritesNestedComputedEmbedContainer(
      Set<FieldPath> changed, Set<FieldPath> containers) {
    return changed.stream()
        .anyMatch(
            changedPath ->
                containers.stream()
                    .anyMatch(
                        container ->
                            container.equals(changedPath) || container.isChildOf(changedPath)));
  }

  /**
   * Result of {@link #classifyViewUpdate}: {@code requiresFull} true means take the full path and
   * {@code fullProcessingReason} is the route reason; otherwise {@code changedEmbedPaths} are the
   * embed sources to re-embed (empty ⇒ filter-only) and the reason is {@code NONE}.
   */
  public record ViewUpdateClassification(
      boolean requiresFull,
      ImmutableSet<FieldPath> changedEmbedPaths,
      ViewFullProcessingReason fullProcessingReason) {
    private static ViewUpdateClassification full(ViewFullProcessingReason reason) {
      return new ViewUpdateClassification(true, ImmutableSet.of(), reason);
    }

    private static ViewUpdateClassification partial(ImmutableSet<FieldPath> changedEmbedPaths) {
      return new ViewUpdateClassification(false, changedEmbedPaths, ViewFullProcessingReason.NONE);
    }
  }

  /** Why derivation fell back to full processing. Exported as the snake_case {@code reason} tag. */
  public enum ViewDerivationFallbackReason {
    /** Derivation threw and was caught. */
    EXCEPTION,
    /** The view pipeline shape could not be statically analyzed. */
    UNSUPPORTED_SHAPE
  }

  /**
   * Per-event selected view UPDATE write route. {@link #NONE} means a partial {@code $set} was
   * emitted; every other value means full processing. Exported as the {@code result} and snake_case
   * {@code reason} tags.
   */
  public enum ViewFullProcessingReason {
    /**
     * A dependency object exists, but derivation was unbounded, so every update must use full
     * processing. This takes precedence over event-content reasons. The companion {@code
     * viewFieldDependenciesDerivationFallback} metric records why derivation fell back once per
     * index, on its first batch.
     */
    DEPENDS_ON_EVERYTHING,

    /**
     * No dependency object was available for the view-based index, so neither field-level
     * classification nor a derivation-fallback reason was available.
     */
    VIEW_DEPENDENCIES_UNAVAILABLE,

    /**
     * The UPDATE event exposed no updated paths because its update description was absent or its
     * updated-fields document was empty. Removals and array truncations are classified before this
     * reason.
     */
    NO_CHANGED_PATHS,

    /**
     * A field removal or array truncation could not be represented by the {@code $set}-only path.
     */
    REMOVED_OR_TRUNCATED_FIELDS,

    /**
     * An update replaced a nested computed embed path's container or an ancestor of that container,
     * requiring full replacement to remove obsolete vectors and hashes. This more specific reason
     * takes precedence over {@link #TRIGGER_HIT} when both classifications apply.
     */
    COMPUTED_EMBED_CONTAINER_OVERWRITE,

    /**
     * A changed base path intersected a derived view dependency: a view-membership field, a
     * computed-filter feeder, or an embed feeder. In the write path, embed-feeder-only changes are
     * handled as partial updates before this reason is selected.
     */
    TRIGGER_HIT,

    /**
     * An earlier event for the same document required full processing and was collapsed into this
     * surviving batch event.
     */
    INTRA_BATCH_PREDECESSOR_REQUIRES_FULL,

    /**
     * {@link AutoEmbeddingMaterializedViewConfig#forceChangeStreamFullUpdate()} explicitly selected
     * full processing.
     */
    FEATURE_FLAG_FORCED_FULL_UPDATE,

    /**
     * The update was initially partial-eligible, but no partial body could be safely emitted:
     * extraction was unavailable or unsafe, filter maintenance required full processing, or the
     * generated partial body was empty. This is a full-route outcome, not necessarily an extraction
     * failure.
     */
    PARTIAL_UPDATE_FALLBACK,

    /** A safe filter-only or partial-embed {@code $set} update was emitted. */
    NONE;

    /** True if this outcome forces full (re-embed + upsert) processing. */
    public boolean requiresFull() {
      return this != NONE;
    }

    /** The {@code result} tag value paired with this reason. */
    public String result() {
      return requiresFull() ? "full" : "partial";
    }
  }

  /** Collects base-document field references from an aggregation expression tree. */
  private static final class RefCollector {

    private final Set<FieldPath> refs = new HashSet<>();
    private boolean wholeDocument = false;

    void collect(BsonValue value) {
      if (value.isString()) {
        collectString(value.asString().getValue());
      } else if (value.isDocument()) {
        for (var entry : value.asDocument().entrySet()) {
          if (entry.getKey().equals(LITERAL_OPERATOR)) {
            // $literal subtrees are data, not references.
            continue;
          }
          if (FIELD_NAME_OPERATORS.contains(entry.getKey())) {
            this.wholeDocument = true;
            return;
          }
          collect(entry.getValue());
        }
      } else if (value.isArray()) {
        value.asArray().forEach(this::collect);
      }
    }

    private void collectString(String expression) {
      if (expression.startsWith(VARIABLE_PREFIX)) {
        if (WHOLE_DOCUMENT_VARIABLES.contains(expression)) {
          this.wholeDocument = true;
        } else if (expression.startsWith(CURRENT_FIELD_PREFIX)) {
          addRef(expression.substring(CURRENT_FIELD_PREFIX.length()));
        } else if (expression.startsWith(ROOT_FIELD_PREFIX)) {
          addRef(expression.substring(ROOT_FIELD_PREFIX.length()));
        }
        // Other $$variables (loop/user variables, $$REMOVE, ...) are not base-field references;
        // the expressions bound to them are collected where they are defined.
      } else if (expression.startsWith(EXPRESSION_PREFIX)) {
        addRef(expression.substring(EXPRESSION_PREFIX.length()));
      }
    }

    private void addRef(String path) {
      if (!path.isEmpty()) {
        this.refs.add(FieldPath.parse(path));
      }
    }
  }
}
