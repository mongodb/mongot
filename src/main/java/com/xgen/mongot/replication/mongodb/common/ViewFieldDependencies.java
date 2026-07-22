package com.xgen.mongot.replication.mongodb.common;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.FluentLogger;
import com.mongodb.client.model.changestream.TruncatedArray;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.xgen.mongot.index.definition.ViewDefinition.SupportedStage;
import com.xgen.mongot.util.FieldPath;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

  /** Union of nonEmbedTriggers and all embed feeders; any change here needs embedding/full work. */
  private final ImmutableSet<FieldPath> triggerFields;

  /**
   * True when extraction could not bound the dependency set; every update is forced to the full
   * re-embed + upsert path regardless of which base fields it touched.
   */
  private final boolean dependsOnEverything;

  private ViewFieldDependencies(
      ImmutableSet<FieldPath> nonEmbedTriggers,
      ImmutableMap<FieldPath, ImmutableSet<FieldPath>> embedFeeders,
      boolean dependsOnEverything) {
    this.nonEmbedTriggers = nonEmbedTriggers;
    this.embedFeeders = embedFeeders;
    this.triggerFields =
        ImmutableSet.<FieldPath>builder()
            .addAll(nonEmbedTriggers)
            .addAll(embedFeeders.values().stream().flatMap(Set::stream).iterator())
            .build();
    this.dependsOnEverything = dependsOnEverything;
  }

  /** A dependency set that couldn't be bounded; forces every update onto the full path. */
  private static ViewFieldDependencies dependsOnEverythingFallback() {
    return new ViewFieldDependencies(ImmutableSet.of(), ImmutableMap.of(), true);
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
      // TODO(CLOUDP-416417): emit a counter on this fallback so operators can distinguish
      // exception-driven dependsOnEverything from intentionally unanalyzable views.
      LOG.atWarning().withCause(e).log(
          "Failed to derive view field dependencies; falling back to full processing");
      return dependsOnEverythingFallback();
    }
  }

  private static ViewFieldDependencies doDerive(
      List<BsonDocument> effectivePipeline, Set<FieldPath> embedPaths, Set<FieldPath> filterPaths) {

    // Fields defined by $set/$addFields so far, mapped to their resolved base-field feeders.
    Map<FieldPath, ImmutableSet<FieldPath>> computed = new HashMap<>();
    Set<FieldPath> triggers = new HashSet<>();

    for (BsonDocument stage : effectivePipeline) {
      if (stage.keySet().size() != 1) {
        return dependsOnEverythingFallback();
      }
      String stageName = stage.keySet().iterator().next();
      Optional<SupportedStage> stageType = SupportedStage.byStageName(stageName);
      if (stageType.isEmpty() || !stage.get(stageName).isDocument()) {
        return dependsOnEverythingFallback();
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
              return dependsOnEverythingFallback();
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
            return dependsOnEverythingFallback();
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

    // A filter field COMPUTED by the view also contributes its feeders: the filter-only partial
    // update re-extracts present filter values but cannot delete one that the view drops (e.g.
    // $cond -> $$REMOVE), so a change to such a feeder must take the full path. Passthrough filter
    // fields (not computed by the pipeline) are excluded, preserving the optimization for them.
    filterPaths.stream()
        .filter(path -> isComputed(path, computed))
        .forEach(path -> triggers.addAll(resolveRefs(Set.of(path), computed)));

    // triggers now holds match-referenced ∪ computed-filter feeders — the non-embed triggers.
    return new ViewFieldDependencies(canonicalize(triggers), embedFeeders, false);
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
    return this.dependsOnEverything;
  }

  /**
   * Returns true if the update may affect view membership or an embed source, i.e. the event must
   * take the embedding/full path rather than a pure filter-only partial update. Coarse gate used by
   * the batch-dedup pre-pass; {@link #classifyViewUpdate} makes the finer partial-embed decision.
   */
  public boolean requiresFullProcessing(@Nullable UpdateDescription updateDescription) {
    if (this.dependsOnEverything || updateDescription == null || forcesFull(updateDescription)) {
      return true;
    }
    Set<FieldPath> changed = changedPaths(updateDescription);
    // An empty update can't prove the indexed document is unaffected — force full.
    return changed.isEmpty() || hitsAny(changed, this.triggerFields);
  }

  /**
   * Classifies an UPDATE on a view-based auto-embed index: whether it must take the full path, and
   * if not, which embed sources need re-embedding (empty ⇒ filter-only partial update).
   */
  public ViewUpdateClassification classifyViewUpdate(
      @Nullable UpdateDescription updateDescription) {
    if (this.dependsOnEverything || updateDescription == null || forcesFull(updateDescription)) {
      return ViewUpdateClassification.full();
    }
    Set<FieldPath> changed = changedPaths(updateDescription);
    // Empty update, or a change to a membership/computed-filter feeder, can't take a partial $set.
    if (changed.isEmpty() || hitsAny(changed, this.nonEmbedTriggers)) {
      return ViewUpdateClassification.full();
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

  /**
   * True if a removal is present — removals can't be applied through the $set-only partial path.
   */
  private static boolean forcesFull(UpdateDescription updateDescription) {
    return !Optional.ofNullable(updateDescription.getRemovedFields())
        .orElseGet(Collections::emptyList)
        .isEmpty();
  }

  /**
   * Normalized base-collection paths a change touches (updated fields ∪ truncated-array fields).
   */
  private static Set<FieldPath> changedPaths(UpdateDescription updateDescription) {
    Set<String> updatedFields =
        Optional.ofNullable(updateDescription.getUpdatedFields())
            .map(BsonDocument::keySet)
            .orElseGet(Collections::emptySet);
    Set<String> truncatedArrays =
        Optional.ofNullable(updateDescription.getTruncatedArrays())
            .map(
                arrays -> arrays.stream().map(TruncatedArray::getField).collect(Collectors.toSet()))
            .orElseGet(Collections::emptySet);
    return Stream.of(updatedFields, truncatedArrays)
        .flatMap(Collection::stream)
        .map(FieldPath::parse)
        .map(ChangeStreams::filterFieldPath)
        .collect(Collectors.toSet());
  }

  /** True if any changed path is a direct relation of any field in {@code targets}. */
  private static boolean hitsAny(Set<FieldPath> changed, Set<FieldPath> targets) {
    return changed.stream().anyMatch(c -> targets.stream().anyMatch(c::isDirectRelation));
  }

  /**
   * Result of {@link #classifyViewUpdate}: {@code requiresFull} true means take the full path;
   * otherwise {@code changedEmbedPaths} are the embed sources to re-embed (empty ⇒ filter-only).
   */
  public record ViewUpdateClassification(
      boolean requiresFull, ImmutableSet<FieldPath> changedEmbedPaths) {
    private static ViewUpdateClassification full() {
      return new ViewUpdateClassification(true, ImmutableSet.of());
    }

    private static ViewUpdateClassification partial(ImmutableSet<FieldPath> changedEmbedPaths) {
      return new ViewUpdateClassification(false, changedEmbedPaths);
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
