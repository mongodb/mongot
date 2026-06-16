package com.xgen.mongot.index.lucene.explain.explainers;

import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.information.SortStats;
import com.xgen.mongot.index.lucene.explain.profiler.ProfileDocIdSetIterator;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.FeatureExplainer;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.search.DocIdSetIterator;

public class SortFeatureExplainer implements FeatureExplainer {
  private final SortSpec sortSpec;
  private final ImmutableSetMultimap<FieldPath, FieldName.TypeField> fieldToSortableTypes;

  private final ExplainTimings timings;

  @GuardedBy("this")
  @Var
  private ArrayList<ProfileDocIdSetIterator> allCompetitiveIterators;

  @GuardedBy("this")
  @Var
  private Optional<Boolean> usesIndexSort;

  public SortFeatureExplainer(
      SortSpec sortSpec,
      ImmutableSetMultimap<FieldPath, FieldName.TypeField> fieldToSortableTypes) {
    this.sortSpec = sortSpec;
    this.fieldToSortableTypes = fieldToSortableTypes;
    this.timings = ExplainTimings.builder().build();
    this.allCompetitiveIterators = new ArrayList<>();
    this.usesIndexSort = Optional.empty();
  }

  public ExplainTimings getTimings() {
    return this.timings;
  }

  @VisibleForTesting
  synchronized List<ProfileDocIdSetIterator> getAllCompetitiveIterators() {
    return List.copyOf(this.allCompetitiveIterators);
  }

  /**
   * Adds the profiled competitive iterator per segment if it is present. This should only be
   * invoked once per segment.
   */
  public synchronized void maybeAddCompetitiveIterator(
      Optional<ProfileDocIdSetIterator> maybeIterator) {
    maybeIterator.ifPresent(this.allCompetitiveIterators::add);
  }

  @VisibleForTesting
  ImmutableSetMultimap<FieldPath, FieldName.TypeField> getFilteredFieldToTypes() {
    Set<FieldPath> sortFields =
        this.sortSpec.getSortFields().stream()
            .map(MongotSortField::field)
            .collect(Collectors.toSet());

    return this.fieldToSortableTypes.entries().stream()
        .filter(entry -> sortFields.contains(entry.getKey()))
        .collect(toImmutableSetMultimap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public synchronized void aggregate() {
    ExplainTimings summed =
        this.allCompetitiveIterators.stream()
            .map(ProfileDocIdSetIterator::getExplainTimings)
            .reduce(ExplainTimings.builder().build(), ExplainTimings::merge);

    this.allCompetitiveIterators =
        new ArrayList<>(List.of(ProfileDocIdSetIterator.create(DocIdSetIterator.empty(), summed)));
  }

  public synchronized void setUsesIndexSort(boolean usesIndexSort) {
    this.usesIndexSort = Optional.of(usesIndexSort);
  }

  @Override
  public synchronized void emitExplanation(
      Explain.Verbosity verbosity, SearchExplainInformationBuilder builder) {
    var filteredFieldToTypes = getFilteredFieldToTypes();

    if (verbosity.equals(Explain.Verbosity.QUERY_PLANNER)) {
      builder.sortStats(SortStats.create(
          filteredFieldToTypes, this.usesIndexSort));
      return;
    }

    var competitiveIteratorTimingData =
        this.allCompetitiveIterators.stream()
            .map(ProfileDocIdSetIterator::getExplainTimings)
            .flatMap(ExplainTimings::stream)
            .collect(ExplainTimings.toExplainTimingData());

    builder.sortStats(
        SortStats.create(
            competitiveIteratorTimingData.isEmpty()
                ? Optional.empty()
                : Optional.of(
                    QueryExecutionArea.sortPrunedResultIterAreaFor(competitiveIteratorTimingData)),
            this.timings.allTimingDataIsEmpty()
                ? Optional.empty()
                : Optional.of(
                    QueryExecutionArea.sortComparatorAreaFor(this.timings.extractTimingData())),
            filteredFieldToTypes,
            this.usesIndexSort));
  }
}
