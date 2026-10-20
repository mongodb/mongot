package com.xgen.mongot.index.lucene.explain.explainers;

import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.errorprone.annotations.Var;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.information.SortStats;
import com.xgen.mongot.index.lucene.explain.information.creator.QueryExplainInformationCreator;
import com.xgen.mongot.index.lucene.explain.profiler.ProfileDocIdSetIterator;
import com.xgen.mongot.index.lucene.explain.query.QueryVisitorQueryExecutionContext;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.FeatureExplainer;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.bson.BsonDocument;
import org.bson.BsonString;

public class SortFeatureExplainer implements FeatureExplainer {
  private final SortSpec sortSpec;
  private final ImmutableSetMultimap<FieldPath, FieldName.TypeField> fieldToSortableTypes;

  private final ExplainTimings timings;

  @GuardedBy("this")
  private final Map<FieldPath, BsonDocument> scopedChildQueries = new HashMap<>();

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

  /** Records the compiled query used by the scoped sort, independently of the main query tree. */
  public synchronized void setScopedChildQuery(FieldPath field, Query query) {
    var node = new QueryVisitorQueryExecutionContext().getOrCreateNode(query).orElseThrow();
    // Child execution timing is outside this contract. Emit structural explain at every verbosity,
    // using the standard visitor and serializer to retain operator paths and query args.
    this.scopedChildQueries.put(
        field,
        QueryExplainInformationCreator.fromNode(node, Explain.Verbosity.QUERY_PLANNER).toBson());
  }

  /** Maps each scoped sort key to its embedded path and compiled child-query explain tree. */
  private synchronized Optional<BsonDocument> scopedEmbeddedDocuments() {
    BsonDocument embeddedDocuments = new BsonDocument();
    for (MongotSortField field : this.sortSpec.getSortFields()) {
      BsonDocument query = this.scopedChildQueries.get(field.field());
      if (field.options() instanceof UserFieldSortOptions userOptions && query != null) {
        userOptions
            .embeddedScope()
            .ifPresent(
                scope ->
                    embeddedDocuments.append(
                        field.field().toString(),
                        new BsonDocument("path", new BsonString(scope.path().toString()))
                            .append("query", query)));
      }
    }
    return embeddedDocuments.isEmpty() ? Optional.empty() : Optional.of(embeddedDocuments);
  }

  @Override
  public synchronized void emitExplanation(
      Explain.Verbosity verbosity, SearchExplainInformationBuilder builder) {
    var filteredFieldToTypes = getFilteredFieldToTypes();
    var embeddedDocuments = scopedEmbeddedDocuments();

    if (verbosity.equals(Explain.Verbosity.QUERY_PLANNER)) {
      builder.sortStats(
          SortStats.create(filteredFieldToTypes, this.usesIndexSort, embeddedDocuments));
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
            this.usesIndexSort,
            embeddedDocuments));
  }
}
