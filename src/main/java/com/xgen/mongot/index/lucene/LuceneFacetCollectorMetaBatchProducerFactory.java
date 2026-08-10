package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.index.IntermediateFacetBucket;
import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.lucene.explain.explainers.FacetFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.facet.ParentDedupedLongRangeFacetCounts;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.LuceneSearchQueryFactoryDistributor;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.ReturnScope;
import com.xgen.mongot.index.query.collectors.AggregationCollector;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import com.xgen.mongot.util.timers.InvocationCountingTimer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.sortedset.ConcurrentSortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.bson.BsonString;

public class LuceneFacetCollectorMetaBatchProducerFactory {

  static LuceneFacetCollectorMetaBatchProducer create(
      CollectorQuery collectorQuery,
      LuceneFacetContext facetContext,
      LuceneIndexSearcherReference searcherReference,
      TopDocs topDocs,
      FacetsCollector facetsCollector,
      Optional<NamedExecutorService> concurrentSearchExecutor,
      LuceneSearchQueryFactoryDistributor queryFactory,
      boolean embeddedScopedFacetsEnabled)
      throws IOException, InterruptedException, InvalidQueryException {
    // Ensure that we have an exact count.
    checkState(
        topDocs.totalHits.relation() == TotalHits.Relation.EQUAL_TO,
        "LuceneFacetCollectorMetaBatchProducer requires an exact count.");
    var returnScopePath = collectorQuery.returnScope().map(ReturnScope::path);
    switch (collectorQuery.collector()) {
      case FacetCollector facetCollector:
        Map<FacetDefinition.Type, Map<String, FacetDefinition>> typeToFacetToDefinition =
            facetCollector.getFacetDefinitionsByType();

        // Partition string facets into document-indexed (unscoped) and embeddedDocument-scoped.
        Map<String, FacetDefinition> unscopedStringDefs = new HashMap<>();
        Map<String, FacetDefinition.StringFacetDefinition> scopedStringDefs = new HashMap<>();
        for (Map.Entry<String, FacetDefinition> entry :
            typeToFacetToDefinition.get(FacetDefinition.Type.STRING).entrySet()) {
          if (entry.getValue() instanceof FacetDefinition.StringFacetDefinition stringDef
              && stringDef.embeddedDocument().isPresent()) {
            scopedStringDefs.put(entry.getKey(), stringDef);
          } else {
            unscopedStringDefs.put(entry.getKey(), entry.getValue());
          }
        }

        // Partition number/date (boundary) facets into document-indexed and scoped, as above.
        Map<String, FacetDefinition> unscopedBoundaryDefs = new HashMap<>();
        Map<String, FacetDefinition.BoundaryFacetDefinition<?>> scopedBoundaryDefs =
            new HashMap<>();
        for (FacetDefinition.Type type :
            List.of(FacetDefinition.Type.NUMBER, FacetDefinition.Type.DATE)) {
          for (Map.Entry<String, FacetDefinition> entry :
              typeToFacetToDefinition.get(type).entrySet()) {
            if (entry.getValue() instanceof FacetDefinition.BoundaryFacetDefinition<?> boundaryDef
                && boundaryDef.embeddedDocument().isPresent()) {
              scopedBoundaryDefs.put(entry.getKey(), boundaryDef);
            } else {
              unscopedBoundaryDefs.put(entry.getKey(), entry.getValue());
            }
          }
        }

        Queue<LuceneMetaBucketProducer> bucketProducers = new LinkedList<>();
        // Add bucket producers for number, date, and string facets.
        bucketProducers.addAll(
            getBoundaryBucketProducers(
                unscopedBoundaryDefs,
                facetContext,
                facetsCollector,
                topDocs.totalHits.value(),
                returnScopePath));

        bucketProducers.addAll(
            getStringBucketProducers(
                unscopedStringDefs,
                facetContext,
                searcherReference,
                facetsCollector,
                returnScopePath,
                concurrentSearchExecutor));

        bucketProducers.addAll(
            getScopedStringBucketProducers(
                scopedStringDefs,
                searcherReference,
                facetsCollector,
                queryFactory,
                embeddedScopedFacetsEnabled));

        bucketProducers.addAll(
            getScopedBoundaryBucketProducers(
                scopedBoundaryDefs,
                facetContext,
                searcherReference,
                facetsCollector,
                queryFactory,
                embeddedScopedFacetsEnabled));

        return new LuceneFacetCollectorMetaBatchProducer(
            topDocs.totalHits.value(), bucketProducers, facetCollector);
      case AggregationCollector aggregationCollector:
        throw new UnsupportedOperationException(
            "Aggregation is not supported for intermediate query yet");
    }
  }

  /**
   * Intermediate (cross-partition) bucket producers for embeddedDocument-scoped string facets. Each
   * facet runs its own child query and counts matching children deduplicated to the root; buckets
   * are emitted via {@code getAllChildren} so the final top-N is applied after cross-partition
   * merge, matching the unscoped token-facet path.
   */
  private static List<LuceneMetaBucketProducer> getScopedStringBucketProducers(
      Map<String, FacetDefinition.StringFacetDefinition> definitions,
      LuceneIndexSearcherReference searcherReference,
      FacetsCollector resultParentHits,
      LuceneSearchQueryFactoryDistributor queryFactory,
      boolean embeddedScopedFacetsEnabled)
      throws IOException, InvalidQueryException {
    if (definitions.isEmpty()) {
      return List.of();
    }
    if (!embeddedScopedFacetsEnabled) {
      throw new InvalidQueryException(
          "embeddedDocument-scoped facets are not enabled. Please contact support.");
    }
    LuceneIndexSearcher searcher = searcherReference.getIndexSearcher();
    List<LuceneMetaBucketProducer> result = new ArrayList<>();
    for (Map.Entry<String, FacetDefinition.StringFacetDefinition> entry : definitions.entrySet()) {
      Optional<EmbeddedScopedStringFacetCounter.ScopedCounts> scopedCounts =
          EmbeddedScopedStringFacetCounter.count(
              searcher, queryFactory, entry.getValue(), resultParentHits);
      if (scopedCounts.isEmpty()) {
        continue;
      }
      FacetResult facetResult =
          scopedCounts.get().counts().getAllChildren(scopedCounts.get().lucenePath());
      if (facetResult != null) {
        result.add(LuceneStringFacetMetaBucketProducer.create(facetResult, entry.getKey()));
      }
    }
    return result;
  }

  /**
   * Intermediate (cross-partition) bucket producers for embeddedDocument-scoped number/date facets.
   * Each facet runs its own child query and counts matching children bucketed by boundaries and
   * deduplicated to the root. Buckets (and the default bucket) are emitted directly from the
   * per-root-deduped counts — the document-indexed {@code totalCount - sum} default-bucket
   * derivation is invalid under dedup, since a root can land in several buckets.
   */
  private static List<LuceneMetaBucketProducer> getScopedBoundaryBucketProducers(
      Map<String, FacetDefinition.BoundaryFacetDefinition<?>> definitions,
      LuceneFacetContext facetContext,
      LuceneIndexSearcherReference searcherReference,
      FacetsCollector resultParentHits,
      LuceneSearchQueryFactoryDistributor queryFactory,
      boolean embeddedScopedFacetsEnabled)
      throws IOException, InvalidQueryException {
    if (definitions.isEmpty()) {
      return List.of();
    }
    if (!embeddedScopedFacetsEnabled) {
      throw new InvalidQueryException(
          "embeddedDocument-scoped facets are not enabled. Please contact support.");
    }
    LuceneIndexSearcher searcher = searcherReference.getIndexSearcher();
    List<LuceneMetaBucketProducer> result = new ArrayList<>();
    for (Map.Entry<String, FacetDefinition.BoundaryFacetDefinition<?>> entry :
        definitions.entrySet()) {
      String facetName = entry.getKey();
      FacetDefinition.BoundaryFacetDefinition<?> definition = entry.getValue();
      ParentDedupedLongRangeFacetCounts counts =
          EmbeddedScopedBoundaryFacetCounter.count(
                  searcher, queryFactory, facetContext, definition, resultParentHits)
              .counts();

      List<IntermediateFacetBucket> buckets = new ArrayList<>();
      int[] rangeCounts = counts.counts();
      for (int i = 0; i < rangeCounts.length; i++) {
        buckets.add(
            new IntermediateFacetBucket(
                IntermediateFacetBucket.Type.FACET,
                facetName,
                definition.boundaries().get(i),
                rangeCounts[i]));
      }
      Optional<IntermediateFacetBucket> defaultBucket =
          definition.defaultBucketName().isPresent()
              ? Optional.of(
                  new IntermediateFacetBucket(
                      IntermediateFacetBucket.Type.FACET,
                      facetName,
                      new BsonString(definition.defaultBucketName().get()),
                      counts.defaultCount()))
              : Optional.empty();
      result.add(new LuceneBoundaryFacetMetaBucketProducer(buckets, defaultBucket));
    }
    return result;
  }

  private static List<LuceneMetaBucketProducer> getBoundaryBucketProducers(
      Map<String, FacetDefinition> definitions,
      LuceneFacetContext facetContext,
      FacetsCollector facetsCollector,
      long totalHits,
      Optional<FieldPath> returnScope)
      throws IOException, InvalidQueryException {
    List<LuceneMetaBucketProducer> results = new ArrayList<>();

    for (Map.Entry<String, FacetDefinition> entry : definitions.entrySet()) {
      var facetDefinition = (FacetDefinition.BoundaryFacetDefinition<?>) entry.getValue();

      FacetResult facetResult =
          LuceneFacetResultUtil.getBoundaryFacetResult(
              facetDefinition, facetContext, facetsCollector, returnScope);

      LuceneBoundaryFacetMetaBucketProducer producer =
          LuceneBoundaryFacetMetaBucketProducer.create(
              facetResult, entry.getKey(), facetDefinition, totalHits);

      results.add(producer);
    }
    return results;
  }

  private static List<LuceneMetaBucketProducer> getStringBucketProducers(
      Map<String, FacetDefinition> definitions,
      LuceneFacetContext facetContext,
      LuceneIndexSearcherReference searcherReference,
      FacetsCollector collector,
      Optional<FieldPath> returnScope,
      Optional<NamedExecutorService> concurrentSearchExecutor)
      throws IOException, InterruptedException, InvalidQueryException {
    var searcher = searcherReference.getIndexSearcher();
    if (definitions.isEmpty()) {
      return List.of();
    }

    Optional<FacetFeatureExplainer> facetFeatureExplainer =
        Explain.getQueryInfo()
            .map(
                queryInfo ->
                    queryInfo.getFeatureExplainer(
                        FacetFeatureExplainer.class, FacetFeatureExplainer::new));

    Map<FieldTypeDefinition.Type, Map<String, FacetDefinition.StringFacetDefinition>>
        facetableStringTypeToNameToDefinition =
            LuceneFacetResultUtil.groupFacetableStringDefinitions(
                facetContext, definitions, returnScope);

    List<LuceneMetaBucketProducer> stringFacetBucketProducers =
        getStringFacetFieldBucketProducers(
            facetableStringTypeToNameToDefinition.get(FieldTypeDefinition.Type.STRING_FACET),
            searcher,
            collector,
            facetFeatureExplainer,
            concurrentSearchExecutor);

    List<LuceneMetaBucketProducer> tokenFacetBucketProducers =
        getTokenFieldBucketProducers(
            facetFeatureExplainer,
            facetableStringTypeToNameToDefinition.get(FieldTypeDefinition.Type.TOKEN),
            searcher,
            collector,
            returnScope,
            concurrentSearchExecutor);

    List<LuceneMetaBucketProducer> result = new ArrayList<>(stringFacetBucketProducers);
    result.addAll(tokenFacetBucketProducers);
    return result;
  }

  private static List<LuceneMetaBucketProducer> getStringFacetFieldBucketProducers(
      Map<String, FacetDefinition.StringFacetDefinition> definitions,
      LuceneIndexSearcher searcher,
      FacetsCollector collector,
      Optional<FacetFeatureExplainer> facetFeatureExplainer,
      Optional<NamedExecutorService> concurrentSearchExecutor)
      throws IOException, InterruptedException {

    List<LuceneMetaBucketProducer> result = new ArrayList<>();
    if (definitions.isEmpty() || searcher.getFacetsState().isEmpty()) {
      return result;
    }
    var facetsState = searcher.getFacetsState().get();

    Optional<ExplainTimings> facetTimings =
        facetFeatureExplainer.map(FacetFeatureExplainer::getCreateCountTimings);

    Facets counts;
    try (var unused =
        new InvocationCountingTimer.AutocloseableOptional<>(
            facetTimings.map(t -> t.split(ExplainTimings.Type.GENERATE_FACET_COUNTS)))) {
      counts =
          concurrentSearchExecutor.isPresent()
              ? new ConcurrentSortedSetDocValuesFacetCounts(
                  facetsState, collector, concurrentSearchExecutor.get())
              : new SortedSetDocValuesFacetCounts(facetsState, collector);
    }

    for (var entry : definitions.entrySet()) {
      facetFeatureExplainer.ifPresent(
          explainer ->
              explainer.addTotalStringFacetCardinalities(
                  entry.getKey(),
                  Optional.ofNullable(facetsState.getOrdRange(entry.getValue().path()))));

      Optional<FacetResult> facetResult =
          Optional.ofNullable(counts.getAllChildren(entry.getValue().path()));

      if (facetResult.isPresent()) {
        result.add(LuceneStringFacetMetaBucketProducer.create(facetResult.get(), entry.getKey()));
      }
    }
    return result;
  }

  private static List<LuceneMetaBucketProducer> getTokenFieldBucketProducers(
      Optional<FacetFeatureExplainer> facetFeatureExplainer,
      Map<String, FacetDefinition.StringFacetDefinition> facetDefinitions,
      LuceneIndexSearcher searcher,
      FacetsCollector collector,
      Optional<FieldPath> returnScope,
      Optional<NamedExecutorService> concurrentSearchExecutor)
      throws IOException, InterruptedException, InvalidQueryException {
    List<LuceneMetaBucketProducer> result = new ArrayList<>();
    Optional<ExplainTimings> facetTimings =
        facetFeatureExplainer.map(FacetFeatureExplainer::getCreateCountTimings);

    if (facetDefinitions.isEmpty()) {
      return result;
    }
    // If cache optional is empty, then FF enabling token facets is not enabled
    if (searcher.getTokenFacetsStateCache().isEmpty()) {
      throw new InvalidQueryException(
          String.format(
              "Faceting over token fields is not enabled. Facets %s are indexed as tokens. "
                  + "Please remove them from this query, or index the fields as stringFacet",
              facetDefinitions.keySet()));
    }

    var tokenFacetsStateCache = searcher.getTokenFacetsStateCache().get();

    for (var entry : facetDefinitions.entrySet()) {
      var lucenePath =
          FieldName.TypeField.TOKEN.getLuceneFieldName(
              FieldPath.parse(entry.getValue().path()), returnScope);
      var fieldState = tokenFacetsStateCache.get(lucenePath);
      facetFeatureExplainer.ifPresent(
          explainer ->
              explainer.addTotalStringFacetCardinalities(
                  entry.getKey(), fieldState.map(state -> state.getOrdRange(lucenePath))));
      if (fieldState.isEmpty()) {
        continue;
      }
      Facets counts;
      try (var unused =
          new InvocationCountingTimer.AutocloseableOptional<>(
              facetTimings.map(t -> t.split(ExplainTimings.Type.GENERATE_FACET_COUNTS)))) {
        counts =
            concurrentSearchExecutor.isPresent()
                ? new com.xgen.mongot.index.lucene.facet.ConcurrentSortedSetDocValuesFacetCounts(
                    fieldState.get(), collector, concurrentSearchExecutor.get())
                : new com.xgen.mongot.index.lucene.facet.SortedSetDocValuesFacetCounts(
                    fieldState.get(), collector);
      }

      Optional<FacetResult> facetResult = Optional.ofNullable(counts.getAllChildren(lucenePath));

      if (facetResult.isPresent()) {
        result.add(LuceneStringFacetMetaBucketProducer.create(facetResult.get(), entry.getKey()));
      }
    }
    return result;
  }
}
