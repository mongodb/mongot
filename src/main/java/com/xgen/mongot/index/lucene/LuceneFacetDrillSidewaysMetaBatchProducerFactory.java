package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.lucene.explain.explainers.FacetFeatureExplainer;
import com.xgen.mongot.index.lucene.facet.TokenFacetsCardinalityLimitExceededException;
import com.xgen.mongot.index.lucene.facet.TokenFacetsStateCache;
import com.xgen.mongot.index.lucene.facet.TokenSsdvFacetState;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.ReturnScope;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Function;
import org.apache.lucene.facet.DrillSideways.DrillSidewaysResult;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

public class LuceneFacetDrillSidewaysMetaBatchProducerFactory {

  static LuceneFacetCollectorMetaBatchProducer create(
      CollectorQuery collectorQuery,
      LuceneFacetContext facetContext,
      LuceneIndexSearcherReference searcherReference,
      TopDocs topDocs,
      Function<String, Optional<DrillSidewaysResult>> facetToDrillSidewaysResultConverter)
      throws IOException, InterruptedException, InvalidQueryException {
    // Ensure that we have an exact count.
    checkState(
        topDocs.totalHits.relation == TotalHits.Relation.EQUAL_TO,
        "LuceneFacetDrillSidewaysMetaBatchProducer requires an exact count.");
    Optional<FieldPath> returnScope = collectorQuery.returnScope().map(ReturnScope::path);

    switch (collectorQuery.collector()) {
      case FacetCollector facetCollector:
        Map<FacetDefinition.Type, Map<String, FacetDefinition>> typeToFacetToDefinition =
            facetCollector.getFacetDefinitionsByType();

        Queue<LuceneMetaBucketProducer> bucketProducers = new LinkedList<>();
        // Add bucket producers for number, date, and string facets.
        bucketProducers.addAll(
            getBoundaryBucketProducers(
                typeToFacetToDefinition.get(FacetDefinition.Type.NUMBER),
                facetContext,
                facetToDrillSidewaysResultConverter,
                topDocs.totalHits.value,
                returnScope));

        bucketProducers.addAll(
            getBoundaryBucketProducers(
                typeToFacetToDefinition.get(FacetDefinition.Type.DATE),
                facetContext,
                facetToDrillSidewaysResultConverter,
                topDocs.totalHits.value,
                returnScope));

        bucketProducers.addAll(
            getStringBucketProducers(
                typeToFacetToDefinition.get(FacetDefinition.Type.STRING),
                facetContext,
                searcherReference,
                facetToDrillSidewaysResultConverter,
                returnScope));

        return new LuceneFacetCollectorMetaBatchProducer(
            topDocs.totalHits.value, bucketProducers, facetCollector);
    }
  }

  private static List<LuceneMetaBucketProducer> getBoundaryBucketProducers(
      Map<String, FacetDefinition> facetToDefinition,
      LuceneFacetContext facetContext,
      Function<String, Optional<DrillSidewaysResult>> facetToDrillSidewaysResultConverter,
      long totalHits,
      Optional<FieldPath> returnScope)
      throws IOException, InvalidQueryException {
    return CheckedStream.from(
            facetToDefinition.entrySet().stream()
                .filter(
                    entry -> facetToDrillSidewaysResultConverter.apply(entry.getKey()).isPresent()))
        .<LuceneMetaBucketProducer, IOException, InvalidQueryException>
            mapAndCollectCheckedBiValueException(
                entry -> {
                  var facetDefinition =
                      ((FacetDefinition.BoundaryFacetDefinition<?>) entry.getValue());
                  FacetResult facetResult =
                      getBoundaryFacetResult(
                          facetDefinition,
                          facetContext,
                          facetToDrillSidewaysResultConverter.apply(entry.getKey()).get(),
                          returnScope);
                  return LuceneBoundaryFacetMetaBucketProducer.create(
                      facetResult, entry.getKey(), facetDefinition, totalHits);
                });
  }

  private static FacetResult getBoundaryFacetResult(
      FacetDefinition.BoundaryFacetDefinition<?> boundaryDefinition,
      LuceneFacetContext facetContext,
      DrillSidewaysResult drillSidewaysResult,
      Optional<FieldPath> returnScope)
      throws IOException, InvalidQueryException {
    String path = facetContext.getBoundaryFacetPath(boundaryDefinition, returnScope);
    return drillSidewaysResult.facets.getAllChildren(path);
  }

  private static List<LuceneMetaBucketProducer> getStringBucketProducers(
      Map<String, FacetDefinition> stringFacetToDefinition,
      LuceneFacetContext facetContext,
      LuceneIndexSearcherReference searcherReference,
      Function<String, Optional<DrillSidewaysResult>> facetToDrillSidewaysResultsConverter,
      Optional<FieldPath> returnScope)
      throws IOException, InvalidQueryException {

    var searcher = searcherReference.getIndexSearcher();

    if (stringFacetToDefinition.isEmpty()) {
      return List.of();
    }

    Map<FieldTypeDefinition.Type, Map<String, FacetDefinition.StringFacetDefinition>>
        facetableStringTypeToNameToDefinition =
            LuceneFacetResultUtil.groupFacetableStringDefinitions(
                facetContext, stringFacetToDefinition, returnScope);

    List<LuceneMetaBucketProducer> stringFacetFieldProducers =
        getStringFacetFieldProducers(
            facetableStringTypeToNameToDefinition.get(FieldTypeDefinition.Type.STRING_FACET),
            facetToDrillSidewaysResultsConverter,
            searcher);
    List<LuceneMetaBucketProducer> tokenFieldProducers =
        getTokenFieldProducers(
            facetableStringTypeToNameToDefinition.get(FieldTypeDefinition.Type.TOKEN),
            facetToDrillSidewaysResultsConverter,
            returnScope,
            searcher);

    List<LuceneMetaBucketProducer> producers = new ArrayList<>(stringFacetFieldProducers);
    producers.addAll(tokenFieldProducers);
    return producers;
  }

  private static List<LuceneMetaBucketProducer> getStringFacetFieldProducers(
      Map<String, FacetDefinition.StringFacetDefinition> stringFacetToDefinition,
      Function<String, Optional<DrillSidewaysResult>> facetToDrillSidewaysResultsConverter,
      LuceneIndexSearcher searcher)
      throws IOException {

    if (searcher.getFacetsState().isEmpty()) {
      return List.of();
    }
    SortedSetDocValuesReaderState facetsState = searcher.getFacetsState().get();
    List<LuceneMetaBucketProducer> producers = new ArrayList<>();
    Optional<FacetFeatureExplainer> explainer = LuceneFacetResultUtil.getFacetFeatureExplainer();

    for (var entry : stringFacetToDefinition.entrySet()) {

      String path = entry.getValue().path();
      Optional<SortedSetDocValuesReaderState.OrdRange> maybeOrdRange =
          Optional.ofNullable(facetsState.getOrdRange(path));
      explainer.ifPresent(
          exp -> exp.addTotalStringFacetCardinalities(entry.getKey(), maybeOrdRange));

      Optional<DrillSidewaysResult> drillSidewaysResult =
          facetToDrillSidewaysResultsConverter.apply(entry.getKey());
      if (drillSidewaysResult.isEmpty()) {
        continue;
      }
      Facets facetCounts = drillSidewaysResult.get().facets;

      Optional<FacetResult> facetResult = Optional.ofNullable(facetCounts.getAllChildren(path));
      if (facetResult.isPresent()) {
        producers.add(
            LuceneStringFacetMetaBucketProducer.create(facetResult.get(), entry.getKey()));
      }
    }
    return producers;
  }

  private static List<LuceneMetaBucketProducer> getTokenFieldProducers(
      Map<String, FacetDefinition.StringFacetDefinition> stringFacetToDefinition,
      Function<String, Optional<DrillSidewaysResult>> facetToDrillSidewaysResultsConverter,
      Optional<FieldPath> returnScope,
      LuceneIndexSearcher searcher)
      throws IOException, TokenFacetsCardinalityLimitExceededException {
    if (searcher.getTokenFacetsStateCache().isEmpty()) {
      return List.of();
    }
    TokenFacetsStateCache facetsState = searcher.getTokenFacetsStateCache().get();
    List<LuceneMetaBucketProducer> producers = new ArrayList<>();
    Optional<FacetFeatureExplainer> explainer = LuceneFacetResultUtil.getFacetFeatureExplainer();

    for (var entry : stringFacetToDefinition.entrySet()) {
      String path = entry.getValue().path();
      String lucenePath =
          FieldName.TypeField.TOKEN.getLuceneFieldName(FieldPath.parse(path), returnScope);
      Optional<TokenSsdvFacetState> fieldState = facetsState.get(lucenePath);
      explainer.ifPresent(
          exp ->
              exp.addTotalStringFacetCardinalities(
                  entry.getKey(), fieldState.map(state -> state.getOrdRange(lucenePath))));

      Optional<DrillSidewaysResult> maybeDrillSidewaysResult =
          facetToDrillSidewaysResultsConverter.apply(entry.getKey());
      if (maybeDrillSidewaysResult.isEmpty()) {
        continue;
      }
      Facets facetCounts = maybeDrillSidewaysResult.get().facets;

      Optional<FacetResult> facetResult =
          Optional.ofNullable(facetCounts.getAllChildren(lucenePath));
      if (facetResult.isPresent()) {
        producers.add(
            LuceneStringFacetMetaBucketProducer.create(facetResult.get(), entry.getKey()));
      }
    }
    return producers;
  }
}
