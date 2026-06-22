package com.xgen.mongot.index.lucene;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.FluentLogger;
import com.mongodb.lang.Nullable;
import com.xgen.mongot.index.definition.FacetableStringFieldDefinition;
import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.lucene.facet.TokenFacetsStateCache;
import com.xgen.mongot.index.lucene.facet.TokenSsdvFacetState;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.util.CheckedExceptionUtils;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.MultiFacets;
import org.apache.lucene.facet.range.LongRangeFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;

public class MongotDrillSideways extends DrillSideways {
  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  private final FacetCollector collector;
  private final LuceneFacetContext facetContext;
  private final Optional<FieldPath> returnScope;
  private final Optional<SortedSetDocValuesReaderState> maybeFacetsState;
  private final Optional<TokenFacetsStateCache> maybeTokenFacetsStateCache;

  MongotDrillSideways(
      IndexSearcher searcher,
      FacetsConfig config,
      FacetCollector collector,
      LuceneFacetContext facetContext,
      Optional<FieldPath> returnScope,
      Optional<SortedSetDocValuesReaderState> maybeFacetsState,
      Optional<TokenFacetsStateCache> maybeTokenFacetsStateCache,
      Optional<NamedExecutorService> optimizedDrillSidewaysExecutor) {
    /* Passing taxoReader and state as null here as they are used only in buildFacetsResult(),
    which has been overridden here */
    super(searcher, config, null, null, optimizedDrillSidewaysExecutor.orElse(null));
    this.collector = collector;
    this.facetContext = facetContext;
    this.returnScope = returnScope;
    this.maybeFacetsState = maybeFacetsState;
    this.maybeTokenFacetsStateCache = maybeTokenFacetsStateCache;
  }

  @Override
  protected Facets buildFacetsResult(
      @Nullable FacetsCollector ignoredDrillDowns,
      FacetsCollector[] drillSideways,
      String[] drillSidewaysDims)
      throws IOException {
    // This represents all the facets we want counts over
    Map<String, Facets> dimToDrillSidewaysFacets = new HashMap<>();

    List<String> emptyStateLuceneFieldNames = new ArrayList<>();
    for (int i = 0; i < drillSideways.length; i++) {
      String dim = drillSidewaysDims[i];
      FacetDefinition facetDefinition = this.collector.facetDefinitions().get(dim);
      try {
        switch (facetDefinition) {
          case FacetDefinition.StringFacetDefinition stringFacetDefinition -> {
            FacetableStringFieldDefinition fieldDefinition =
                this.facetContext.getStringFacetFieldDefinition(
                    stringFacetDefinition, this.returnScope);

            String path = stringFacetDefinition.path();

            if (fieldDefinition.getType() == FieldTypeDefinition.Type.TOKEN) {
              addFacetOrTagEmptyNameToken(
                  path, dimToDrillSidewaysFacets, emptyStateLuceneFieldNames, drillSideways[i]);
            } else if (fieldDefinition.getType() == FieldTypeDefinition.Type.STRING_FACET) {
              addFacetOrTagEmptyNameStringFacet(
                  dimToDrillSidewaysFacets, emptyStateLuceneFieldNames, path, drillSideways[i]);
            } else {
              throw new UnsupportedOperationException(
                  "Unknown string facet type : " + fieldDefinition.getClass());
            }
          }

          case FacetDefinition.NumericFacetDefinition numericFacetDefinition -> {
            String luceneDim =
                this.facetContext.getBoundaryFacetPath(numericFacetDefinition, this.returnScope);
            dimToDrillSidewaysFacets.put(
                luceneDim,
                new LongRangeFacetCounts(
                    luceneDim,
                    drillSideways[i],
                    this.facetContext.getRanges(numericFacetDefinition, this.returnScope)));
          }

          case FacetDefinition.DateFacetDefinition dateFacetDefinition -> {
            String luceneDim =
                this.facetContext.getBoundaryFacetPath(dateFacetDefinition, this.returnScope);
            dimToDrillSidewaysFacets.put(
                luceneDim,
                new LongRangeFacetCounts(
                    luceneDim,
                    drillSideways[i],
                    this.facetContext.getRanges(dateFacetDefinition, this.returnScope)));
          }
        }
      } catch (InvalidQueryException e) {
        // This Runtime exception is actually caught in the public MongotDrillSideways.searchSafe()
        // method and the unwrapped checked exception is rethrown.
        throw new RuntimeException(e);
      }
    }

    if (!emptyStateLuceneFieldNames.isEmpty()) {
      FLOGGER.atInfo().atMostEvery(1, TimeUnit.HOURS).log(
          "TokenSsdvFacetState is empty for LuceneFieldName: %s", emptyStateLuceneFieldNames);
    }
    return new MultiFacets(dimToDrillSidewaysFacets);
  }

  @VisibleForTesting
  Facets buildFacetsResult(FacetsCollector[] drillSideways, String[] drillSidewaysDims)
      throws IOException {
    // Passing drill downs as null as a different flow is used for handling drill down queries
    return buildFacetsResult(null, drillSideways, drillSidewaysDims);
  }

  private void addFacetOrTagEmptyNameStringFacet(
      Map<String, Facets> dimToDrillSidewaysFacets,
      List<String> emptyStateLuceneFieldNames,
      String path,
      FacetsCollector drillSidewaysCollector)
      throws IOException {
    if (this.maybeFacetsState.isPresent()) {
      SortedSetDocValuesReaderState facetsState = this.maybeFacetsState.get();
      dimToDrillSidewaysFacets.put(
          path, new SortedSetDocValuesFacetCounts(facetsState, drillSidewaysCollector));
    } else {
      emptyStateLuceneFieldNames.add(path);
    }
  }

  private void addFacetOrTagEmptyNameToken(
      String path,
      Map<String, Facets> dimToDrillSidewaysFacets,
      List<String> emptyStateLuceneFieldNames,
      FacetsCollector drillSidewaysCollector)
      throws InvalidQueryException, IOException {

    if (this.maybeTokenFacetsStateCache.isEmpty()) {
      throw new InvalidQueryException(
          String.format(
              "Faceting over token fields is not enabled. Facets %s are indexed as tokens."
                  + " Please remove them from this query, or index the fields as stringFacet",
              this.collector.facetDefinitions().keySet()));
    }

    String luceneDim =
        FieldName.TypeField.TOKEN.getLuceneFieldName(FieldPath.parse(path), this.returnScope);
    Optional<TokenSsdvFacetState> maybeTokenState =
        this.maybeTokenFacetsStateCache.get().get(luceneDim);
    if (maybeTokenState.isPresent()) {
      TokenSsdvFacetState tokenState = maybeTokenState.get();
      dimToDrillSidewaysFacets.put(
          luceneDim,
          new com.xgen.mongot.index.lucene.facet.SortedSetDocValuesFacetCounts(
              tokenState, drillSidewaysCollector));
    } else {
      emptyStateLuceneFieldNames.add(luceneDim);
    }
  }

  /**
   * Overrides base {@link DrillSideways#search} to throw exception to encourage safer {@link
   * MongotDrillSideways#searchSafe}
   *
   * @deprecated use {@link MongotDrillSideways#searchSafe} instead for better checked exceptions
   */
  @Deprecated
  @Override
  public <R> ConcurrentDrillSidewaysResult<R> search(
      DrillDownQuery query, CollectorManager<?, R> hitCollectorManager) throws IOException {
    throw new UnsupportedOperationException("Please use searchSafe() instead");
  }

  public <R> ConcurrentDrillSidewaysResult<R> searchSafe(
      DrillDownQuery query, CollectorManager<?, R> hitCollectorManager)
      throws IOException, InvalidQueryException {
    try {
      return super.search(query, hitCollectorManager);
    } catch (RuntimeException e) {
      CheckedExceptionUtils.propagateCheckedIfType(e, InvalidQueryException.class);
      throw e;
    }
  }
}
