package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.aggregation.AggregationContext;
import com.xgen.mongot.index.query.aggregation.AggregationResult;
import com.xgen.mongot.index.query.aggregation.AggregationSpecBuilder;
import com.xgen.mongot.index.query.aggregation.TopAggregationSpec;
import com.xgen.mongot.index.query.collectors.AggregationCollector;
import com.xgen.mongot.index.query.collectors.FacetDefinition.BoundaryFacetDefinition;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;

/**
 * The LuceneAggregationSearchManager is used to manage the execution of a single lucene search. It
 * is used by the {@link LuceneSearchIndexReader} to run an initial search and generate
 * AggregationQueryInfo with the info necessary to initialize a batch producer with the search
 * results, and either to build full {@link com.xgen.mongot.index.MetaResults} or produce
 * intermediate meta buckets.
 */
public class LuceneAggregationSearchManager
    extends AbstractLuceneSearchManager<LuceneAggregationSearchManager.AggregationQueryInfo> {

  private final LuceneFacetContext facetContext;
  private final AggregationCollector aggregationCollector;

  public static class AggregationQueryInfo extends QueryInfo {
    private final AggregationResult aggregationResult;

    public AggregationQueryInfo(
        TopDocs topDocs, boolean luceneExhausted, AggregationResult aggregationResult) {
      super(topDocs, luceneExhausted);
      this.aggregationResult = aggregationResult;
    }

    public AggregationResult getAggregationResult() {
      return this.aggregationResult;
    }
  }

  public LuceneAggregationSearchManager(
      Query luceneQuery,
      Optional<Sort> luceneSort,
      Optional<SequenceToken> searchAfter,
      LuceneFacetContext facetContext,
      AggregationCollector aggregationCollector) {
    super(luceneQuery, luceneSort, searchAfter);
    this.facetContext = facetContext;
    this.aggregationCollector = aggregationCollector;
  }

  @Override
  public AggregationQueryInfo initialSearch(
      LuceneIndexSearcherReference searcherReference, int batchSize) throws IOException {
    LuceneIndexSearcher searcher = searcherReference.getIndexSearcher();

    AggregationContext context = AggregationContext.createDefault(searcher.getIndexReader());
    TopAggregationSpec rootSpec =
        AggregationSpecBuilder.toTopLevelSpec(
            this.aggregationCollector,
            context.getRegistry(),
            Optional.of(new FacetContextBoundaryResolver(this.facetContext)));

    int readerLimit = Math.max(1, searcher.getIndexReader().maxDoc());
    int searchLimit = Math.min(readerLimit, batchSize);

    var searchCollectorManager = createCollectorManager(searchLimit, Integer.MAX_VALUE);

    AccumulatorCollectorManager accCM = new AccumulatorCollectorManager(rootSpec, context);

    var results =
        searcher.search(
            this.getLuceneQuery(), new MultiCollectorManager(accCM, searchCollectorManager));

    AggregationResult aggResult = Check.instanceOf(results[0], AggregationResult.class);
    TopDocs topDocs = Check.instanceOf(results[1], TopDocs.class);
    maybePopulateScores(searcher, topDocs.scoreDocs);

    return new AggregationQueryInfo(topDocs, topDocs.scoreDocs.length < batchSize, aggResult);
  }

  private static final class FacetContextBoundaryResolver
      implements AggregationSpecBuilder.BoundaryFieldResolver {
    private final LuceneFacetContext facetContext;

    FacetContextBoundaryResolver(LuceneFacetContext facetContext) {
      this.facetContext = facetContext;
    }

    @Override
    public FieldDefinition getFieldDefinition(FieldPath path) {
      return this.facetContext
          .getFieldDefinitionResolver()
          .getFieldDefinition(path, Optional.empty())
          .orElseThrow(
              () -> new IllegalStateException("Unresolvable field definition for path: " + path));
    }

    @Override
    public TypeField getBoundaryFacetFieldType(BoundaryFacetDefinition<?> facetDefinition) {
      try {
        return this.facetContext.getBoundaryFacetFieldType(facetDefinition, Optional.empty());
      } catch (Exception e) {
        // Note: ideally we should capture InvalidQueryException here but that introduced
        // java package circular dependencies. So we capture a generic Exception and convert
        // it into a runtime exception.
        throw new IllegalStateException(
            "Failed to get field type for " + facetDefinition.path(), e);
      }
    }
  }
}
