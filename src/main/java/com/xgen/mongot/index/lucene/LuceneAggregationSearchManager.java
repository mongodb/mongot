package com.xgen.mongot.index.lucene;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.aggregation.AggregationContext;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs;
import com.xgen.mongot.index.query.aggregation.AggregationResult;
import com.xgen.mongot.index.query.aggregation.AggregationSpec;
import com.xgen.mongot.index.query.aggregation.SearchAggregationBootstrap;
import com.xgen.mongot.index.query.aggregation.TopAggregationSpec;
import com.xgen.mongot.index.query.collectors.AggDefinition;
import com.xgen.mongot.index.query.collectors.AggDefinition.LeafOperator;
import com.xgen.mongot.index.query.collectors.AggregationCollector;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.index.query.collectors.FacetDefinition.BoundaryFacetDefinition;
import com.xgen.mongot.index.query.sort.SequenceToken;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.bson.BsonString;

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
    AggregationSpec rootSpec = toTopLevelSpec(context, this.facetContext);

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

  /** Construct a top-level spec from all branches */
  private TopAggregationSpec toTopLevelSpec(
      AggregationContext context, LuceneFacetContext facetContext) {
    List<AggregationSpec> children =
        new ArrayList<>(this.aggregationCollector.aggDefinitions().size());
    for (var entry : this.aggregationCollector.aggDefinitions().entrySet()) {
      children.add(lower(entry.getKey(), entry.getValue(), context, facetContext));
    }
    return new TopAggregationSpec(children);
  }

  // Build one facet's nested spec.
  private AggregationSpec lower(
      String name, AggDefinition def, AggregationContext context, LuceneFacetContext facetContext) {
    @Var AggregationSpec spec = operatorSpec(name, def.leafOperator(), context.getRegistry());
    List<FacetDefinition> aggs = def.aggs();
    // build from bottom to up
    for (int i = aggs.size() - 1; i >= 0; i--) {
      boolean isRoot = i == 0;
      spec = groupSpec(name, aggs.get(i), isRoot, List.of(spec), context, facetContext);
    }
    return spec;
  }

  private AggregationSpec groupSpec(
      String name,
      FacetDefinition facetDefinition,
      boolean isRoot,
      List<AggregationSpec> childrenSpecs,
      AggregationContext context,
      LuceneFacetContext facetContext) {
    AggregationRegistry registry = context.getRegistry();
    FieldPath path = FieldPath.parse(facetDefinition.path());
    return switch (facetDefinition) {
      case FacetDefinition.StringFacetDefinition stringFacetDefinition ->
          registry
              .getGroupSpecSupplier(SearchAggregationBootstrap.TERMS)
              .build(
                  name,
                  path,
                  childrenSpecs,
                  new GroupingArgs.TermsArgs(
                      isRoot, TypeField.TOKEN, stringFacetDefinition.numBuckets()),
                  context);
      case FacetDefinition.DateFacetDefinition dateFacetDefinition -> {
        FieldDefinition fieldDef =
            facetContext
                .getFieldDefinitionResolver()
                .getFieldDefinition(path, Optional.empty())
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Unresolvable field definition for path: " + path));
        yield registry
            .getGroupSpecSupplier(SearchAggregationBootstrap.DATE_BOUNDARY)
            .build(
                name,
                path,
                childrenSpecs,
                new GroupingArgs.BoundaryArgs(
                    dateFacetDefinition.boundaries(),
                    dateFacetDefinition.defaultBucketName().map(BsonString::new),
                    getBoundaryFacetFieldTypeWithRuntimeException(
                        facetContext, dateFacetDefinition),
                    fieldDef),
                context);
      }
      case FacetDefinition.NumericFacetDefinition numericFacetDefinition -> {
        FieldDefinition fieldDef =
            facetContext
                .getFieldDefinitionResolver()
                .getFieldDefinition(path, Optional.empty())
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Unresolvable field definition for path: " + path));
        yield registry
            .getGroupSpecSupplier(SearchAggregationBootstrap.NUMBER_BOUNDARY)
            .build(
                name,
                path,
                childrenSpecs,
                new GroupingArgs.BoundaryArgs(
                    numericFacetDefinition.boundaries(),
                    numericFacetDefinition.defaultBucketName().map(BsonString::new),
                    getBoundaryFacetFieldTypeWithRuntimeException(
                        facetContext, numericFacetDefinition),
                    fieldDef),
                context);
      }
    };
  }

  private AggregationSpec operatorSpec(
      String name, LeafOperator leafOperator, AggregationRegistry registry) {
    return registry.getOperatorSupplier(leafOperator.name()).build(name, leafOperator.path());
  }

  FieldName.TypeField getBoundaryFacetFieldTypeWithRuntimeException(
      LuceneFacetContext facetContext, BoundaryFacetDefinition<?> fieldDefinition) {
    try {
      return facetContext.getBoundaryFacetFieldType(fieldDefinition, Optional.empty());
    } catch (Exception e) {
      // Note: ideally we should capture InvalidQueryException here but that introduced
      // java package circular dependencies. So we capture a generic Exception and convert
      // it into a runtime exception.
      throw new IllegalStateException("Failed to get field type for " + fieldDefinition.path(), e);
    }
  }
}
