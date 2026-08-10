package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.lucene.facet.ParentDedupedLongRangeFacetCounts;
import com.xgen.mongot.index.lucene.query.LuceneSearchQueryFactoryDistributor;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.collectors.EmbeddedDocumentScope;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.search.Query;

/**
 * Computes counts for a single embeddedDocument-scoped number/date (boundary) facet: runs the
 * facet's {@code embeddedDocument.operator} as a child query and counts the matching children
 * bucketed by the facet's boundaries, deduplicated to the root document (each root at most once per
 * bucket, and at most once in the default bucket). The number/date analog of {@link
 * EmbeddedScopedStringFacetCounter}. Shared by the full-result path ({@link
 * LuceneMetaResultsBuilder}) and the intermediate cross-partition path ({@link
 * LuceneFacetCollectorMetaBatchProducerFactory}).
 */
final class EmbeddedScopedBoundaryFacetCounter {

  /** The deduplicated boundary counts plus the boundary ranges they align with. */
  record ScopedBoundaryCounts(ParentDedupedLongRangeFacetCounts counts) {}

  private EmbeddedScopedBoundaryFacetCounter() {}

  static ScopedBoundaryCounts count(
      LuceneIndexSearcher searcher,
      LuceneSearchQueryFactoryDistributor queryFactory,
      LuceneFacetContext facetContext,
      FacetDefinition.BoundaryFacetDefinition<?> definition,
      FacetsCollector resultParentHits)
      throws IOException, InvalidQueryException {
    EmbeddedDocumentScope scope = definition.embeddedDocument().orElseThrow();
    Optional<com.xgen.mongot.util.FieldPath> embeddedPath = Optional.of(scope.path());

    // The scoped facet reads the child-indexed numeric field under the embedded path, and buckets
    // against the same ranges (encoded consistently with the doc-values) the doc-indexed path uses.
    String lucenePath = facetContext.getBoundaryFacetPath(definition, embeddedPath);
    LongRange[] ranges = facetContext.getRanges(definition, embeddedPath);

    Query childQuery =
        queryFactory.createEmbeddedChildQuery(
            scope.operator(), scope.path(), searcher.getIndexReader());
    FacetsCollector childHits = searcher.search(childQuery, new FacetsCollectorManager());

    ParentDedupedLongRangeFacetCounts counts =
        new ParentDedupedLongRangeFacetCounts(
            lucenePath,
            ranges,
            childHits,
            queryFactory.rootParentBitSetProducer(),
            resultParentHits,
            searcher.getIndexReader());
    return new ScopedBoundaryCounts(counts);
  }
}
