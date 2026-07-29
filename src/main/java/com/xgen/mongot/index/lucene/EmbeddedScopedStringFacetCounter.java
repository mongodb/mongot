package com.xgen.mongot.index.lucene;

import static java.lang.String.format;

import com.xgen.mongot.index.lucene.facet.ParentDedupedSortedSetDocValuesFacetCounts;
import com.xgen.mongot.index.lucene.facet.TokenSsdvFacetState;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.LuceneSearchQueryFactoryDistributor;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.collectors.EmbeddedDocumentScope;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.search.Query;

/**
 * Computes counts for a single embeddedDocument-scoped string (token) facet: runs the facet's
 * {@code embeddedDocument.operator} as a child query and counts the matching children deduplicated
 * to the root document (each root at most once per bucket). Shared by both the full-result path
 * ({@link LuceneMetaResultsBuilder}) and the intermediate cross-partition path ({@link
 * LuceneFacetCollectorMetaBatchProducerFactory}); each caller decides whether to read the counts
 * via {@code getTopChildren} (final result, honoring {@code numBuckets}) or {@code getAllChildren}
 * (intermediate, so the final top-N is applied after cross-partition merge).
 */
final class EmbeddedScopedStringFacetCounter {

  /** The deduplicated facet counts plus the child-scoped Lucene field name to read them under. */
  record ScopedCounts(Facets counts, String lucenePath) {}

  private EmbeddedScopedStringFacetCounter() {}

  static Optional<ScopedCounts> count(
      LuceneIndexSearcher searcher,
      LuceneSearchQueryFactoryDistributor queryFactory,
      FacetDefinition.StringFacetDefinition definition,
      FacetsCollector resultParentHits)
      throws IOException, InvalidQueryException {
    EmbeddedDocumentScope scope = definition.embeddedDocument().orElseThrow();

    if (searcher.getTokenFacetsStateCache().isEmpty()) {
      throw new InvalidQueryException(
          format(
              "Faceting over token fields is not enabled. Facet path '%s' is indexed as a token.",
              definition.path()));
    }

    // The scoped facet reads the child-indexed token field under the embedded path.
    String lucenePath =
        FieldName.TypeField.TOKEN.getLuceneFieldName(
            FieldPath.parse(definition.path()), Optional.of(scope.path()));
    Optional<TokenSsdvFacetState> fieldState =
        searcher.getTokenFacetsStateCache().get().get(lucenePath);
    if (fieldState.isEmpty()) {
      return Optional.empty();
    }

    Query childQuery =
        queryFactory.createEmbeddedChildQuery(
            scope.operator(), scope.path(), searcher.getIndexReader());
    FacetsCollector childHits = searcher.search(childQuery, new FacetsCollectorManager());

    Facets counts =
        new ParentDedupedSortedSetDocValuesFacetCounts(
            fieldState.get(), childHits, queryFactory.rootParentBitSetProducer(), resultParentHits);
    return Optional.of(new ScopedCounts(counts, lucenePath));
  }
}
