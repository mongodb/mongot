package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.index.lucene.LuceneSearchManager.QueryInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.quantization.BinaryQuantizedRescoreQuery;
import com.xgen.mongot.index.lucene.quantization.BinaryQuantizedVectorRescorer;
import com.xgen.mongot.index.lucene.query.NestedAvgVectorRescorer;
import com.xgen.mongot.index.lucene.query.custom.WrappedKnnQuery;
import com.xgen.mongot.index.lucene.query.util.WrappedToParentBlockJoinQuery;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.join.BitSetProducer;

/**
 * Version of {@link LuceneOperatorSearchManager} which overrides the batchSize with {@link
 * VectorSearchQuery.Fields#NUM_CANDIDATES} and caps Lucene results using the {@link
 * VectorSearchQuery.Fields#LIMIT}.
 */
public class LuceneVectorSearchManager implements LuceneSearchManager<QueryInfo> {

  private final Query luceneQuery;
  private final VectorSearchCriteria criteria;
  private final Optional<BinaryQuantizedVectorRescorer> rescorer;
  private final Optional<NestedAvgVectorRescorer> nestedAvgRescorer;

  public LuceneVectorSearchManager(
      Query luceneQuery,
      VectorSearchCriteria criteria,
      Optional<BinaryQuantizedVectorRescorer> rescorer,
      Optional<NestedAvgVectorRescorer> nestedAvgRescorer) {

    this.rescorer = rescorer;
    this.nestedAvgRescorer = nestedAvgRescorer;
    this.luceneQuery = luceneQuery;
    this.criteria = criteria;
  }

  @Override
  public QueryInfo initialSearch(LuceneIndexSearcherReference searcherReference, int batchSize)
      throws IOException {

    boolean requiresAnyRescoring = this.rescorer.isPresent() || this.nestedAvgRescorer.isPresent();
    int hitsToCollect =
        requiresAnyRescoring
            // rescoring needs to operate on the full set of candidates
            ? ((ApproximateVectorSearchCriteria) this.criteria).numCandidates()
            : this.criteria.limit();

    LuceneIndexSearcher indexSearcher = searcherReference.getIndexSearcher();
    TopScoreDocCollectorManager collectorManager =
        new TopScoreDocCollectorManager(hitsToCollect, 0);

    @Var TopDocs topCandidates = topCandidates(indexSearcher, collectorManager);

    if (this.nestedAvgRescorer.isPresent()) {
      topCandidates =
          this.nestedAvgRescorer
              .get()
              .rescore(indexSearcher, topCandidates, this.luceneQuery, this.criteria.limit());
    }

    TopDocs result =
        new TopDocs(
            // override number of hits to match the limit
            new TotalHits(topCandidates.scoreDocs.length, TotalHits.Relation.EQUAL_TO),
            topCandidates.scoreDocs);

    // explicitly mark results as exhausted, as current limits on numCandidates
    // guarantee that all results fit in a single batch
    return new QueryInfo(result, true);
  }

  private TopDocs topCandidates(
      LuceneIndexSearcher indexSearcher, TopScoreDocCollectorManager collectorManager)
      throws IOException {
    TopDocs topCandidates = indexSearcher.search(this.luceneQuery, collectorManager);

    // Surface a timeout to the user if the deadline is exceeded
    DeadlineQueryTimeout.throwIfExceeded(Optional.ofNullable(indexSearcher.getTimeout()));

    if (this.rescorer.isEmpty()) {
      return topCandidates;
    }

    Optional<WrappedToParentBlockJoinQuery> blockJoinQuery =
        extractBlockJoinQuery(this.luceneQuery);

    Query knnQuery;
    Optional<BitSetProducer> parentFilter;

    if (blockJoinQuery.isPresent()) {
      Query childQuery = blockJoinQuery.get().getChildQuery();
      knnQuery = (childQuery instanceof WrappedKnnQuery wkq) ? wkq.getQuery() : childQuery;
      parentFilter = Optional.of(blockJoinQuery.get().getParentsFilter());
    } else {
      knnQuery = extractKnnQuery(this.luceneQuery);
      parentFilter = Optional.empty();
    }

    return this.rescorer
        .get()
        .rescore(
            indexSearcher,
            topCandidates,
            (ApproximateVectorSearchCriteria) this.criteria,
            BinaryQuantizedRescoreQuery.from(knnQuery),
            parentFilter);
  }

  /**
   * Extracts the underlying KNN vector query from the query tree. Handles three shapes: a bare
   * {@code KnnFloatVectorQuery}/{@code KnnByteVectorQuery}, a {@link WrappedKnnQuery} wrapping one,
   * or a {@link BooleanQuery} whose MUST clause is a KNN vector query.
   *
   * @param query the top-level query to extract from
   * @return the extracted {@code KnnFloatVectorQuery} or {@code KnnByteVectorQuery}
   */
  @VisibleForTesting
  static Query extractKnnQuery(Query query) {
    Query unwrapped = (query instanceof WrappedKnnQuery wkq) ? wkq.getQuery() : query;
    if (isKnnVectorQuery(unwrapped)) {
      return unwrapped;
    }
    if (unwrapped instanceof BooleanQuery bq) {
      for (BooleanClause clause : bq.clauses()) {
        if (clause.occur() == BooleanClause.Occur.MUST && isKnnVectorQuery(clause.query())) {
          return clause.query();
        }
      }
    }
    return Check.unreachable(
        "Unrecognized flat vector query shape — expected KnnFloatVectorQuery or"
            + " KnnByteVectorQuery");
  }

  private static boolean isKnnVectorQuery(Query query) {
    return query instanceof KnnFloatVectorQuery || query instanceof KnnByteVectorQuery;
  }

  @VisibleForTesting
  static Optional<WrappedToParentBlockJoinQuery> extractBlockJoinQuery(Query query) {
    Query unwrapped = (query instanceof WrappedKnnQuery wkq) ? wkq.getQuery() : query;
    if (unwrapped instanceof WrappedToParentBlockJoinQuery bjq) {
      return Optional.of(bjq);
    }
    if (unwrapped instanceof BooleanQuery bq) {
      for (BooleanClause clause : bq.clauses()) {
        if (clause.query() instanceof WrappedToParentBlockJoinQuery bjq) {
          return Optional.of(bjq);
        }
      }
    }
    return Optional.empty();
  }

  @Override
  public TopDocs getMoreTopDocs(
      LuceneIndexSearcherReference searcherReference, ScoreDoc lastScoreDoc, int batchSize) {
    // the cursor is always marked as exhausted after the initial search,
    // so the getMore call is unexpected
    return Check.unreachable("getMoreTopDocs should not be called for vector search");
  }

  @VisibleForTesting
  public Query getLuceneQuery() {
    return this.luceneQuery;
  }
}
