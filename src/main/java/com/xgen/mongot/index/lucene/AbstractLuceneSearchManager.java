package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.sort.SequenceToken;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopScoreDocCollectorManager;

abstract class AbstractLuceneSearchManager<T> implements LuceneSearchManager<T> {

  private final Query luceneQuery;
  private final Optional<Sort> luceneSort;
  private final Optional<SequenceToken> searchAfter;

  AbstractLuceneSearchManager(
      Query luceneQuery, Optional<Sort> luceneSort, Optional<SequenceToken> searchAfter) {
    this.luceneQuery = luceneQuery;
    this.luceneSort = luceneSort;
    this.searchAfter = searchAfter;
  }

  @Override
  public TopDocs getMoreTopDocs(
      LuceneIndexSearcherReference searcherReference, ScoreDoc lastScoreDoc, int batchSize)
      throws IOException {

    TopDocs topDocs =
        this.luceneSort.isPresent()
            ? searcherReference
                .getIndexSearcher()
                .searchAfter(lastScoreDoc, this.luceneQuery, batchSize, this.luceneSort.get(), true)
            : searcherReference
                .getIndexSearcher()
                .searchAfter(lastScoreDoc, this.luceneQuery, batchSize);

    maybePopulateScores(searcherReference.getIndexSearcher(), topDocs.scoreDocs);
    return topDocs;
  }

  /**
   * Create a {@link CollectorManager} that will use concurrent segment search to find the top
   * {@code batchSize} docs matching the query and estimate hits up to {@code hitThreshold}.
   *
   * @param batchSize the maximum number of doc IDs to return
   * @param hitsThreshold the threshold beyond which we stop estimating hit counts accurately
   */
  protected CollectorManager<? extends TopDocsCollector<?>, ? extends TopDocs>
      createCollectorManager(int batchSize, int hitsThreshold) {
    FieldDoc after = this.searchAfter.map(SequenceToken::fieldDoc).orElse(null);
    return this.luceneSort.isPresent()
        ? new TopFieldCollectorManager(this.luceneSort.get(), batchSize, after, hitsThreshold)
        : new TopScoreDocCollectorManager(batchSize, after, hitsThreshold);
  }

  /**
   * Populates score values in each {@link ScoreDoc} if they are missing and required.
   *
   * <p>{@link org.apache.lucene.search.TopFieldCollector} does not compute scores unless the sort
   * criteria requires them. This saves time, but downstream aggregation stages may rely on
   * $searchScore, in which case we need to explicitly populate them here.
   */
  protected void maybePopulateScores(LuceneIndexSearcher searcher, ScoreDoc[] scoreDocs)
      throws IOException {
    if (this.luceneSort.isPresent()) {
      TopFieldCollector.populateScores(scoreDocs, searcher, this.getLuceneQuery());
    }
  }

  public Query getLuceneQuery() {
    return this.luceneQuery;
  }
}
