package com.xgen.mongot.index.lucene.quantization;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.FluentLogger;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.jetbrains.annotations.Nullable;

/**
 * Performs two-stage rescoring of {@link TopDocs} provided after HNSW graph traversal based on
 * binary quantized vectors.
 *
 * <p>Supports both flat and nested (parent-child block join) vector indexes. For nested indexes,
 * provide a {@link BitSetProducer} identifying parent documents; each parent is rescored by taking
 * the maximum similarity across its child vectors.
 */
public class BinaryQuantizedVectorRescorer {

  private final Optional<NamedExecutorService> executor;

  public BinaryQuantizedVectorRescorer(Optional<NamedExecutorService> executor) {
    this.executor = executor;
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public TopDocs rescore(
      IndexSearcher indexSearcher,
      TopDocs topDocs,
      ApproximateVectorSearchCriteria searchCriteria,
      BinaryQuantizedRescoreQuery luceneQuery)
      throws IOException {
    return rescore(indexSearcher, topDocs, searchCriteria, luceneQuery, Optional.empty());
  }

  /**
   * Rescores vector search candidates using two-stage binary quantization rescoring, supporting
   * both flat and nested (parent-child block join) indexes.
   *
   * <p>For nested indexes, supply a {@code parentFilter} identifying parent documents. Each parent
   * is scored by the maximum similarity across its child vectors; the vector iterator advances
   * through children in ascending doc-ID order, so {@code topDocs} must contain parent doc IDs (as
   * returned by a {@code ToParentBlockJoinQuery}).
   *
   * @param indexSearcher the index searcher for accessing segment data
   * @param topDocs the initial ANN candidates; for nested indexes these are parent doc IDs
   * @param searchCriteria provides {@code limit} and {@code numCandidates} for rescoring bounds
   * @param luceneQuery the wrapped version of a KNN query, used to retrieve the query vector and
   *     field name
   * @param parentFilter for nested indexes, a {@link BitSetProducer} identifying parent documents
   *     per segment; {@link Optional#empty()} for flat indexes
   * @return rescored and truncated {@link TopDocs}, ordered by descending similarity
   */
  public TopDocs rescore(
      IndexSearcher indexSearcher,
      TopDocs topDocs,
      ApproximateVectorSearchCriteria searchCriteria,
      BinaryQuantizedRescoreQuery luceneQuery,
      Optional<BitSetProducer> parentFilter)
      throws IOException {

    if (topDocs.scoreDocs.length == 0) {
      return topDocs;
    }

    // Stage 1 - rescore query against the oversampled dequantized binary vectors
    ApproximateRescorer approximateRescorer = new ApproximateRescorer(luceneQuery, parentFilter);
    TopDocs approximatelyRescored =
        approximateRescorer.rescore(indexSearcher, topDocs, searchCriteria.resolveNumCandidates());

    // Stage 2 - rescore query against full fidelity vectors for the limited subset of docs
    FullFidelityRescorer fullFidelityRescorer =
        new FullFidelityRescorer(luceneQuery, this.executor, parentFilter);

    // Use geometric mean between the limit and numCandidates to get a number in between with a bias
    // towards the lower one (limit). Using a larger number improves recall, but affects latency
    // as accessing full fidelity vectors could result in page faults
    int fullFidelityLimit =
        (int) Math.sqrt(searchCriteria.limit() * searchCriteria.resolveNumCandidates());
    TopDocs fullFidelityRescoredDocs =
        fullFidelityRescorer.rescore(indexSearcher, approximatelyRescored, fullFidelityLimit);

    if (fullFidelityRescoredDocs.scoreDocs.length > searchCriteria.limit()) {
      // Truncate the final result result to limit
      fullFidelityRescoredDocs.scoreDocs =
          Arrays.copyOf(fullFidelityRescoredDocs.scoreDocs, searchCriteria.limit());
    }

    luceneQuery.examineResultsAfterRescoring(fullFidelityRescoredDocs);

    return fullFidelityRescoredDocs;
  }

  private static void scoreSegment(
      LeafReaderContext segment, VectorScorer scorer, ScoreDoc[] scoreDocs, int start, int end)
      throws IOException {

    for (int i = start; i < end; i++) {
      int target = scoreDocs[i].doc - segment.docBase;
      if (scorer.iterator().advance(target) != target) {
        throw new IllegalArgumentException("Doc " + scoreDocs[i].doc + " doesn't have a vector");
      }

      scoreDocs[i].score = scorer.score();
    }
  }

  /**
   * Rescores parent documents in a nested index by iterating each parent's child docs and taking
   * the maximum child similarity score. Parents are processed in ascending doc-ID order so the
   * forward-only vector iterator advances correctly across the whole segment.
   */
  private static void scoreNestedSegment(
      LeafReaderContext segment,
      VectorScorer scorer,
      ScoreDoc[] scoreDocs,
      int start,
      int end,
      BitSetProducer parentFilter,
      @Nullable BitSet childFilterBitSet)
      throws IOException {

    BitSet parentBitSet = parentFilter.getBitSet(segment);
    if (parentBitSet == null) {
      // getBitSet returns null when the parent filter matches no documents in this segment.
      // In that case ToParentBlockJoinQuery produces no hits for this segment either, so
      // scoreDocs[start..end] contains no entries from it and there is nothing to rescore.
      return;
    }

    for (int i = start; i < end; i++) {
      int localParentDoc = scoreDocs[i].doc - segment.docBase;
      int prevParent = (localParentDoc > 0) ? parentBitSet.prevSetBit(localParentDoc - 1) : -1;
      int firstChild = prevParent + 1;

      if (firstChild >= localParentDoc) {
        // No child documents exist between the previous parent and this parent in this segment.
        // This should not occur in a well-formed block join index (every parent must have at least
        // one child). Assign score 0 (the minimum for all VectorSimilarityFunction
        // implementations) so this parent does not unfairly compete with correctly rescored
        // parents that had their ANN score replaced by maxScore.
        scoreDocs[i].score = 0;
        continue;
      }

      // All Lucene VectorSimilarityFunction implementations return non-negative scores, so 0 is a
      // safe initializer and represents the worst possible similarity value for a parent.
      @Var float maxScore = 0;
      @Var int docId = firstChild;
      while (docId < localParentDoc) {
        int found =
            (scorer.iterator().docID() >= docId)
                ? scorer.iterator().docID()
                : scorer.iterator().advance(docId);
        if (found == NO_MORE_DOCS || found >= localParentDoc) {
          break;
        }
        if (childFilterBitSet == null || childFilterBitSet.get(found)) {
          maxScore = Math.max(maxScore, scorer.score());
        }
        docId = found + 1;
      }
      scoreDocs[i].score = maxScore;
    }
  }

  @Nullable
  private static Weight createChildFilterWeight(
      IndexSearcher searcher, BinaryQuantizedRescoreQuery query, boolean hasParentFilter)
      throws IOException {
    if (!hasParentFilter) {
      return null;
    }
    @Nullable Query childFilter = query.getFilter();
    if (childFilter == null) {
      return null;
    }
    return searcher.createWeight(searcher.rewrite(childFilter), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
  }

  private static BitSet computeChildFilterBitSet(
      Weight childFilterWeight, LeafReaderContext segment) throws IOException {
    Scorer filterScorer = childFilterWeight.scorer(segment);
    return filterScorer != null
        ? BitSet.of(filterScorer.iterator(), segment.reader().maxDoc())
        : new FixedBitSet(segment.reader().maxDoc());
  }

  private static FieldInfo getFieldInfo(LeafReader context, BinaryQuantizedRescoreQuery query) {
    return context.getFieldInfos().fieldInfo(query.getField());
  }

  private static TopDocs deepCopy(TopDocs topDocs, int topN) {
    ScoreDoc[] arrayCopy = new ScoreDoc[Math.min(topDocs.scoreDocs.length, topN)];
    for (int i = 0; i < arrayCopy.length; ++i) {
      ScoreDoc s = topDocs.scoreDocs[i];
      arrayCopy[i] = new ScoreDoc(s.doc, s.score, s.shardIndex);
    }
    return new TopDocs(new TotalHits(arrayCopy.length, TotalHits.Relation.EQUAL_TO), arrayCopy);
  }

  @VisibleForTesting
  static class ApproximateRescorer extends Rescorer {

    private static final FluentLogger flogger = FluentLogger.forEnclosingClass();

    private final BinaryQuantizedRescoreQuery query;
    private final Optional<BitSetProducer> parentFilter;

    public ApproximateRescorer(BinaryQuantizedRescoreQuery query) {
      this(query, Optional.empty());
    }

    public ApproximateRescorer(
        BinaryQuantizedRescoreQuery query, Optional<BitSetProducer> parentFilter) {
      this.query = query;
      this.parentFilter = parentFilter;
    }

    @Override
    public TopDocs rescore(IndexSearcher searcher, TopDocs topDocs, int topN) throws IOException {

      // truncate topDocs to topN
      TopDocs topDocsCopy = deepCopy(topDocs, topN);
      int limit = topDocsCopy.scoreDocs.length;

      // sort for the sequential access
      Arrays.sort(topDocsCopy.scoreDocs, Comparator.comparingInt(hit -> hit.doc));
      List<LeafReaderContext> segments = searcher.getIndexReader().leaves();

      @Nullable
      Weight childFilterWeight =
          createChildFilterWeight(searcher, this.query, this.parentFilter.isPresent());

      @Var int i = 0;
      while (i < limit) {

        int start = i;
        LeafReaderContext segment =
            segments.get(ReaderUtil.subIndex(topDocsCopy.scoreDocs[i].doc, segments));
        int maxDoc = segment.docBase + segment.reader().maxDoc();

        for (i = i + 1; i < limit; i++) {
          if (topDocsCopy.scoreDocs[i].doc >= maxDoc) {
            break;
          }
        }
        int end = i;

        Optional<QuantizedVectorsReader> vectorsReader = getVectorsReader(segment.reader());

        if (vectorsReader.isEmpty()) {
          flogger.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
              "Skipped rescoring because vectors reader was not found for the segment");
          return topDocs; // skip re-scoring if vectors reader was not found
        }

        if (vectorsReader.get().getQuantizedVectorValues(this.query.getField()) == null) {
          flogger.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
              "Skipped rescoring because quantized vector values for %s were null",
              this.query.getField());
          return topDocs; // skip re-scoring if vector values were not found
        }

        VectorScorer scorer =
            this.query.approximateScorer(
                vectorsReader.get().getQuantizedVectorValues(this.query.getField()),
                getFieldInfo(segment.reader(), this.query).getVectorSimilarityFunction());

        if (this.parentFilter.isPresent()) {
          @Nullable
          BitSet childFilterBitSet =
              childFilterWeight != null
                  ? computeChildFilterBitSet(childFilterWeight, segment)
                  : null;
          scoreNestedSegment(
              segment,
              scorer,
              topDocsCopy.scoreDocs,
              start,
              end,
              this.parentFilter.get(),
              childFilterBitSet);
        } else {
          scoreSegment(segment, scorer, topDocsCopy.scoreDocs, start, end);
        }
      }

      // sort by score
      Arrays.sort(topDocsCopy.scoreDocs, (a, b) -> -Float.compare(a.score, b.score));

      return topDocsCopy;
    }

    @Override
    public Explanation explain(
        IndexSearcher searcher, Explanation firstPassExplanation, int docID) {
      throw new UnsupportedOperationException();
    }

    private Optional<QuantizedVectorsReader> getVectorsReader(LeafReader reader) {

      if (!(reader instanceof CodecReader codecReader)) {
        return Optional.empty();
      }

      @Var KnnVectorsReader vectorsReader = codecReader.getVectorReader();

      if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
        vectorsReader = fieldsReader.getFieldReader(this.query.getField());
      }

      if (vectorsReader instanceof QuantizedVectorsReader quantizedVectorsReader) {
        return Optional.of(quantizedVectorsReader);
      }

      return Optional.empty();
    }
  }

  @VisibleForTesting
  static class FullFidelityRescorer extends Rescorer {

    private final BinaryQuantizedRescoreQuery query;
    private final Optional<TaskExecutor> executor;
    private final Optional<BitSetProducer> parentFilter;

    public FullFidelityRescorer(
        BinaryQuantizedRescoreQuery query, Optional<NamedExecutorService> executorService) {
      this(query, executorService, Optional.empty());
    }

    public FullFidelityRescorer(
        BinaryQuantizedRescoreQuery query,
        Optional<NamedExecutorService> executorService,
        Optional<BitSetProducer> parentFilter) {
      this.query = query;
      this.executor = executorService.map(TaskExecutor::new);
      this.parentFilter = parentFilter;
    }

    @Override
    public TopDocs rescore(IndexSearcher searcher, TopDocs topDocs, int topN) throws IOException {

      // truncate topDocs to the specified limit
      TopDocs topDocsCopy = deepCopy(topDocs, topN);
      int limit = topDocsCopy.scoreDocs.length;
      // sort for the sequential access
      Arrays.sort(topDocsCopy.scoreDocs, Comparator.comparingInt(hit -> hit.doc));

      List<LeafReaderContext> segments = searcher.getIndexReader().leaves();
      List<Callable<Void>> tasks = new ArrayList<>();

      @Nullable
      Weight childFilterWeight =
          createChildFilterWeight(searcher, this.query, this.parentFilter.isPresent());

      @Var int i = 0;

      while (i < limit) {

        int start = i;
        LeafReaderContext segment =
            segments.get(ReaderUtil.subIndex(topDocsCopy.scoreDocs[i].doc, segments));
        int maxDoc = segment.docBase + segment.reader().maxDoc();

        for (i = i + 1; i < limit; i++) {
          if (topDocsCopy.scoreDocs[i].doc >= maxDoc) {
            break;
          }
        }
        int end = i;

        VectorSimilarityFunction sim =
            getFieldInfo(segment.reader(), this.query).getVectorSimilarityFunction();
        VectorScorer scorer = this.query.fullFidelityScorer(segment.reader(), sim);

        if (this.parentFilter.isPresent()) {
          BitSetProducer filter = this.parentFilter.get();
          @Nullable
          BitSet childFilterBitSet =
              childFilterWeight != null
                  ? computeChildFilterBitSet(childFilterWeight, segment)
                  : null;
          tasks.add(
              () -> {
                scoreNestedSegment(
                    segment, scorer, topDocsCopy.scoreDocs, start, end, filter, childFilterBitSet);
                return null;
              });
        } else {
          tasks.add(
              () -> {
                scoreSegment(segment, scorer, topDocsCopy.scoreDocs, start, end);
                return null;
              });
        }
      }

      if (this.executor.isPresent()) {
        // execute tasks and wait for completion. if exceptions happen,
        // they are accumulated across tasks and thrown and the end
        this.executor.get().invokeAll(tasks);
      } else {
        invokeAllSequentially(tasks);
      }

      // sort by score
      Arrays.sort(topDocsCopy.scoreDocs, (a, b) -> -Float.compare(a.score, b.score));

      return topDocsCopy;
    }

    @Override
    public Explanation explain(
        IndexSearcher searcher, Explanation firstPassExplanation, int docID) {
      throw new UnsupportedOperationException();
    }

    private void invokeAllSequentially(List<Callable<Void>> tasks) throws IOException {
      for (Callable<Void> task : tasks) {
        try {
          task.call();
        } catch (IOException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
