package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.Check.checkState;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.index.BatchProducer;
import com.xgen.mongot.index.CountResult;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.aggregation.AggregationResult;
import com.xgen.mongot.index.query.aggregation.AggregationResultBuilder;
import com.xgen.mongot.index.query.counts.Count;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.BsonArrayBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.util.IOUtils;
import org.bson.BsonArray;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

/**
 * Streams {@link BatchProducer} batches for aggregation results of one {@link AggregationResult}
 * tree — either a single partition's result, or the tree merged across all partitions of a
 * partitioned index via {@link #merge}.
 *
 * <p>For the sharded {@code intermediateQuery} path, {@code getNextBatch()} emits a legacy count
 * document ({@code type: "count"}) when it fits within {@code resultsSizeLimit}, followed by flat
 * aggregation intermediate documents from {@link AggregationResult#toIntermediateBson()} across
 * batches, respecting the per-batch BSON size limit (same semantics as {@link
 * LuceneFacetCollectorMetaBatchProducer}).
 *
 * <p>For the non-sharded path, {@link #getMetaResultsAndClose} drains the nested result to {@link
 * MetaResults} instead; the two purposes are mutually exclusive per instance.
 */
final class AggregationIntermediateBatchProducer implements BatchProducer {

  private final AggregationResult aggregationResult;
  private final long totalHits;

  /**
   * Flattened wire rows, populated by {@link #execute} and empty until then.
   *
   * <p>Only the sharded path ever fills this in, because only sharded execution calls {@link
   * #execute}. The non-sharded partitioned path builds a producer per partition and drains it
   * through {@link #getMetaResultsAndClose}, merging nested trees directly. Flattening is therefore
   * deferred out of the constructor, so that path neither builds nor discards per-partition wire
   * rows it has no use for.
   *
   * <p>Volatile because a producer is handed to the cursor thread that reads its batches.
   */
  private volatile Optional<BsonArray> intermediateDocs;

  private int nextDocIndex;
  private boolean countProduced;
  private boolean closed;

  AggregationIntermediateBatchProducer(AggregationResult aggregationResult, long totalHits) {
    this.aggregationResult = aggregationResult;
    this.intermediateDocs = Optional.empty();
    this.totalHits = totalHits;
    this.nextDocIndex = 0;
    this.countProduced = false;
    this.closed = false;
  }

  /**
   * Merges many per-partition producers into one, folding their result trees through {@link
   * AggregationResultBuilder} (hash-based state carried across the whole fold: no per-merge map
   * rebuilds or sorting; buckets are ordered once at read time) and summing total hits. The input
   * producers are consumed: every one is closed and must not be used afterwards, including when the
   * fold itself fails partway (results of differing shape make {@link AggregationResultBuilder#add}
   * throw), so an early failure never leaves the rest unclosed.
   *
   * <p>The sequential fold costs O(P) merges on the query thread. To parallelize later, fold each
   * slice of partitions through its own {@link AggregationResultBuilder} and combine the built
   * results in a parent builder. Not done now: P is small and the fold is cheap relative to the
   * per-partition searches that precede it. TODO(CLOUDP-426967): Implement parallelized merging.
   *
   * <p>{@code producers} must be quiesced: the caller's per-partition fan-out has to have joined
   * before calling. The list is read and iterated without holding a lock, so a caller that hands
   * over a still-growing {@link java.util.Collections#synchronizedList} (whose iterator does not
   * self-synchronize) would risk a {@code ConcurrentModificationException}.
   */
  static AggregationIntermediateBatchProducer merge(
      List<AggregationIntermediateBatchProducer> producers) throws IOException {
    Check.argNotEmpty(producers, "producers");
    AggregationResultBuilder resultBuilder = producers.get(0).aggregationResult.toBuilder();
    @Var long totalHits = 0;
    @Var boolean folded = false;
    try {
      for (int i = 0; i < producers.size(); i++) {
        try (var indexPartitionResourceManager = Explain.maybeEnterIndexPartitionQueryContext(i)) {
          AggregationIntermediateBatchProducer producer = producers.get(i);
          totalHits += producer.totalHits;
          resultBuilder.add(producer.aggregationResult);
        }
      }
      folded = true;
    } finally {
      if (folded) {
        // Closes all, rethrowing the first failure with the rest suppressed.
        IOUtils.close(producers);
      } else {
        // The fold already failed. A throwing finally would replace that exception outright (Java
        // discards it, it is not even suppressed), so swallow close failures to keep the root
        // cause.
        IOUtils.closeWhileHandlingException(producers);
      }
    }
    return new AggregationIntermediateBatchProducer(resultBuilder.build(), totalHits);
  }

  /**
   * Drains the nested aggregation result to the format of MetaResults, and then closes this batch
   * producer.
   *
   * <p>Only called from the multi-partitioned reader ({@code MultiLuceneSearchIndexReader}) on the
   * non-sharded path. Full cardinality is returned on purpose: the final per-level bucket limits
   * (e.g. numBuckets, CLOUDP-419089) are applied exactly once at the non-sharded terminal
   * (SearchCommand.getBatch -> MetaResults#applyAggregationLimit), which this merged MetaResults
   * flows through like a single-partition one. The sharded path (getNextBatch) also streams full
   * cardinality because top-N-by-count is not associative across shard merges.
   */
  MetaResults getMetaResultsAndClose(Count.Type countType) {
    CountResult countResult = LuceneFacetResultUtil.getCount(this.totalHits, countType);
    this.close();
    return new MetaResults(countResult, Optional.empty(), Optional.of(this.aggregationResult));
  }

  /**
   * Flattens the result tree to wire rows, once per producer. {@link
   * com.xgen.mongot.cursor.MongotCursor} calls this before every {@code getNextBatch}, so later
   * calls are no-ops and {@link #nextDocIndex} keeps indexing into the same rows.
   *
   * <p>Only reached on the sharded query execution path; see {@link #intermediateDocs}.
   */
  @Override
  public void execute(Bytes sizeLimit, BatchCursorOptions queryCursorOptions) throws IOException {
    if (this.intermediateDocs.isEmpty()) {
      this.intermediateDocs = Optional.of(this.aggregationResult.toIntermediateBson());
    }
  }

  @Override
  public BsonArray getNextBatch(Bytes resultsSizeLimit) throws IOException {
    checkState(!this.closed, "Cannot call getNextBatch() on closed BatchProducer.");
    if (isExhausted()) {
      return new BsonArray();
    }

    BsonArrayBuilder resultsBuilder = BsonArrayBuilder.withLimit(resultsSizeLimit);

    if (!this.countProduced) {
      if (!resultsBuilder.append(
          LuceneFacetCollectorMetaBatchProducer.getCountRawBsonDoc(this.totalHits))) {
        throw new IllegalStateException("Could not fit count bucket in meta buckets batch.");
      }
      this.countProduced = true;
    }

    BsonArray docs = getIntermediateDocs();
    while (this.nextDocIndex < docs.size()) {
      RawBsonDocument doc = toRawBsonDocument(docs.get(this.nextDocIndex));
      if (resultsBuilder.append(doc)) {
        this.nextDocIndex++;
      } else {
        return resultsBuilder.build();
      }
    }

    return resultsBuilder.build();
  }

  @Override
  public boolean isExhausted() {
    // The closed check has to come first, and has to be here at all: a producer can be closed
    // without ever being executed, and reading the rows in that state throws. See
    // AggregationIntermediateBatchProducerTest.close_doesNotFlattenTheResultTree.
    return this.closed || (this.countProduced && this.nextDocIndex >= getIntermediateDocs().size());
  }

  @Override
  public void close() {
    // Does not wind nextDocIndex forward to the row count, as it once did. The rows are not
    // guaranteed to exist here: getMetaResultsAndClose closes a producer the non-sharded path never
    // executed, so reading their size would throw. The closed short-circuit in isExhausted serves
    // the same purpose without needing them.
    this.closed = true;
    this.countProduced = true;
  }

  private BsonArray getIntermediateDocs() {
    return this.intermediateDocs.orElseThrow(
        () ->
            new IllegalStateException(
                "execute() must run before intermediate rows are read; only the sharded streaming"
                    + " path may read them."));
  }

  private static RawBsonDocument toRawBsonDocument(BsonValue value) {
    return BsonUtils.documentToRaw(value.asDocument());
  }
}
