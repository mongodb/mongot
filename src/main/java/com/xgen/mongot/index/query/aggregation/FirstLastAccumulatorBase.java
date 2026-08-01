package com.xgen.mongot.index.query.aggregation;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.bson.BsonValue;

/**
 * Machinery shared by {@link FirstAccumulator} and {@link LastAccumulator}.
 *
 * <p>Everything here is direction-free. A subclass says only which sort to collect under and which
 * result type to build; this class routes documents to the collectors and reads the winners'
 * values.
 *
 * <p>Values are read once per bucket in {@link #buildResults}, not per document: a bucket visits
 * many documents and keeps one.
 */
abstract class FirstLastAccumulatorBase extends Accumulator {

  private final ResolvedTokenField field;
  private final BucketWinners winners;
  private final Optional<Sort> mergeSort;
  private final int partitionId;

  /**
   * Resolves {@code path} and sets up one collector per bucket.
   *
   * @param collectorSort the sort each bucket's collector runs under, oriented for this operator
   * @param mergeSort the sort slice winners are merged under, or empty when the query had no sort
   *     and the merge falls back to partition and doc order
   */
  FirstLastAccumulatorBase(
      String name,
      FieldPath path,
      AggregationContext context,
      Optional<Sort> mergeSort,
      Sort collectorSort) {
    super(name, List.of(), context);
    this.field =
        context
            .getFieldResolver()
            .orElseThrow(
                () -> new IllegalStateException("Aggregation field resolver is not configured"))
            .resolveToken(path);
    this.winners = new BucketWinners(collectorSort);
    this.mergeSort = mergeSort;
    this.partitionId = context.getPartitionId();
  }

  protected final Optional<Sort> mergeSort() {
    return this.mergeSort;
  }

  /** Wraps this bucket's winner, if it found one, in the result type for this operator. */
  protected abstract AggregationResult createResult(
      Optional<FirstLastAggregationResultBase.Candidate> candidate);

  @Override
  public final LeafBucketCollector getLeafCollector(AggregationContext context) throws IOException {
    BucketWinners.LeafWinners leafWinners = this.winners.forLeaf(context.getLeafReaderContext());

    return new LeafBucketCollector() {
      @Override
      public void collect(int doc, long owningBucketOrd) throws IOException {
        // Deliberately ignores isMultiValued: a fanned-out document belongs to each of its
        // buckets, so returning its value in all of them is what MQL does after $unwind. sum/avg
        // skip it to keep totals adding up (CLOUDP-421371); no arithmetic here to distort.
        leafWinners.collect(doc, Math.toIntExact(owningBucketOrd));
      }
    };
  }

  @Override
  protected final List<AggregationResult> buildResults(LongArray ordsToCollect) throws IOException {
    // Read each ordinal's winner exactly once: doing so drains the state that produced it.
    List<Optional<FieldDoc>> winningDocs = new ArrayList<>(ordsToCollect.size());
    for (int i = 0; i < ordsToCollect.size(); i++) {
      winningDocs.add(this.winners.winner(Math.toIntExact(ordsToCollect.get(i))));
    }

    List<Optional<BsonValue>> values = fetchWinningValues(winningDocs);
    return IntStream.range(0, winningDocs.size())
        .mapToObj(
            i ->
                createResult(
                    winningDocs
                        .get(i)
                        .map(
                            winner ->
                                new FirstLastAggregationResultBase.Candidate(
                                    this.partitionId, winner, values.get(i)))))
        .toList();
  }

  /** A bucket's winning document and the result slot its value belongs in. */
  private record PendingFetch(int globalDocId, int resultIndex) {}

  /**
   * Reads each bucket winner's value, in doc-id order with one fetcher per segment.
   *
   * <p>The read is a loop, not a stream: every step depends on the segment the previous one landed
   * in, and the reads throw.
   */
  private List<Optional<BsonValue>> fetchWinningValues(List<Optional<FieldDoc>> winningDocs)
      throws IOException {
    List<PendingFetch> pending =
        IntStream.range(0, winningDocs.size())
            .boxed()
            .flatMap(
                i -> winningDocs.get(i).stream().map(winner -> new PendingFetch(winner.doc, i)))
            .sorted(Comparator.comparingInt(PendingFetch::globalDocId))
            .toList();

    List<Optional<BsonValue>> values =
        new ArrayList<>(Collections.nCopies(winningDocs.size(), Optional.empty()));
    if (pending.isEmpty()) {
      return values;
    }

    List<LeafReaderContext> leaves = this.context.getIndexReader().leaves();
    @Var int currentLeafIndex = ReaderUtil.subIndex(pending.get(0).globalDocId(), leaves);
    @Var
    FieldValueFetcher fetcher = FieldValueFetcher.create(this.field, leaves.get(currentLeafIndex));
    for (PendingFetch entry : pending) {
      int leafIndex = ReaderUtil.subIndex(entry.globalDocId(), leaves);
      if (leafIndex != currentLeafIndex) {
        currentLeafIndex = leafIndex;
        fetcher = FieldValueFetcher.create(this.field, leaves.get(leafIndex));
      }
      values.set(
          entry.resultIndex(), fetcher.fetch(entry.globalDocId() - leaves.get(leafIndex).docBase));
    }
    return values;
  }
}
