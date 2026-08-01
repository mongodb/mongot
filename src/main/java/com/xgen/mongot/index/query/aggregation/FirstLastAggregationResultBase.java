package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.util.Check;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;

/**
 * Leaf result shared by the {@code first} and {@code last} aggregation operators.
 *
 * <p>The result carries the winning document alongside its value, because merging the per-slice
 * results of one shard has to re-run the comparison the accumulator did: the winner of the merged
 * bucket is the winner among the slice winners.
 *
 * <p>That merge is direction-free. Each operator collects under a sort oriented its own way -- the
 * query's sort for {@code first}, its inverse for {@code last} -- so merging is always a matter of
 * taking the top document under {@link #collectorSort}, which is what {@link TopDocs#merge} does.
 */
public abstract sealed class FirstLastAggregationResultBase extends MetricAggregationResult
    permits FirstAggregationResult, LastAggregationResult {

  /**
   * A bucket's winning document: the partition it came from, the document itself with the sort key
   * its collector materialized, and the value read from it.
   */
  record Candidate(int partitionId, FieldDoc doc, Optional<BsonValue> value) {}

  /**
   * The sort the winner was collected under, already oriented for this operator; empty when the
   * query had no sort and encounter order alone ordered the bucket.
   */
  private final Optional<Sort> collectorSort;

  private final Optional<Candidate> candidate;

  FirstLastAggregationResultBase(
      String name, Optional<Sort> collectorSort, Optional<Candidate> candidate) {
    super(name);
    this.collectorSort = collectorSort;
    this.candidate = candidate;
  }

  Optional<Sort> collectorSort() {
    return this.collectorSort;
  }

  Optional<Candidate> candidate() {
    return this.candidate;
  }

  @Override
  public final AggregationResult applyLimit() {
    return this;
  }

  /**
   * A bucket whose winning document has no value at the path reports {@code null}, as does a bucket
   * that collected no document at all.
   *
   * <p>Values come from stored source, so a multi-valued path yields the array as stored rather
   * than a single element or {@code null}.
   */
  @Override
  public final BsonValue toStandaloneBson() {
    return this.candidate.flatMap(Candidate::value).orElse(BsonNull.VALUE);
  }

  @Override
  public final BsonDocument toIntermediateMetricPartials() {
    // TODO(CLOUDP-425978 follow-up): carry the winning value and its sort key as partials so
    // mongos can merge across shards with $top/$bottom. Deferred with buildShardMergePipeline in
    // First/LastAggregationSpec so the wire format and the merge pipeline land as one contract.
    throw new IllegalStateException(
        "Sharded merge for 'first'/'last' aggregations is not implemented yet");
  }

  /**
   * Merges bucket winners by handing them back to Lucene.
   *
   * <p>Subclasses differ only in {@link #encounterOrder}, which settles winners that {@link
   * #collectorSort} leaves tied, and which is the whole comparison when there is no sort.
   */
  abstract static class Builder implements AggregationResultBuilder {

    private final Optional<Sort> collectorSort;
    private final List<Candidate> candidates = new ArrayList<>();

    Builder(Optional<Sort> collectorSort) {
      this.collectorSort = collectorSort;
    }

    /**
     * Orders candidates so the one this operator prefers on a tie comes first.
     *
     * <p>Needed even with no sort at all. Slice and partition winners reach the builder in fold
     * order, so "the first document" is not simply the first one added -- it has to be identified,
     * and this is the comparison that does it. Doc ids are comparable only within a partition, so
     * partition leads, mirroring the {@code (shardIndex, doc)} tiebreak {@link
     * com.xgen.mongot.index.lucene.Comparators.CustomSortComparator} uses. It runs once per bucket
     * at merge time, not per document.
     */
    protected abstract Comparator<Candidate> encounterOrder();

    /** Builds this operator's result around the merged winner. */
    protected abstract AggregationResult create(
        Optional<Sort> collectorSort, Optional<Candidate> winner);

    @Override
    public final void add(AggregationResult other) {
      Check.instanceOf(other, FirstLastAggregationResultBase.class)
          .candidate()
          .ifPresent(this.candidates::add);
    }

    @Override
    public final AggregationResult build() {
      return create(this.collectorSort, pick());
    }

    private Optional<Candidate> pick() {
      if (this.candidates.isEmpty()) {
        return Optional.empty();
      }

      // Ordered first so that this operator's preferred candidate sits at the lower index, which
      // is what both branches below then lean on to settle a tie.
      List<Candidate> ordered = new ArrayList<>(this.candidates);
      ordered.sort(encounterOrder());

      return Optional.of(
          this.collectorSort.map(sort -> mergeTop(sort, ordered)).orElseGet(ordered::getFirst));
    }

    /**
     * The top candidate under {@code sort}, as Lucene's own cross-shard merge computes it.
     *
     * <p>{@link TopDocs#merge} breaks a tie on {@code shardIndex}, so each candidate is given its
     * position in the already-ordered list: the tiebreak then falls out as encounter order without
     * this class comparing any sort values itself.
     */
    private static Candidate mergeTop(Sort sort, List<Candidate> ordered) {
      TopFieldDocs[] shards = new TopFieldDocs[ordered.size()];
      for (int i = 0; i < ordered.size(); i++) {
        FieldDoc doc = ordered.get(i).doc();
        doc.shardIndex = i;
        shards[i] =
            new TopFieldDocs(
                new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {doc},
                sort.getSort());
      }
      return ordered.get(TopDocs.merge(sort, 1, shards).scoreDocs[0].shardIndex);
    }

    /** Partition before doc id, since doc ids are comparable only within a partition. */
    static Comparator<Candidate> ascendingEncounterOrder() {
      return Comparator.<Candidate>comparingInt(Candidate::partitionId)
          .thenComparingInt(candidate -> candidate.doc().doc);
    }
  }
}
