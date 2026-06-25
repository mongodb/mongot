package com.xgen.mongot.index.lucene;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.index.BatchProducer;
import com.xgen.mongot.index.CountResult;
import com.xgen.mongot.index.FacetBucket;
import com.xgen.mongot.index.FacetInfo;
import com.xgen.mongot.index.IntermediateFacetBucket;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.collectors.FacetDefinition;
import com.xgen.mongot.index.query.counts.Count;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.BsonArrayBuilder;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonValue;

/**
 * A batch producer that takes in multiple LuceneFacetCollectorMetaBatchProducers, each from one sub
 * index, and merges their returned batches into one single batch based on merging the counts of the
 * same buckets. This class handles facet metadata results only, not the search results. This class
 * serves two mutual exclusive purposes: 1) getNextBatch() 2) Drains all results to `MetaResults`.
 */
public class FacetMergingBatchProducer implements BatchProducer {
  // Populated on first call to getNextBatch().
  private Optional<ArrayDeque<IntermediateFacetBucket>> facetBuckets;
  private final MergedResult mergedResult;
  private final List<LuceneFacetCollectorMetaBatchProducer> batchProducers;

  private boolean countProduced;

  private FacetMergingBatchProducer(
      MergedResult mergedResult, List<LuceneFacetCollectorMetaBatchProducer> batchProducers) {
    // TODO(CLOUDP-280897): If there are N index-partitions with M string facet buckets, we need to
    // store N*M entries in the memory. If this memory consumption becomes a concern, we can use one
    // FacetsCollector for all index partitions.
    this.mergedResult = mergedResult;
    this.batchProducers = batchProducers;
    this.countProduced = false;
    this.facetBuckets = Optional.empty();
  }

  /**
   * Creates this class from a list of LuceneFacetCollectorMetaBatchProducers. We use a factory
   * method to potentially throw an exception during the creation.
   */
  public static FacetMergingBatchProducer create(
      List<LuceneFacetCollectorMetaBatchProducer> batchProducers) throws IOException {
    Check.argNotEmpty(batchProducers, "batchProducers");
    MergedResult mergedResult = mergeToResult(batchProducers);
    return new FacetMergingBatchProducer(mergedResult, batchProducers);
  }

  // Sorts the string facet first by count in descending order, then by string key in ascending
  // order.
  static class StringFacetCountComparator implements Comparator<Map.Entry<BsonValue, Long>> {
    @Override
    public int compare(Map.Entry<BsonValue, Long> a, Map.Entry<BsonValue, Long> b) {
      int valueCompare = Long.compare(b.getValue(), a.getValue());
      if (valueCompare != 0) {
        return valueCompare;
      }
      return a.getKey().asString().getValue().compareTo(b.getKey().asString().getValue());
    }
  }

  // Sorts the string facet by string key in ascending order.
  static class StringFacetKeyComparator implements Comparator<Map.Entry<BsonValue, Long>> {
    @Override
    public int compare(Map.Entry<BsonValue, Long> a, Map.Entry<BsonValue, Long> b) {
      return a.getKey().asString().getValue().compareTo(b.getKey().asString().getValue());
    }
  }

  private static class MergedResult {
    public final Table<String, BsonValue, Long> table; // tuples of (facetName, bucket, count)
    public final long totalHits;

    public MergedResult(Table<String, BsonValue, Long> table, long totalHits) {
      this.table = table;
      this.totalHits = totalHits;
    }
  }

  /** Merged the input batchProducers to the format of MergedResult. */
  private static MergedResult mergeToResult(
      List<LuceneFacetCollectorMetaBatchProducer> batchProducers) throws IOException {
    // This is a full merge algorithm that requires every bucket from every index partition. This
    // will be
    // slow if there are too many distinct string facet values. There are two potential solutions:
    // 1. Use disk
    // 2. Limit number of buckets (e.g. 10000) for string facet.
    //  The HashBasedTable uses LinkedHashMap, so it guarantees the read iterator order == the
    // inserted order.
    Table<String, BsonValue, Long> facetTable = HashBasedTable.create();
    @Var long mergedTotalHits = 0;
    for (int i = 0; i < batchProducers.size(); i++) {
      try (var indexPartitionResourceManager = Explain.maybeEnterIndexPartitionQueryContext(i)) {
        mergedTotalHits += batchProducers.get(i).getTotalHits();
        List<IntermediateFacetBucket> buckets = batchProducers.get(i).getAllBucketResults();
        for (var bucket : buckets) {
          // Tag is the facet name.
          if (facetTable.contains(bucket.tag(), bucket.bucket())) {
            facetTable.put(
                bucket.tag(),
                bucket.bucket(),
                facetTable.get(bucket.tag(), bucket.bucket()) + bucket.count());
          } else {
            facetTable.put(bucket.tag(), bucket.bucket(), bucket.count());
          }
        }
      }
    }
    return new MergedResult(facetTable, mergedTotalHits);
  }

  private static ArrayDeque<IntermediateFacetBucket> convertToIntermediateBuckets(
      Table<String, BsonValue, Long> facetTable,
      Map<String, FacetDefinition> facetNameToDefinition) {
    // Convert the table to a list of the encodable IntermediateFacetBucket.
    ArrayDeque<IntermediateFacetBucket> mergedFacetBuckets = new ArrayDeque<>();
    // We output the facetName in the same order of inputs.
    for (String facetName : facetTable.rowKeySet()) {
      FacetDefinition facetDefinition = facetNameToDefinition.get(facetName);
      if (facetDefinition == null) {
        throw new IllegalStateException(
            "The input facet facetName is not found in the facetDefinitions.");
      }
      @Var var entryStream = facetTable.row(facetName).entrySet().stream();
      if (facetDefinition.getType() == FacetDefinition.Type.STRING) {
        /*
         * Apply sort to string facet by ascending facet name string order, which is consistent how
         * string facet is ordered in IntermediateFacetBucket in non index partitions. (Ref: //
         * https://github.com/apache/lucene/blob/9b185b99c429290c80bac5be0bcc2398f58b58db/lucene/core/src/java/org/apache/lucene/index/SortedSetDocValues.java#L28)
         * We don't apply limit here, since this code path is used for intermediateCollectorQuery()
         * only, which is needed by mongos merging results from multiple shards. In this process, we
         * potentially return _all_ string facet buckets.
         */
        entryStream = entryStream.sorted(new StringFacetKeyComparator());
      }
      // We output the facet values in the same order of inputs.
      entryStream.forEach(
          entry -> {
            mergedFacetBuckets.addLast(
                new IntermediateFacetBucket(
                    IntermediateFacetBucket.Type.FACET,
                    facetName,
                    entry.getKey(),
                    entry.getValue()));
          });
    }
    return mergedFacetBuckets;
  }

  @Override
  public void execute(Bytes sizeLimit, BatchCursorOptions queryCursorOptions) throws IOException {}

  @Override
  public BsonArray getNextBatch(Bytes resultsSizeLimit) throws IOException {
    // On the first call, populate the facetBuckets.
    if (this.facetBuckets.isEmpty()) {
      this.facetBuckets =
          Optional.of(
              convertToIntermediateBuckets(
                  this.mergedResult.table,
                  this.batchProducers.get(0).getFacetCollector().facetDefinitions()));
    }

    // Convert to the final list of encoded documents.
    BsonArrayBuilder builder = BsonArrayBuilder.withLimit(resultsSizeLimit);

    if (!this.countProduced) {
      if (!builder.append(
          LuceneFacetCollectorMetaBatchProducer.getCountRawBsonDoc(this.mergedResult.totalHits))) {
        throw new IllegalStateException("Could not fit count bucket in meta buckets batch.");
      }
      this.countProduced = true;
    }

    ArrayDeque<IntermediateFacetBucket> facetBucketValue = this.facetBuckets.get();
    while (!facetBucketValue.isEmpty()) {
      if (builder.append(facetBucketValue.getFirst().toRawBson())) {
        facetBucketValue.removeFirst();
      } else {
        break;
      }
    }
    return builder.build();
  }

  /**
   * Drains all the result information to the format of MetaResults, and then closes this batch
   * producer.
   */
  public MetaResults getMetaResultsAndClose(Count.Type countType) {
    CountResult countResult =
        LuceneFacetResultUtil.getCount(this.mergedResult.totalHits, countType);
    var facetTable = this.mergedResult.table;

    // Convert the table to the format of MetaResults.
    Map<String, FacetDefinition> facetNameToDefinition =
        this.batchProducers.get(0).getFacetCollector().facetDefinitions();
    Map<String, FacetInfo> facetNameToInfo = new HashMap<>();
    for (String facetName : facetNameToDefinition.keySet()) {
      FacetDefinition facetDefinition = facetNameToDefinition.get(facetName);
      if (facetDefinition == null) {
        throw new IllegalStateException(
            "The input facet facetName is not found in the facetDefinitions.");
      }
      // Some facetNames in the facetNameToDefinition may be missing in the `facetTable` if that
      // string facet has no hit. In this case, we still want to create an empty FacetInfo, and the
      // entryStream is empty.
      @Var var entryStream = facetTable.row(facetName).entrySet().stream();
      if (facetDefinition instanceof FacetDefinition.StringFacetDefinition stringFacetDefinition) {
        // Apply sort to string facet. We need to apply limit here because this code path is used
        // in non-intermediate queries.
        entryStream =
            entryStream
                .sorted(new StringFacetCountComparator())
                .limit(stringFacetDefinition.numBuckets());
      }
      List<FacetBucket> buckets =
          entryStream
              .map(entry -> new FacetBucket(entry.getKey(), entry.getValue()))
              .collect(Collectors.toList());
      FacetInfo facetInfo = new FacetInfo(buckets);
      facetNameToInfo.put(facetName, facetInfo);
    }
    // Explicitly calling close() from this class, which throws no exception.
    this.close();
    return new MetaResults(countResult, Optional.of(facetNameToInfo), Optional.empty());
  }

  @Override
  public boolean isExhausted() {
    return this.facetBuckets.isPresent() && this.facetBuckets.get().isEmpty();
  }

  @Override
  public void close() {
    for (var batchProducer : this.batchProducers) {
      batchProducer.close();
    }
  }
}
