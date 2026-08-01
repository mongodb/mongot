package com.xgen.mongot.index.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.index.CountResult;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.query.aggregation.AggregationResult;
import com.xgen.mongot.index.query.aggregation.AggregationResultBuilder;
import com.xgen.mongot.index.query.counts.Count;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Bytes;
import java.io.IOException;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.Test;

public class AggregationIntermediateBatchProducerTest {

  private static final BsonDocument COUNT_DOC =
      new BsonDocument("type", new BsonString("count")).append("count", new BsonInt64(42));

  /**
   * A result that refuses to flatten, standing in for a leaf metric with no cross-shard
   * intermediate format yet -- {@code first}/{@code last} throw from {@code
   * toIntermediateMetricPartials}.
   */
  private static AggregationResult unflattenableResult() {
    return new AggregationResult() {
      @Override
      public String getName() {
        throw new UnsupportedOperationException();
      }

      @Override
      public AggregationResultBuilder toBuilder() {
        throw new UnsupportedOperationException();
      }

      @Override
      public AggregationResult applyLimit() {
        return this;
      }

      @Override
      public BsonValue toStandaloneBson() {
        throw new UnsupportedOperationException();
      }

      @Override
      public BsonArray toIntermediateBson() {
        throw new IllegalStateException("Sharded merge is not implemented yet");
      }
    };
  }

  /**
   * The non-sharded partitioned path builds a producer per partition and only ever drains it to
   * MetaResults, so closing one must not flatten its tree. A leaf metric that has no intermediate
   * format yet would otherwise throw on a query that is not sharded at all.
   */
  @Test
  public void close_doesNotFlattenTheResultTree() {
    AggregationIntermediateBatchProducer producer =
        new AggregationIntermediateBatchProducer(unflattenableResult(), 7);

    producer.close();

    assertTrue(producer.isExhausted());
  }

  /** Exhaustion after close is reported without consulting the rows. */
  @Test
  public void close_marksExhausted() {
    AggregationIntermediateBatchProducer producer =
        new AggregationIntermediateBatchProducer(
            fakeResult(new BsonArray(List.of(new BsonDocument("_id", new BsonString("a"))))), 1);

    assertFalse(producer.isExhausted());

    producer.close();

    assertTrue(producer.isExhausted());
  }

  /**
   * The non-sharded terminal takes the nested tree as-is, so a metric with no cross-shard format
   * still serves a query that is not sharded.
   */
  @Test
  public void getMetaResultsAndClose_doesNotFlattenTheResultTree() {
    AggregationIntermediateBatchProducer producer =
        new AggregationIntermediateBatchProducer(unflattenableResult(), 7);

    MetaResults metaResults = producer.getMetaResultsAndClose(Count.Type.LOWER_BOUND);

    assertTrue(metaResults.aggregation().isPresent());
  }

  /**
   * Constructing a producer over a tree that cannot be flattened is fine; only {@code execute},
   * reached solely from the sharded streaming path, surfaces the missing wire format.
   */
  @Test
  public void execute_isTheOnlySiteThatFlattens() throws IOException {
    AggregationIntermediateBatchProducer producer =
        new AggregationIntermediateBatchProducer(unflattenableResult(), 7);

    assertThrows(
        IllegalStateException.class,
        () ->
            producer.execute(
                CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptions.empty()));
  }

  /** Repeated execute() calls, one per batch from MongotCursor, flatten exactly once. */
  @Test
  public void execute_isIdempotentAcrossBatches() throws IOException {
    int[] flattenCount = {0};
    AggregationResult countingResult =
        new AggregationResult() {
          @Override
          public String getName() {
            throw new UnsupportedOperationException();
          }

          @Override
          public AggregationResultBuilder toBuilder() {
            throw new UnsupportedOperationException();
          }

          @Override
          public AggregationResult applyLimit() {
            return this;
          }

          @Override
          public BsonValue toStandaloneBson() {
            throw new UnsupportedOperationException();
          }

          @Override
          public BsonArray toIntermediateBson() {
            flattenCount[0]++;
            return new BsonArray(List.of(intermediateDoc("NYC", 30.0)));
          }
        };
    AggregationIntermediateBatchProducer producer =
        new AggregationIntermediateBatchProducer(countingResult, 42);

    producer.execute(CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptions.empty());
    producer.execute(CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT, BatchCursorOptions.empty());

    assertEquals(1, flattenCount[0]);
  }

  /**
   * Mirrors {@link com.xgen.mongot.cursor.MongotCursor}, which calls {@code execute} before every
   * {@code getNextBatch}. Flattening happens in {@code execute}, so a test that skips it is not
   * exercising the sharded streaming path.
   */
  private static BsonArray nextBatch(AggregationIntermediateBatchProducer producer, Bytes sizeLimit)
      throws IOException {
    producer.execute(sizeLimit, BatchCursorOptions.empty());
    return producer.getNextBatch(sizeLimit);
  }

  /** Fake result whose only supported operation is flattening to the given intermediate docs. */
  static AggregationResult fakeResult(BsonArray intermediateDocs) {
    return new AggregationResult() {
      @Override
      public String getName() {
        throw new UnsupportedOperationException();
      }

      @Override
      public AggregationResultBuilder toBuilder() {
        throw new UnsupportedOperationException();
      }

      @Override
      public AggregationResult applyLimit() {
        // Test fake: no buckets to truncate.
        return this;
      }

      @Override
      public BsonValue toStandaloneBson() {
        throw new UnsupportedOperationException();
      }

      @Override
      public BsonArray toIntermediateBson() {
        return intermediateDocs;
      }
    };
  }

  private static BsonDocument intermediateDoc(String city, double sum) {
    return new BsonDocument()
        .append("group_name", new BsonString("by_city"))
        .append("keys", new BsonDocument("city", new BsonString(city)))
        .append("sum_sum_price", new BsonDouble(sum));
  }

  @Test
  public void getNextBatch_prependsCountDocAndReturnsIntermediateDocs() throws IOException {
    BsonArray intermediateDocs = new BsonArray(List.of(intermediateDoc("NYC", 30.0)));

    AggregationIntermediateBatchProducer producer =
        new AggregationIntermediateBatchProducer(fakeResult(intermediateDocs), 42);

    assertFalse(producer.isExhausted());

    BsonArray batch = nextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    assertEquals(2, batch.size());
    assertEquals(COUNT_DOC, batch.get(0).asDocument());
    assertEquals(intermediateDocs.get(0), batch.get(1));

    assertTrue(producer.isExhausted());
    assertEquals(0, nextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT).size());
  }

  @Test
  public void getNextBatch_emptyIntermediateDocs_returnsCountOnly() throws IOException {
    AggregationIntermediateBatchProducer producer =
        new AggregationIntermediateBatchProducer(fakeResult(new BsonArray()), 5);

    BsonArray batch = nextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    assertEquals(1, batch.size());
    assertEquals(
        LuceneFacetCollectorMetaBatchProducer.getCountRawBsonDoc(5).asDocument(),
        batch.get(0).asDocument());
    assertTrue(producer.isExhausted());
  }

  @Test
  public void getNextBatch_respectsSizeLimitAcrossMultipleBatches() throws IOException {
    BsonArray intermediateDocs =
        new BsonArray(List.of(intermediateDoc("NYC", 30.0), intermediateDoc("SF", 50.0)));

    AggregationIntermediateBatchProducer producer =
        new AggregationIntermediateBatchProducer(fakeResult(intermediateDocs), 42);

    BsonArray sizeForCountAndOneDoc = new BsonArray();
    sizeForCountAndOneDoc.add(COUNT_DOC);
    sizeForCountAndOneDoc.add(intermediateDocs.get(0));
    Bytes sizeLimit = BsonUtils.bsonValueSerializedBytes(sizeForCountAndOneDoc);

    @Var boolean countChecked = false;
    @Var int docCount = 0;
    @Var int batchCount = 0;
    BsonArray drainedIntermediateDocs = new BsonArray();
    while (!producer.isExhausted()) {
      BsonArray batch = nextBatch(producer, sizeLimit);
      batchCount++;
      for (BsonValue value : batch) {
        BsonDocument doc = value.asDocument();
        if (doc.containsKey("type")
            && doc.get("type").isString()
            && "count".equals(doc.getString("type").getValue())) {
          if (!countChecked) {
            assertEquals(new BsonInt64(42), doc.get("count"));
            countChecked = true;
          }
          continue;
        }
        drainedIntermediateDocs.add(doc);
      }
      docCount += batch.size();
    }

    assertTrue(countChecked);
    assertEquals(intermediateDocs, drainedIntermediateDocs);
    assertEquals(3, docCount);
    assertEquals(2, batchCount);
  }

  // ===== merge(): fold per-partition producers into one =====

  /** Fake mergeable result: partition merge sums the value; flattening emits one doc. */
  private record SumResult(long value) implements AggregationResult {
    @Override
    public String getName() {
      return "sum_result";
    }

    @Override
    public AggregationResultBuilder toBuilder() {
      // Zero-identity sum builder: starts at 0 (the sum monoid's identity) and folds uniformly.
      return new AggregationResultBuilder() {
        private long sum;

        @Override
        public void add(AggregationResult other) {
          this.sum += ((SumResult) other).value;
        }

        @Override
        public AggregationResult build() {
          return new SumResult(this.sum);
        }
      };
    }

    @Override
    public AggregationResult applyLimit() {
      // Leaf metric: no buckets to truncate.
      return this;
    }

    @Override
    public BsonValue toStandaloneBson() {
      return new BsonDocument("value", new BsonInt64(this.value));
    }

    @Override
    public BsonArray toIntermediateBson() {
      return new BsonArray(List.of(new BsonDocument("sum_value", new BsonInt64(this.value))));
    }
  }

  /** Fake result whose builder rejects every add(), standing in for a partition shape mismatch. */
  private record RejectingResult() implements AggregationResult {
    @Override
    public String getName() {
      return "rejecting_result";
    }

    @Override
    public AggregationResultBuilder toBuilder() {
      return new AggregationResultBuilder() {
        @Override
        public void add(AggregationResult other) {
          throw new IllegalArgumentException("cannot merge results of differing shape");
        }

        @Override
        public AggregationResult build() {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public AggregationResult applyLimit() {
      return this;
    }

    @Override
    public BsonValue toStandaloneBson() {
      throw new UnsupportedOperationException();
    }

    @Override
    public BsonArray toIntermediateBson() {
      return new BsonArray();
    }
  }

  private static AggregationIntermediateBatchProducer partitionProducer(
      long value, long totalHits) {
    return new AggregationIntermediateBatchProducer(new SumResult(value), totalHits);
  }

  private static AggregationIntermediateBatchProducer merged(
      List<AggregationIntermediateBatchProducer> partitionProducers) throws IOException {
    return AggregationIntermediateBatchProducer.merge(partitionProducers);
  }

  @Test
  public void merge_mergesPartitionResultsAndTotalHits() throws IOException {
    AggregationIntermediateBatchProducer producer =
        merged(List.of(partitionProducer(30, 10), partitionProducer(12, 5)));

    BsonArray batch = nextBatch(producer, CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    assertEquals(2, batch.size());
    assertEquals(
        new BsonDocument("type", new BsonString("count")).append("count", new BsonInt64(15)),
        batch.get(0).asDocument());
    assertEquals(new BsonDocument("sum_value", new BsonInt64(42)), batch.get(1).asDocument());
    assertTrue(producer.isExhausted());
  }

  @Test
  public void merge_closesPartitionProducers() throws IOException {
    List<AggregationIntermediateBatchProducer> partitionProducers =
        List.of(partitionProducer(30, 10), partitionProducer(12, 5));

    AggregationIntermediateBatchProducer producer = merged(partitionProducers);

    for (var partitionProducer : partitionProducers) {
      assertThrows(
          IllegalStateException.class,
          () -> partitionProducer.getNextBatch(CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT));
    }
    // The merged producer itself is still usable.
    assertFalse(producer.isExhausted());
  }

  @Test
  public void getMetaResultsAndClose_returnsMergedNestedResult() throws IOException {
    AggregationIntermediateBatchProducer producer =
        merged(List.of(partitionProducer(30, 10), partitionProducer(12, 5)));

    MetaResults metaResults = producer.getMetaResultsAndClose(Count.Type.TOTAL);

    assertEquals(CountResult.totalCount(15), metaResults.count());
    assertTrue(metaResults.facet().isEmpty());
    assertEquals(
        new BsonDocument("value", new BsonInt64(42)),
        metaResults.aggregation().orElseThrow().toStandaloneBson());

    // getMetaResultsAndClose closes the producer.
    assertThrows(
        IllegalStateException.class,
        () -> producer.getNextBatch(CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT));
  }

  @Test
  public void merge_singlePartition_returnsThatResult() throws IOException {
    AggregationIntermediateBatchProducer producer = merged(List.of(partitionProducer(7, 3)));

    MetaResults metaResults = producer.getMetaResultsAndClose(Count.Type.LOWER_BOUND);

    assertEquals(CountResult.lowerBoundCount(3), metaResults.count());
    assertEquals(
        new BsonDocument("value", new BsonInt64(7)),
        metaResults.aggregation().orElseThrow().toStandaloneBson());
  }

  @Test
  public void merge_foldFailure_stillClosesEveryPartitionProducer() {
    // The first add() throws, so the fold aborts before reaching the second producer.
    List<AggregationIntermediateBatchProducer> partitionProducers =
        List.of(
            new AggregationIntermediateBatchProducer(new RejectingResult(), 10),
            partitionProducer(12, 5));

    assertThrows(
        IllegalArgumentException.class,
        () -> AggregationIntermediateBatchProducer.merge(partitionProducers));

    for (var partitionProducer : partitionProducers) {
      assertThrows(
          IllegalStateException.class,
          () -> partitionProducer.getNextBatch(CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT));
    }
  }

  @Test
  public void merge_emptyProducers_throws() throws IOException {
    assertThrows(
        IllegalArgumentException.class,
        () -> AggregationIntermediateBatchProducer.merge(List.of()));
  }
}
