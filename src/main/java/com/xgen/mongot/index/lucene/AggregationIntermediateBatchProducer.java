package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.index.BatchProducer;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.bson.BsonArrayBuilder;
import java.io.IOException;
import org.bson.BsonArray;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

/**
 * Streams {@link BatchProducer} batches for sharded aggregation {@code intermediateQuery} results.
 *
 * <p>The first batch includes a legacy count document ({@code type: "count"}) when it fits within
 * {@code resultsSizeLimit}, followed by flat aggregation intermediate documents from {@link
 * com.xgen.mongot.index.query.aggregation.AggregationResult#toIntermediateBson()}. Additional
 * intermediate documents are returned across subsequent batches, respecting the per-batch BSON size
 * limit (same semantics as {@link LuceneFacetCollectorMetaBatchProducer}).
 */
final class AggregationIntermediateBatchProducer implements BatchProducer {

  private final BsonArray intermediateDocs;
  private final long totalHits;

  private int nextDocIndex;
  private boolean countProduced;
  private boolean closed;

  AggregationIntermediateBatchProducer(BsonArray intermediateDocs, long totalHits) {
    this.intermediateDocs = intermediateDocs;
    this.totalHits = totalHits;
    this.nextDocIndex = 0;
    this.countProduced = false;
    this.closed = false;
  }

  @Override
  public void execute(Bytes sizeLimit, BatchCursorOptions queryCursorOptions) throws IOException {
    // Docs are precomputed before the producer is created.
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

    while (this.nextDocIndex < this.intermediateDocs.size()) {
      RawBsonDocument doc = toRawBsonDocument(this.intermediateDocs.get(this.nextDocIndex));
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
    return this.countProduced && this.nextDocIndex >= this.intermediateDocs.size();
  }

  @Override
  public void close() {
    this.closed = true;
    this.nextDocIndex = this.intermediateDocs.size();
    this.countProduced = true;
  }

  private static RawBsonDocument toRawBsonDocument(BsonValue value) {
    return BsonUtils.documentToRaw(value.asDocument());
  }
}
