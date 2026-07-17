package com.xgen.mongot.index.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.cursor.CursorConfig;
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
        new AggregationIntermediateBatchProducer(intermediateDocs, 42);

    assertFalse(producer.isExhausted());

    BsonArray batch = producer.getNextBatch(CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

    assertEquals(2, batch.size());
    assertEquals(COUNT_DOC, batch.get(0).asDocument());
    assertEquals(intermediateDocs.get(0), batch.get(1));

    assertTrue(producer.isExhausted());
    assertEquals(0, producer.getNextBatch(CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT).size());
  }

  @Test
  public void getNextBatch_emptyIntermediateDocs_returnsCountOnly() throws IOException {
    AggregationIntermediateBatchProducer producer =
        new AggregationIntermediateBatchProducer(new BsonArray(), 5);

    BsonArray batch = producer.getNextBatch(CursorConfig.DEFAULT_BSON_SIZE_SOFT_LIMIT);

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
        new AggregationIntermediateBatchProducer(intermediateDocs, 42);

    BsonArray sizeForCountAndOneDoc = new BsonArray();
    sizeForCountAndOneDoc.add(COUNT_DOC);
    sizeForCountAndOneDoc.add(intermediateDocs.get(0));
    Bytes sizeLimit = BsonUtils.bsonValueSerializedBytes(sizeForCountAndOneDoc);

    @Var boolean countChecked = false;
    @Var int docCount = 0;
    @Var int batchCount = 0;
    BsonArray drainedIntermediateDocs = new BsonArray();
    while (!producer.isExhausted()) {
      BsonArray batch = producer.getNextBatch(sizeLimit);
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
}
