package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.BoundaryArgs;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.TermsArgs;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.Test;

public class BoundaryAggregationTest extends AggregationTestSupport {

  // ranges: r0=[0,100), r1=[100,200), r2=[200,300)
  private static final long[] EDGES = {0, 100, 200, 300};
  private static final List<BsonValue> RANGE_KEYS =
      List.of(new BsonInt32(0), new BsonInt32(100), new BsonInt32(200));
  private static final BsonValue DEFAULT_KEY = new BsonString("other");

  private static final List<TestDoc> DATASET =
      List.of(
          new TestDoc("NYC", "red", 10), // r0
          new TestDoc("NYC", "blue", 50), // r0
          new TestDoc("SF", "green", 250), // r2
          new TestDoc("LA", "blue", 350), // out of range (>= 300)
          new TestDoc("SF", "red", -5)); // out of range (< 0)

  private static TopAggregationSpec priceBoundary(
      Optional<BsonValue> defaultKey, boolean lastEdgeInclusive) {
    AggregationSpec sum = new TestOperatorSpecs.SumSpec("sum_price", PRICE_LUCENE_FIELD);
    AggregationSpec byPrice =
        new BoundaryGroupAggregationSpec(
            "by_price",
            FieldPath.parse(PRICE_FIELD),
            List.of(sum),
            EDGES,
            RANGE_KEYS,
            lastEdgeInclusive,
            new BoundaryArgs(RANGE_KEYS, defaultKey, TypeField.NUMBER_INT64_V2, null));
    return new TopAggregationSpec(List.of(byPrice));
  }

  @Test
  public void returnsAllRangeBucketsWithCountsAndChildSums() throws Exception {
    buildIndex(DATASET, DATASET.size()); // single segment
    Accumulator top = collectAll(priceBoundary(Optional.empty(), false), newContext());

    assertTrue(top.children().get(0) instanceof BoundaryAccumulator);

    BoundaryAggregationResult byPrice = firstBoundary(top);
    // Out-of-range docs dropped (no default); 3 range buckets, including the empty r1.
    assertEquals(3, byPrice.getBuckets(BucketOrder.KEY_ASC).size());

    assertEquals(new BsonInt32(0), keyAt(byPrice, 0));
    assertEquals(2L, countAt(byPrice, 0));
    assertEquals(60.0, sumAt(bucketAt(byPrice, 0), "sum_price"), 1e-9);

    // Empty range is still emitted with count 0 (boundary keeps empties; terms drops them).
    assertEquals(new BsonInt32(100), keyAt(byPrice, 1));
    assertEquals(0L, countAt(byPrice, 1));
    assertEquals(0.0, sumAt(bucketAt(byPrice, 1), "sum_price"), 1e-9);

    assertEquals(new BsonInt32(200), keyAt(byPrice, 2));
    assertEquals(1L, countAt(byPrice, 2));
    assertEquals(250.0, sumAt(bucketAt(byPrice, 2), "sum_price"), 1e-9);
  }

  @Test
  public void multiSegmentBucketsIndependently() throws Exception {
    buildIndex(DATASET, 2); // 3 segments -- boundary needs no OrdinalMap; values are absolute
    Accumulator top = collectAll(priceBoundary(Optional.empty(), false), newContext());

    BoundaryAggregationResult byPrice = firstBoundary(top);
    assertEquals(3, byPrice.getBuckets(BucketOrder.KEY_ASC).size());
    assertEquals(2L, countAt(byPrice, 0));
    assertEquals(0L, countAt(byPrice, 1));
    assertEquals(1L, countAt(byPrice, 2));
    assertEquals(60.0, sumAt(bucketAt(byPrice, 0), "sum_price"), 1e-9);
    assertEquals(250.0, sumAt(bucketAt(byPrice, 2), "sum_price"), 1e-9);
  }

  @Test
  public void multiValuedDocCountedOncePerRange() throws Exception {
    // doc0 has two values in r0 (counted once); doc1 spans r1+r2 (counted in both); doc2 in r0.
    buildPriceIndex(
        List.of(
            new long[] {10, 50}, // both r0
            new long[] {150, 250}, // r1 + r2
            new long[] {50, 60}), // both r0
        3);
    Accumulator top = collectAll(priceBoundary(Optional.empty(), false), newContext());

    BoundaryAggregationResult byPrice = firstBoundary(top);
    // r0: doc0 + doc2 (each once despite two in-range values); r1: doc1; r2: doc1.
    assertEquals(2L, countAt(byPrice, 0));
    assertEquals(1L, countAt(byPrice, 1));
    assertEquals(1L, countAt(byPrice, 2));
  }

  @Test
  public void defaultBucketCapturesOutOfRangeAndMissingField() throws Exception {
    buildPriceIndex(
        List.of(
            new long[] {10}, // r0
            new long[] {250}, // r2
            new long[] {350}, // out (>= 300) -> default
            new long[] {-5}, // out (< 0) -> default
            new long[] {}), // missing field -> default
        5);
    Accumulator top = collectAll(priceBoundary(Optional.of(DEFAULT_KEY), false), newContext());

    BoundaryAggregationResult byPrice = firstBoundary(top);
    assertEquals(4, byPrice.getBuckets(BucketOrder.KEY_ASC).size()); // 3 ranges + default

    assertEquals(1L, countAt(byPrice, 0));
    assertEquals(0L, countAt(byPrice, 1));
    assertEquals(1L, countAt(byPrice, 2));

    // Default bucket is last, keyed by the supplied label, counting all 3 no-match docs.
    assertEquals(DEFAULT_KEY, keyAt(byPrice, 3));
    assertEquals(3L, countAt(byPrice, 3));
  }

  @Test
  public void noDefaultDropsOutOfRangeDocs() throws Exception {
    buildPriceIndex(List.of(new long[] {10}, new long[] {350}, new long[] {-5}, new long[] {}), 4);
    Accumulator top = collectAll(priceBoundary(Optional.empty(), false), newContext());

    BoundaryAggregationResult byPrice = firstBoundary(top);
    assertEquals(3, byPrice.getBuckets(BucketOrder.KEY_ASC).size()); // no default bucket
    assertEquals(1L, countAt(byPrice, 0)); // only the in-range doc; 350/-5/missing dropped
    assertEquals(0L, countAt(byPrice, 1));
    assertEquals(0L, countAt(byPrice, 2));
  }

  @Test
  public void lastEdgeInclusiveAttachesTopBoundaryValueToLastRange() throws Exception {
    // price=300 is exactly the last edge: inclusive captures it in r2; exclusive sends it to
    // default.
    buildPriceIndex(List.of(new long[] {10}, new long[] {250}, new long[] {300}), 3);

    BoundaryAggregationResult inclusive =
        firstBoundary(collectAll(priceBoundary(Optional.of(DEFAULT_KEY), true), newContext()));
    assertEquals(1L, countAt(inclusive, 0)); // price=10 in r0
    assertEquals(2L, countAt(inclusive, 2)); // price=250 + price=300 captured by r2
    assertEquals(0L, countAt(inclusive, 3)); // default is empty: 300 landed in r2, not here

    BoundaryAggregationResult exclusive =
        firstBoundary(collectAll(priceBoundary(Optional.of(DEFAULT_KEY), false), newContext()));
    assertEquals(1L, countAt(inclusive, 0)); // price=10 in r0
    assertEquals(1L, countAt(exclusive, 2)); // only price=250 in r2
    assertEquals(1L, countAt(exclusive, 3)); // price=300 is out-of-range, sent to default
  }

  @Test
  public void termsOuterBoundaryInnerMultiSegment() throws Exception {
    List<TestDoc> data =
        List.of(
            new TestDoc("NYC", "red", 10), // NYC r0
            new TestDoc("NYC", "blue", 150), // NYC r1
            new TestDoc("LA", "red", 50), // LA  r0
            new TestDoc("LA", "blue", 250), // LA  r2
            new TestDoc("SF", "green", 350)); // SF  default
    buildIndex(data, 2); // 3 segments: terms parent builds a global OrdinalMap

    AggregationSpec sum = new TestOperatorSpecs.SumSpec("sum_price", PRICE_LUCENE_FIELD);
    AggregationSpec byPrice =
        new BoundaryGroupAggregationSpec(
            "by_price",
            FieldPath.parse(PRICE_FIELD),
            List.of(sum),
            EDGES,
            RANGE_KEYS,
            false,
            new BoundaryArgs(
                RANGE_KEYS, Optional.of(DEFAULT_KEY), TypeField.NUMBER_INT64_V2, null));
    AggregationSpec byCity =
        new TermsGroupAggregationSpec(
            "by_city",
            FieldPath.parse(CITY_FIELD),
            List.of(byPrice),
            new TermsArgs(true, TypeField.TOKEN, 100));
    Accumulator top = collectAll(new TopAggregationSpec(List.of(byCity)), newContext());

    assertTrue(top.children().get(0) instanceof DenseTermsAccumulator);
    assertTrue(top.children().get(0).children().get(0) instanceof BoundaryAccumulator);

    TermsAggregationResult byCityResult =
        (TermsAggregationResult)
            ((TopLevelAggregationResult) top.buildTopLevelResult()).childResults().get(0);

    BoundaryAggregationResult nyc = childBoundary(bucket(byCityResult, "NYC"));
    assertEquals(4, nyc.getBuckets(BucketOrder.KEY_ASC).size());
    assertEquals(1L, countAt(nyc, 0)); // 10
    assertEquals(1L, countAt(nyc, 1)); // 150
    assertEquals(0L, countAt(nyc, 2));
    assertEquals(0L, countAt(nyc, 3)); // default

    BoundaryAggregationResult la = childBoundary(bucket(byCityResult, "LA"));
    assertEquals(1L, countAt(la, 0)); // 50
    assertEquals(0L, countAt(la, 1));
    assertEquals(1L, countAt(la, 2)); // 250
    assertEquals(0L, countAt(la, 3));

    BoundaryAggregationResult sf = childBoundary(bucket(byCityResult, "SF"));
    assertEquals(0L, countAt(sf, 0));
    assertEquals(0L, countAt(sf, 1));
    assertEquals(0L, countAt(sf, 2));
    assertEquals(1L, countAt(sf, 3)); // 350 -> default
  }

  @Test
  public void mergeSumsCountsAndChildrenPositionally() {
    BoundaryAggregationResult left =
        byPriceResult(
            boundaryBucket(new BsonInt32(0), 2, 60.0),
            boundaryBucket(new BsonInt32(100), 1, 150.0),
            boundaryBucket(new BsonInt32(200), 0, 0.0));
    BoundaryAggregationResult right =
        byPriceResult(
            boundaryBucket(new BsonInt32(0), 1, 10.0),
            boundaryBucket(new BsonInt32(100), 0, 0.0),
            boundaryBucket(new BsonInt32(200), 3, 900.0));

    BoundaryAggregationResult merged = (BoundaryAggregationResult) merge(left, right);

    assertEquals(3, merged.getBuckets(BucketOrder.KEY_ASC).size());
    assertEquals(new BsonInt32(0), keyAt(merged, 0));
    assertEquals(3L, countAt(merged, 0)); // 2 + 1
    assertEquals(70.0, sumAt(bucketAt(merged, 0), "sum_price"), 1e-9); // 60 + 10
    assertEquals(1L, countAt(merged, 1));
    assertEquals(150.0, sumAt(bucketAt(merged, 1), "sum_price"), 1e-9);
    assertEquals(3L, countAt(merged, 2));
    assertEquals(900.0, sumAt(bucketAt(merged, 2), "sum_price"), 1e-9);
  }

  @Test
  public void mergeRejectsDifferentBucketCounts() {
    BoundaryAggregationResult left =
        byPriceResult(
            boundaryBucket(new BsonInt32(0), 1, 0.0), boundaryBucket(new BsonInt32(100), 1, 0.0));
    BoundaryAggregationResult right = byPriceResult(boundaryBucket(new BsonInt32(0), 1, 0.0));
    assertThrows(IllegalArgumentException.class, () -> merge(left, right));
  }

  @Test
  public void mergeRejectsDivergingBucketKeys() {
    BoundaryAggregationResult left = byPriceResult(boundaryBucket(new BsonInt32(0), 1, 0.0));
    BoundaryAggregationResult right = byPriceResult(boundaryBucket(new BsonInt32(5), 1, 0.0));
    assertThrows(IllegalArgumentException.class, () -> merge(left, right));
  }

  @Test
  public void specRejectsNonAscendingEdges() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new BoundaryGroupAggregationSpec(
                "by_price",
                FieldPath.parse(PRICE_FIELD),
                List.of(),
                new long[] {0, 100, 50},
                List.of(new BsonInt32(0), new BsonInt32(100)),
                false,
                new BoundaryArgs(
                    List.of(new BsonInt32(0), new BsonInt32(100)),
                    Optional.empty(),
                    TypeField.NUMBER_INT64_V2,
                    null)));
  }

  @Test
  public void specRejectsRangeKeySizeMismatch() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new BoundaryGroupAggregationSpec(
                "by_price",
                FieldPath.parse(PRICE_FIELD),
                List.of(),
                new long[] {0, 100, 200}, // 2 ranges
                List.of(new BsonInt32(0)), // only 1 key
                false,
                new BoundaryArgs(
                    List.of(new BsonInt32(0)), Optional.empty(), TypeField.NUMBER_INT64_V2, null)));
  }

  private static BoundaryAggregationResult firstBoundary(Accumulator top) throws IOException {
    TopLevelAggregationResult result = (TopLevelAggregationResult) top.buildTopLevelResult();
    return (BoundaryAggregationResult) result.childResults().get(0);
  }

  private static BoundaryAggregationResult.BoundaryBucket bucketAt(
      BoundaryAggregationResult result, int index) {
    return (BoundaryAggregationResult.BoundaryBucket)
        result.getBuckets(BucketOrder.KEY_ASC).get(index);
  }

  private static BsonValue keyAt(BoundaryAggregationResult result, int index) {
    return bucketAt(result, index).getKey();
  }

  private static long countAt(BoundaryAggregationResult result, int index) {
    return bucketAt(result, index).getDocCount();
  }

  private static double sumAt(BoundaryAggregationResult.BoundaryBucket bucket, String childName) {
    return ((TestOperatorSpecs.SumResult) bucket.getChildResults().get(childName)).value();
  }

  private static BoundaryAggregationResult childBoundary(
      TermsAggregationResult.TermsBucket bucket) {
    return bucket.getChildResults().get("by_price");
  }

  private static BoundaryAggregationResult.BoundaryBucket boundaryBucket(
      BsonValue key, long docCount, double sum) {
    AggregationResults children =
        AggregationResults.from(List.of(new TestOperatorSpecs.SumResult("sum_price", sum)));
    return new BoundaryAggregationResult.BoundaryBucket(key, docCount, children);
  }

  private static BoundaryAggregationResult byPriceResult(
      BoundaryAggregationResult.BoundaryBucket... buckets) {
    return new BoundaryAggregationResult(
        "by_price", FieldPath.parse(PRICE_FIELD), List.of(buckets));
  }

  /**
   * Build an in-memory index where each document carries zero or more {@code price} values (empty
   * array =&gt; the field is missing), committing every {@code docsPerSegment} docs for segment
   * control. Reuses the shared {@code directory} / {@code reader} / {@code searcher} fields so the
   * base class's teardown closes them.
   */
  private void buildPriceIndex(List<long[]> pricesPerDoc, int docsPerSegment) throws IOException {
    this.directory = new ByteBuffersDirectory();
    IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(this.directory, config)) {
      for (int i = 0; i < pricesPerDoc.size(); i++) {
        Document doc = new Document();
        for (long value : pricesPerDoc.get(i)) {
          doc.add(new SortedNumericDocValuesField(PRICE_LUCENE_FIELD, value));
        }
        writer.addDocument(doc);
        if ((i + 1) % docsPerSegment == 0) {
          writer.commit();
        }
      }
    }
    this.reader = DirectoryReader.open(this.directory);
    this.searcher = new IndexSearcher(this.reader);
  }
}
