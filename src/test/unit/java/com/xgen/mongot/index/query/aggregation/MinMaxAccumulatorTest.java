package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.field.FieldValue;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.TermsArgs;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * Tests for doc-values-backed {@code min}/{@code max}. Values are read from every sortable column
 * the metric path is indexed under, so a path carrying several types at once contributes all of
 * them, ordered by the BSON total ordering. Paths indexed under none of those types are rejected
 * during field resolution, covered in {@code LuceneAggregationContextFieldResolverTest}.
 *
 * <p>Unlike sum/avg, a multi-valued metric field contributes the extremum of its values rather than
 * being skipped, matching Elasticsearch {@code min}/{@code max} over arrays. Also unlike sum/avg, a
 * doc fanned into several buckets by a multi-valued group key contributes to all of them, since
 * selecting an extremum is idempotent and so cannot double-count.
 */
public class MinMaxAccumulatorTest extends AggregationTestSupport {

  @Test
  public void topLevelMinMax_selectSmallestAndLargest() throws Exception {
    buildInt64PriceIndex(List.of(values(10), values(20), values(5)));

    assertEquals(Optional.of(new BsonInt64(5)), minValue(runTopLevel(minSpec(), int64Context())));
    assertEquals(Optional.of(new BsonInt64(20)), maxValue(runTopLevel(maxSpec(), int64Context())));
  }

  @Test
  public void int64Representation_preservesIntegerType() throws Exception {
    // min/max return a value drawn from the data, so an int64 field must not widen to double the
    // way the computed sum/avg metrics do.
    buildInt64PriceIndex(List.of(values(7)));

    assertEquals(Optional.of(new BsonInt64(7)), minValue(runTopLevel(minSpec(), int64Context())));
  }

  @Test
  public void multiValuedMetric_takesExtremumOfTheArray() throws Exception {
    // doc1: 1, doc2: [-1, 2, 4] => min -1, max 4. sum/avg skip multi-valued metric docs entirely;
    // min/max must instead fold every value in the array into the bucket.
    buildInt64PriceIndex(List.of(values(1), values(-1, 2, 4)));

    assertEquals(Optional.of(new BsonInt64(-1)), minValue(runTopLevel(minSpec(), int64Context())));
    assertEquals(Optional.of(new BsonInt64(4)), maxValue(runTopLevel(maxSpec(), int64Context())));
  }

  @Test
  public void multiValuedMetric_singleDocArrayOnly() throws Exception {
    // The whole array lives on one doc: the extremum still comes from within it, so a max cannot be
    // dragged down by the array's first (smallest) value.
    buildInt64PriceIndex(List.of(values(3, 8, 5)));

    assertEquals(Optional.of(new BsonInt64(3)), minValue(runTopLevel(minSpec(), int64Context())));
    assertEquals(Optional.of(new BsonInt64(8)), maxValue(runTopLevel(maxSpec(), int64Context())));
  }

  @Test
  public void doubleRepresentation_decodesSortableLongValues() throws Exception {
    buildDoublePriceIndex(List.of(List.of(10.5), List.of(-3.25), List.of(20.75)));

    assertEquals(
        Optional.of(new BsonDouble(-3.25)), minValue(runTopLevel(minSpec(), doubleContext())));
    assertEquals(
        Optional.of(new BsonDouble(20.75)), maxValue(runTopLevel(maxSpec(), doubleContext())));
  }

  @Test
  public void doubleRepresentation_multiValued() throws Exception {
    buildDoublePriceIndex(List.of(List.of(1.5), List.of(-1.5, 2.5, 4.5)));

    assertEquals(
        Optional.of(new BsonDouble(-1.5)), minValue(runTopLevel(minSpec(), doubleContext())));
    assertEquals(
        Optional.of(new BsonDouble(4.5)), maxValue(runTopLevel(maxSpec(), doubleContext())));
  }

  @Test
  public void doubleRepresentation_nanSortsBelowAllNumbers() throws Exception {
    // NaN is indexed as NAN_SENTINEL, which the MQL sortable encoding places below -Infinity, so
    // comparing in encoded space gives NaN < every real number -- matching MQL $min.
    buildDoublePriceIndex(List.of(List.of(Double.NaN), List.of(-1.0), List.of(5.0)));

    Optional<BsonValue> min = minValue(runTopLevel(minSpec(), doubleContext()));
    assertTrue(Double.isNaN(((BsonDouble) min.orElseThrow()).getValue()));
    assertEquals(
        Optional.of(new BsonDouble(5.0)), maxValue(runTopLevel(maxSpec(), doubleContext())));
  }

  @Test
  public void missingMetricValue_doesNotContribute() throws Exception {
    // Second doc has no price column at all: only the present value counts.
    buildInt64PriceIndex(List.of(values(42), values()));

    assertEquals(Optional.of(new BsonInt64(42)), minValue(runTopLevel(minSpec(), int64Context())));
  }

  @Test
  public void allMissing_yieldsNoValue() throws Exception {
    buildInt64PriceIndex(List.of(values(), values()));

    MinAggregationResult min = (MinAggregationResult) runTopLevel(minSpec(), int64Context());
    assertEquals(Optional.empty(), min.getValue());
    assertEquals(BsonNull.VALUE, min.toStandaloneBson());
    // No value on this partition: the intermediate partial is omitted so the cross-shard $min
    // skips it.
    assertTrue(min.toIntermediateMetricPartials().isEmpty());
  }

  @Test
  public void allMissing_maxYieldsNoValueAndOmitsPartial() throws Exception {
    // Mirror of allMissing_yieldsNoValue for the max side: the two results are separate classes
    // with separately written toStandaloneBson/toIntermediateMetricPartials, so covering only min
    // leaves half the contract unpinned.
    buildInt64PriceIndex(List.of(values(), values()));

    MaxAggregationResult max = (MaxAggregationResult) runTopLevel(maxSpec(), int64Context());
    assertEquals(Optional.empty(), max.getValue());
    assertEquals(BsonNull.VALUE, max.toStandaloneBson());
    assertTrue(max.toIntermediateMetricPartials().isEmpty());
  }

  @Test
  public void manyBuckets_growPerBucketStateInStep() throws Exception {
    // Per-bucket state is an ObjectArray seeded at capacity 8. An unset slot is empty, so a resize
    // that dropped values would misreport extrema only once the bucket count passes that capacity.
    List<CityDoc> docs = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      docs.add(new CityDoc("city" + i, values(i)));
    }
    buildGroupedIndex(docs);

    Map<String, Optional<BsonValue>> minByCity = runGroupedByCity(minSpec());
    Map<String, Optional<BsonValue>> maxByCity = runGroupedByCity(maxSpec());

    assertEquals(30, minByCity.size());
    for (int i = 0; i < 30; i++) {
      assertEquals(Optional.of(new BsonInt64(i)), minByCity.get("city" + i));
      assertEquals(Optional.of(new BsonInt64(i)), maxByCity.get("city" + i));
    }
  }

  @Test
  public void zeroValue_isDistinguishedFromNoValue() throws Exception {
    // Per-bucket state uses null for "unset", so a real zero must not be treated as missing.
    buildInt64PriceIndex(List.of(values(0)));

    assertEquals(Optional.of(new BsonInt64(0)), minValue(runTopLevel(minSpec(), int64Context())));
    assertEquals(Optional.of(new BsonInt64(0)), maxValue(runTopLevel(maxSpec(), int64Context())));
  }

  @Test
  public void groupedByCity_keepsPerBucketExtremum() throws Exception {
    buildGroupedIndex(
        List.of(
            new CityDoc("NYC", values(10)),
            new CityDoc("NYC", values(30)),
            new CityDoc("LA", values(5)),
            new CityDoc("LA", values(15)),
            // Missing price contributes nothing, so LA's extrema stay 5 and 15.
            new CityDoc("LA", values())));

    Map<String, Optional<BsonValue>> minByCity = runGroupedByCity(minSpec());
    Map<String, Optional<BsonValue>> maxByCity = runGroupedByCity(maxSpec());

    assertEquals(Optional.of(new BsonInt64(10)), minByCity.get("NYC"));
    assertEquals(Optional.of(new BsonInt64(30)), maxByCity.get("NYC"));
    assertEquals(Optional.of(new BsonInt64(5)), minByCity.get("LA"));
    assertEquals(Optional.of(new BsonInt64(15)), maxByCity.get("LA"));
  }

  @Test
  public void groupedByCity_multiValuedMetricFoldsIntoItsBucket() throws Exception {
    buildGroupedIndex(List.of(new CityDoc("NYC", values(1)), new CityDoc("NYC", values(-1, 2, 4))));

    assertEquals(Optional.of(new BsonInt64(-1)), runGroupedByCity(minSpec()).get("NYC"));
    assertEquals(Optional.of(new BsonInt64(4)), runGroupedByCity(maxSpec()).get("NYC"));
  }

  @Test
  public void multiValuedGroupKey_lowersTheExtremumOfEveryBucketItJoins() throws Exception {
    // The fanned doc is cheaper than anything already in NYC and dearer than nothing in LA, so it
    // must move NYC's min and leave a visible mark in both buckets. This is the case that would
    // silently regress if the sum/avg skip were reinstated: NYC would report 200, not 100.
    buildGroupedIndex(
        List.of(
            new CityDoc("NYC", values(200)),
            new CityDoc("LA", values(500)),
            new CityDoc(List.of("NYC", "LA"), values(100))));

    Map<String, Optional<BsonValue>> minByCity = runGroupedByCity(minSpec());
    Map<String, Optional<BsonValue>> maxByCity = runGroupedByCity(maxSpec());

    assertEquals(Optional.of(new BsonInt64(100)), minByCity.get("NYC"));
    assertEquals(Optional.of(new BsonInt64(100)), minByCity.get("LA"));
    assertEquals(Optional.of(new BsonInt64(200)), maxByCity.get("NYC"));
    assertEquals(Optional.of(new BsonInt64(500)), maxByCity.get("LA"));
  }

  @Test
  public void groupedByCity_bucketWithNoValue_yieldsNull() throws Exception {
    buildGroupedIndex(List.of(new CityDoc("NYC", values(10)), new CityDoc("LA", values())));

    Map<String, Optional<BsonValue>> minByCity = runGroupedByCity(minSpec());

    assertEquals(Optional.of(new BsonInt64(10)), minByCity.get("NYC"));
    assertEquals(Optional.empty(), minByCity.get("LA"));
  }

  @Test
  public void multiValuedGroupKey_contributesToEveryBucket() throws Exception {
    // The doc fans into both LA and NYC. sum/avg skip such docs because adding the value to both
    // buckets double-counts, but selecting an extremum is idempotent: the doc belongs to both
    // buckets, so both must see its value. Skipping it here would report no min at all for two
    // buckets that plainly contain a document.
    buildMultiValueCityIndex(100);

    Map<String, Optional<BsonValue>> minByCity = runGroupedByCity(minSpec());
    Map<String, Optional<BsonValue>> maxByCity = runGroupedByCity(maxSpec());

    assertEquals(Optional.of(new BsonInt64(100)), minByCity.get("NYC"));
    assertEquals(Optional.of(new BsonInt64(100)), minByCity.get("LA"));
    assertEquals(Optional.of(new BsonInt64(100)), maxByCity.get("NYC"));
    assertEquals(Optional.of(new BsonInt64(100)), maxByCity.get("LA"));
  }

  @Test
  public void nestedGrouping_keepsExtremumPerLeafBucket() throws Exception {
    // terms(city) > terms(color) > min/max. Nested grouping is where a metric can most easily be
    // attributed to the wrong bucket ord, and no test on any path combined it with min/max.
    buildIndex(
        List.of(
            new TestDoc("NYC", "red", 10),
            new TestDoc("NYC", "red", 30),
            new TestDoc("NYC", "blue", 200),
            new TestDoc("LA", "red", 7)),
        Integer.MAX_VALUE);

    Map<String, Map<String, Optional<BsonValue>>> minByCityColor = runNestedByCityColor(minSpec());
    Map<String, Map<String, Optional<BsonValue>>> maxByCityColor = runNestedByCityColor(maxSpec());

    assertEquals(Optional.of(new BsonInt64(10)), minByCityColor.get("NYC").get("red"));
    assertEquals(Optional.of(new BsonInt64(30)), maxByCityColor.get("NYC").get("red"));
    assertEquals(Optional.of(new BsonInt64(200)), minByCityColor.get("NYC").get("blue"));
    assertEquals(Optional.of(new BsonInt64(200)), maxByCityColor.get("NYC").get("blue"));
    assertEquals(Optional.of(new BsonInt64(7)), minByCityColor.get("LA").get("red"));
    assertEquals(Optional.of(new BsonInt64(7)), maxByCityColor.get("LA").get("red"));
  }

  @Test
  public void multipleSegments_mergeExtremaAcrossLeaves() throws Exception {
    buildInt64PriceIndex(List.of(values(10), values(2), values(30)), 1);

    assertEquals(Optional.of(new BsonInt64(2)), minValue(runTopLevel(minSpec(), int64Context())));
    assertEquals(Optional.of(new BsonInt64(30)), maxValue(runTopLevel(maxSpec(), int64Context())));
  }

  @Test
  public void mergeBuilder_selectsExtremumAcrossPartitions() {
    MinAggregationResult left = new MinAggregationResult("m", Optional.of(new BsonInt64(10)));
    MinAggregationResult right = new MinAggregationResult("m", Optional.of(new BsonInt64(3)));
    assertEquals(
        Optional.of(new BsonInt64(3)), ((MinAggregationResult) merge(left, right)).getValue());

    MaxAggregationResult maxLeft = new MaxAggregationResult("m", Optional.of(new BsonInt64(10)));
    MaxAggregationResult maxRight = new MaxAggregationResult("m", Optional.of(new BsonInt64(3)));
    assertEquals(
        Optional.of(new BsonInt64(10)),
        ((MaxAggregationResult) merge(maxLeft, maxRight)).getValue());
  }

  @Test
  public void mergeBuilder_maxComparesAcrossNumericTypes() {
    MaxAggregationResult max =
        (MaxAggregationResult)
            merge(
                new MaxAggregationResult("m", Optional.of(new BsonInt64(5))),
                new MaxAggregationResult("m", Optional.of(new BsonDouble(9.5))));
    assertEquals(Optional.of(new BsonDouble(9.5)), max.getValue());
  }

  @Test
  public void mergeBuilder_ignoresNullPartials() {
    MinAggregationResult value = new MinAggregationResult("m", Optional.of(new BsonInt64(7)));
    MinAggregationResult empty = new MinAggregationResult("m", Optional.empty());

    assertEquals(
        Optional.of(new BsonInt64(7)), ((MinAggregationResult) merge(value, empty)).getValue());
    assertEquals(
        Optional.of(new BsonInt64(7)), ((MinAggregationResult) merge(empty, value)).getValue());
    assertEquals(Optional.empty(), ((MinAggregationResult) merge(empty, empty)).getValue());
  }

  @Test
  public void mergeBuilder_comparesAcrossNumericTypes() {
    // Partitions of the same field share a representation, but the merge builder is also the
    // cross-shard path, so it must order an int64 against a double correctly.
    MinAggregationResult min =
        (MinAggregationResult)
            merge(
                new MinAggregationResult("m", Optional.of(new BsonInt64(5))),
                new MinAggregationResult("m", Optional.of(new BsonDouble(2.5))));
    assertEquals(Optional.of(new BsonDouble(2.5)), min.getValue());
  }

  @Test
  public void mergeBuilder_nullIsSmallerThanNumber() {
    MinAggregationResult min =
        (MinAggregationResult)
            merge(
                new MinAggregationResult("m", Optional.of(BsonNull.VALUE)),
                new MinAggregationResult("m", Optional.of(new BsonInt64(5))));
    assertEquals(Optional.of(BsonNull.VALUE), min.getValue());

    MaxAggregationResult max =
        (MaxAggregationResult)
            merge(
                new MaxAggregationResult("m", Optional.of(BsonNull.VALUE)),
                new MaxAggregationResult("m", Optional.of(new BsonInt64(5))));
    assertEquals(Optional.of(new BsonInt64(5)), max.getValue());
  }

  @Test
  public void intermediatePartials_carryValueWhenPresent() {
    MinAggregationResult min = new MinAggregationResult("m", Optional.of(new BsonInt64(7)));
    assertEquals(new BsonDocument("min_m", new BsonInt64(7)), min.toIntermediateMetricPartials());

    MaxAggregationResult max = new MaxAggregationResult("m", Optional.of(new BsonInt64(7)));
    assertEquals(new BsonDocument("max_m", new BsonInt64(7)), max.toIntermediateMetricPartials());
  }

  @Test
  public void intermediatePartials_encodeNullAsMinKey() {
    // mongod $min/$max ignore BSON null, so a shard whose extremum is null would lose to any
    // non-null on another shard. MinKey is the stand-in; standalone still reports null.
    MinAggregationResult min = new MinAggregationResult("m", Optional.of(BsonNull.VALUE));
    assertEquals(new BsonDocument("min_m", new BsonMinKey()), min.toIntermediateMetricPartials());
    assertEquals(BsonNull.VALUE, min.toStandaloneBson());

    MaxAggregationResult max = new MaxAggregationResult("m", Optional.of(BsonNull.VALUE));
    assertEquals(new BsonDocument("max_m", new BsonMinKey()), max.toIntermediateMetricPartials());
    assertEquals(BsonNull.VALUE, max.toStandaloneBson());
  }

  @Test
  public void mixedTypes_selectAcrossBsonBrackets() throws Exception {
    BsonDateTime date = new BsonDateTime(1_700_000_000_000L);
    BsonObjectId oid = new BsonObjectId(new ObjectId("507f1f77bcf86cd799439011"));
    buildMixedPriceIndex(
        List.of(
            List.of(new BsonInt64(42)),
            List.of(new BsonString("apple")),
            List.of(BsonBoolean.TRUE),
            List.of(date),
            List.of(oid)));

    assertEquals(Optional.of(new BsonInt64(42)), minValue(runTopLevel(minSpec(), newContext())));
    assertEquals(Optional.of(date), maxValue(runTopLevel(maxSpec(), newContext())));
  }

  @Test
  public void mixedTypes_numericBracketMixesIntAndDouble() throws Exception {
    buildMixedPriceIndex(List.of(List.of(new BsonInt64(5)), List.of(new BsonDouble(2.5))));

    assertEquals(Optional.of(new BsonDouble(2.5)), minValue(runTopLevel(minSpec(), newContext())));
    assertEquals(Optional.of(new BsonInt64(5)), maxValue(runTopLevel(maxSpec(), newContext())));
  }

  @Test
  public void mixedTypes_multiValuedArrayWithinAndAcrossTypes() throws Exception {
    // One doc carries [3, 8] int64 and ["a", "z"] token. Within each column the selector collapses
    // to that column's extremum; across columns BSON order picks 3 as min and "z" as max.
    buildMixedPriceIndex(
        List.of(
            List.of(new BsonInt64(3), new BsonInt64(8), new BsonString("a"), new BsonString("z"))));

    assertEquals(Optional.of(new BsonInt64(3)), minValue(runTopLevel(minSpec(), newContext())));
    assertEquals(Optional.of(new BsonString("z")), maxValue(runTopLevel(maxSpec(), newContext())));
  }

  @Test
  public void mixedTypes_nullParticipatesInBsonOrder() throws Exception {
    buildMixedPriceIndex(List.of(List.of(BsonNull.VALUE), List.of(new BsonInt64(5))));

    assertEquals(Optional.of(BsonNull.VALUE), minValue(runTopLevel(minSpec(), newContext())));
    assertEquals(Optional.of(new BsonInt64(5)), maxValue(runTopLevel(maxSpec(), newContext())));
  }

  @Test
  public void mixedTypes_nanBelowAllNumbers() throws Exception {
    buildMixedPriceIndex(
        List.of(
            List.of(new BsonDouble(Double.NaN)),
            List.of(new BsonInt64(-1)),
            List.of(new BsonString("z"))));

    Optional<BsonValue> min = minValue(runTopLevel(minSpec(), newContext()));
    assertTrue(Double.isNaN(((BsonDouble) min.orElseThrow()).getValue()));
    assertEquals(Optional.of(new BsonString("z")), maxValue(runTopLevel(maxSpec(), newContext())));
  }

  @Test
  public void mixedTypes_stringOrderingUsesTermOrder() throws Exception {
    buildMixedPriceIndex(
        List.of(
            List.of(new BsonString("banana")),
            List.of(new BsonString("apple")),
            List.of(new BsonString("cherry"))));

    assertEquals(
        Optional.of(new BsonString("apple")), minValue(runTopLevel(minSpec(), newContext())));
    assertEquals(
        Optional.of(new BsonString("cherry")), maxValue(runTopLevel(maxSpec(), newContext())));
  }

  @Test
  public void mixedTypes_booleanFalseBeforeTrue() throws Exception {
    buildMixedPriceIndex(List.of(List.of(BsonBoolean.TRUE), List.of(BsonBoolean.FALSE)));

    assertEquals(Optional.of(BsonBoolean.FALSE), minValue(runTopLevel(minSpec(), newContext())));
    assertEquals(Optional.of(BsonBoolean.TRUE), maxValue(runTopLevel(maxSpec(), newContext())));
  }

  @Test
  public void mixedTypes_objectIdOrdering() throws Exception {
    BsonObjectId low = new BsonObjectId(new ObjectId("0".repeat(24)));
    BsonObjectId high = new BsonObjectId(new ObjectId("f".repeat(24)));
    buildMixedPriceIndex(List.of(List.of(high), List.of(low)));

    assertEquals(Optional.of(low), minValue(runTopLevel(minSpec(), newContext())));
    assertEquals(Optional.of(high), maxValue(runTopLevel(maxSpec(), newContext())));
  }

  @Test
  public void mixedTypes_binaryUuidOrdering() throws Exception {
    BsonBinary low = new BsonBinary(UUID.fromString("11111111-1111-1111-1111-111111111111"));
    BsonBinary high = new BsonBinary(UUID.fromString("22222222-2222-2222-2222-222222222222"));
    buildMixedPriceIndex(List.of(List.of(high), List.of(low)));

    assertEquals(Optional.of(low), minValue(runTopLevel(minSpec(), newContext())));
    assertEquals(Optional.of(high), maxValue(runTopLevel(maxSpec(), newContext())));
  }

  @Test
  public void mixedTypes_underTermsGrouping() throws Exception {
    buildMixedGroupedIndex(
        List.of(
            new CityMixedDoc("NYC", List.of(new BsonInt64(10), new BsonString("z"))),
            new CityMixedDoc("NYC", List.of(new BsonInt64(3))),
            new CityMixedDoc("LA", List.of(BsonBoolean.TRUE)),
            new CityMixedDoc("LA", List.of(BsonBoolean.FALSE))));

    Map<String, Optional<BsonValue>> minByCity = runGroupedByCity(minSpec());
    Map<String, Optional<BsonValue>> maxByCity = runGroupedByCity(maxSpec());

    assertEquals(Optional.of(new BsonInt64(3)), minByCity.get("NYC"));
    assertEquals(Optional.of(new BsonString("z")), maxByCity.get("NYC"));
    assertEquals(Optional.of(BsonBoolean.FALSE), minByCity.get("LA"));
    assertEquals(Optional.of(BsonBoolean.TRUE), maxByCity.get("LA"));
  }

  @Test
  public void mixedTypes_missingInSomeColumns() throws Exception {
    buildMixedPriceIndex(List.of(List.of(new BsonString("only-token")), List.of(new BsonInt64(7))));

    assertEquals(Optional.of(new BsonInt64(7)), minValue(runTopLevel(minSpec(), newContext())));
    assertEquals(
        Optional.of(new BsonString("only-token")), maxValue(runTopLevel(maxSpec(), newContext())));
  }

  @Test
  public void mixedTypes_acrossSegments_matchesSingleSegment() throws Exception {
    List<List<BsonValue>> docs =
        List.of(
            List.of(new BsonInt64(10)),
            List.of(new BsonString("apple")),
            List.of(BsonBoolean.TRUE),
            List.of(new BsonDateTime(50)));

    buildMixedPriceIndex(docs);
    Optional<BsonValue> singleMin = minValue(runTopLevel(minSpec(), newContext()));
    Optional<BsonValue> singleMax = maxValue(runTopLevel(maxSpec(), newContext()));
    this.reader.close();
    this.directory.close();

    buildMixedPriceIndex(docs, 1);
    assertEquals(singleMin, minValue(runTopLevel(minSpec(), newContext())));
    assertEquals(singleMax, maxValue(runTopLevel(maxSpec(), newContext())));
  }

  @Test
  public void mixedTypes_largeInt64VsDouble_pinsSortUtilBehavior() throws Exception {
    // 2^53+1 cannot be represented as a double. SortUtil promotes the int64, treating the two as
    // equal, so the first-seen value wins both min and max. MqlComparator (merge) would consider
    // the int64 strictly larger. Collection follows the sort path; pin that here.
    long justAbovePrecise = (1L << 53) + 1;
    buildMixedPriceIndex(
        List.of(List.of(new BsonInt64(justAbovePrecise)), List.of(new BsonDouble(1L << 53))));

    assertEquals(
        Optional.of(new BsonInt64(justAbovePrecise)),
        minValue(runTopLevel(minSpec(), newContext())));
    assertEquals(
        Optional.of(new BsonInt64(justAbovePrecise)),
        maxValue(runTopLevel(maxSpec(), newContext())));
  }

  // ===== helpers =====

  private static long[] values(long... values) {
    return values;
  }

  private static MinAggregationSpec minSpec() {
    return new MinAggregationSpec("m", FieldPath.parse(PRICE_FIELD));
  }

  private static MaxAggregationSpec maxSpec() {
    return new MaxAggregationSpec("m", FieldPath.parse(PRICE_FIELD));
  }

  private static Optional<BsonValue> minValue(AggregationResult result) {
    return ((MinAggregationResult) result).getValue();
  }

  private static Optional<BsonValue> maxValue(AggregationResult result) {
    return ((MaxAggregationResult) result).getValue();
  }

  private AggregationContext int64Context() {
    return newContextWithFieldResolver();
  }

  private AggregationContext doubleContext() {
    return newContextWithDoubleFieldResolver();
  }

  private AggregationResult runTopLevel(OperatorAggregationSpec spec, AggregationContext context)
      throws IOException {
    TopAggregationSpec top = new TopAggregationSpec(List.of(spec));
    Accumulator accumulator = collectAll(top, context);
    TopLevelAggregationResult result =
        (TopLevelAggregationResult) accumulator.buildTopLevelResult();
    return result.childResults().get(0);
  }

  /** Runs {@code spec} under a terms grouping on {@code city}, returning value-by-bucket-key. */
  private Map<String, Optional<BsonValue>> runGroupedByCity(OperatorAggregationSpec spec)
      throws IOException {
    AggregationSpec byCity =
        new TermsGroupAggregationSpec(
            "by_city",
            FieldPath.parse(CITY_FIELD),
            List.of(spec),
            new TermsArgs(true, TypeField.TOKEN, 100));
    Accumulator accumulator = collectAll(new TopAggregationSpec(List.of(byCity)), int64Context());
    TopLevelAggregationResult top = (TopLevelAggregationResult) accumulator.buildTopLevelResult();

    TermsAggregationResult terms = (TermsAggregationResult) top.childResults().get(0);
    Map<String, Optional<BsonValue>> valueByCity = new LinkedHashMap<>();
    for (MultiBucketAggregationResult.Bucket rawBucket : terms.getBuckets(BucketOrder.KEY_ASC)) {
      TermsAggregationResult.TermsBucket bucket = (TermsAggregationResult.TermsBucket) rawBucket;
      MetricAggregationResult metric =
          (MetricAggregationResult) bucket.getChildResults().get(spec.name());
      valueByCity.put(
          bucket.getKey().getValue(),
          metric instanceof MinAggregationResult min
              ? min.getValue()
              : ((MaxAggregationResult) metric).getValue());
    }
    return valueByCity;
  }

  /** Runs {@code spec} under terms(city) > terms(color), returning value-by-city-by-color. */
  private Map<String, Map<String, Optional<BsonValue>>> runNestedByCityColor(
      OperatorAggregationSpec spec) throws IOException {
    AggregationSpec byColor =
        new TermsGroupAggregationSpec(
            "by_color",
            FieldPath.parse(COLOR_FIELD),
            List.of(spec),
            new TermsArgs(false, TypeField.TOKEN, 100));
    AggregationSpec byCity =
        new TermsGroupAggregationSpec(
            "by_city",
            FieldPath.parse(CITY_FIELD),
            List.of(byColor),
            new TermsArgs(true, TypeField.TOKEN, 100));

    Accumulator accumulator = collectAll(new TopAggregationSpec(List.of(byCity)), int64Context());
    TopLevelAggregationResult top = (TopLevelAggregationResult) accumulator.buildTopLevelResult();

    Map<String, Map<String, Optional<BsonValue>>> byCityColor = new LinkedHashMap<>();
    TermsAggregationResult cityTerms = (TermsAggregationResult) top.childResults().get(0);
    for (MultiBucketAggregationResult.Bucket rawCity : cityTerms.getBuckets(BucketOrder.KEY_ASC)) {
      TermsAggregationResult.TermsBucket cityBucket = (TermsAggregationResult.TermsBucket) rawCity;
      Map<String, Optional<BsonValue>> byColor2 = new LinkedHashMap<>();
      TermsAggregationResult colorTerms =
          (TermsAggregationResult) cityBucket.getChildResults().get("by_color");
      for (MultiBucketAggregationResult.Bucket rawColor :
          colorTerms.getBuckets(BucketOrder.KEY_ASC)) {
        TermsAggregationResult.TermsBucket colorBucket =
            (TermsAggregationResult.TermsBucket) rawColor;
        MetricAggregationResult metric =
            (MetricAggregationResult) colorBucket.getChildResults().get(spec.name());
        byColor2.put(
            colorBucket.getKey().getValue(),
            metric instanceof MinAggregationResult min
                ? min.getValue()
                : ((MaxAggregationResult) metric).getValue());
      }
      byCityColor.put(cityBucket.getKey().getValue(), byColor2);
    }
    return byCityColor;
  }

  /** One grouped document: a city group key plus zero or more {@code price} doc values. */
  private record CityDoc(List<String> cities, long[] prices) {
    CityDoc(String city, long[] prices) {
      this(List.of(city), prices);
    }
  }

  private void buildInt64PriceIndex(List<long[]> docs) throws IOException {
    buildInt64PriceIndex(docs, Integer.MAX_VALUE);
  }

  private void buildInt64PriceIndex(List<long[]> docs, int docsPerSegment) throws IOException {
    this.directory = new ByteBuffersDirectory();
    IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(this.directory, config)) {
      for (int i = 0; i < docs.size(); i++) {
        Document luceneDoc = new Document();
        for (long price : docs.get(i)) {
          luceneDoc.add(new SortedNumericDocValuesField(PRICE_LUCENE_FIELD, price));
        }
        writer.addDocument(luceneDoc);
        if ((i + 1) % docsPerSegment == 0) {
          writer.commit();
        }
      }
    }
    openReader();
  }

  private void buildDoublePriceIndex(List<List<Double>> docs) throws IOException {
    this.directory = new ByteBuffersDirectory();
    IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(this.directory, config)) {
      for (List<Double> doc : docs) {
        Document luceneDoc = new Document();
        for (double price : doc) {
          luceneDoc.add(
              new SortedNumericDocValuesField(
                  DOUBLE_PRICE_LUCENE_FIELD, LuceneDoubleConversionUtils.toMqlSortableLong(price)));
        }
        writer.addDocument(luceneDoc);
      }
    }
    openReader();
  }

  private void buildGroupedIndex(List<CityDoc> docs) throws IOException {
    this.directory = new ByteBuffersDirectory();
    IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(this.directory, config)) {
      for (CityDoc doc : docs) {
        Document luceneDoc = new Document();
        for (String city : doc.cities()) {
          luceneDoc.add(new SortedSetDocValuesField(CITY_LUCENE_FIELD, new BytesRef(city)));
        }
        for (long price : doc.prices()) {
          luceneDoc.add(new SortedNumericDocValuesField(PRICE_LUCENE_FIELD, price));
        }
        writer.addDocument(luceneDoc);
      }
    }
    openReader();
  }

  /** A single document carrying two city tokens, so the group key fans it into both buckets. */
  private void buildMultiValueCityIndex(long price) throws IOException {
    buildIndexWithMultiValueCityDoc(price);
  }

  private void openReader() throws IOException {
    this.reader = DirectoryReader.open(this.directory);
    this.searcher = new IndexSearcher(this.reader);
  }

  private record CityMixedDoc(List<String> cities, List<BsonValue> values) {
    CityMixedDoc(String city, List<BsonValue> values) {
      this(List.of(city), values);
    }
  }

  private void buildMixedPriceIndex(List<List<BsonValue>> docs) throws IOException {
    buildMixedPriceIndex(docs, Integer.MAX_VALUE);
  }

  private void buildMixedPriceIndex(List<List<BsonValue>> docs, int docsPerSegment)
      throws IOException {
    this.directory = new ByteBuffersDirectory();
    IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(this.directory, config)) {
      for (int i = 0; i < docs.size(); i++) {
        Document luceneDoc = new Document();
        for (BsonValue value : docs.get(i)) {
          addTypedValue(luceneDoc, value);
        }
        writer.addDocument(luceneDoc);
        if ((i + 1) % docsPerSegment == 0) {
          writer.commit();
        }
      }
    }
    openReader();
  }

  private void buildMixedGroupedIndex(List<CityMixedDoc> docs) throws IOException {
    this.directory = new ByteBuffersDirectory();
    IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(this.directory, config)) {
      for (CityMixedDoc doc : docs) {
        Document luceneDoc = new Document();
        for (String city : doc.cities()) {
          luceneDoc.add(new SortedSetDocValuesField(CITY_LUCENE_FIELD, new BytesRef(city)));
        }
        for (BsonValue value : doc.values()) {
          addTypedValue(luceneDoc, value);
        }
        writer.addDocument(luceneDoc);
      }
    }
    openReader();
  }

  private static void addTypedValue(Document luceneDoc, BsonValue value) {
    FieldPath path = FieldPath.parse(PRICE_FIELD);
    switch (value.getBsonType()) {
      case INT64 ->
          luceneDoc.add(
              new SortedNumericDocValuesField(
                  TypeField.NUMBER_INT64_V2.getLuceneFieldName(path, Optional.empty()),
                  value.asInt64().getValue()));
      case DOUBLE ->
          luceneDoc.add(
              new SortedNumericDocValuesField(
                  TypeField.NUMBER_DOUBLE_V2.getLuceneFieldName(path, Optional.empty()),
                  LuceneDoubleConversionUtils.toMqlSortableLong(value.asDouble().getValue())));
      case STRING ->
          luceneDoc.add(
              new SortedSetDocValuesField(
                  TypeField.TOKEN.getLuceneFieldName(path, Optional.empty()),
                  new BytesRef(value.asString().getValue())));
      case BOOLEAN ->
          luceneDoc.add(
              new SortedSetDocValuesField(
                  TypeField.BOOLEAN.getLuceneFieldName(path, Optional.empty()),
                  new BytesRef(FieldValue.fromBoolean(value.asBoolean().getValue()))));
      case DATE_TIME ->
          luceneDoc.add(
              new SortedNumericDocValuesField(
                  TypeField.DATE_V2.getLuceneFieldName(path, Optional.empty()),
                  value.asDateTime().getValue()));
      case OBJECT_ID ->
          luceneDoc.add(
              new SortedSetDocValuesField(
                  TypeField.OBJECT_ID.getLuceneFieldName(path, Optional.empty()),
                  new BytesRef(value.asObjectId().getValue().toByteArray())));
      case BINARY ->
          luceneDoc.add(
              new SortedSetDocValuesField(
                  TypeField.UUID.getLuceneFieldName(path, Optional.empty()),
                  new BytesRef(value.asBinary().asUuid().toString())));
      case NULL ->
          luceneDoc.add(
              new SortedDocValuesField(
                  TypeField.NULL.getLuceneFieldName(path, Optional.empty()),
                  new BytesRef(FieldValue.NULL_FIELD_VALUE)));
      default -> throw new IllegalArgumentException("unsupported test value: " + value);
    }
  }
}
