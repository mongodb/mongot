package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.Test;

/**
 * Validates both halves of the {@code first}/{@code last} cross-shard contract: the partials a
 * shard writes onto a leaf bucket row, and the merge stages built from the query that mongod reads
 * them back with.
 *
 * <p>The two are asserted together on purpose -- a field name is only right if both sides agree on
 * it -- and pipeline BSON is asserted verbatim so an accidental change to either is caught.
 */
public class FirstLastShardMergeTest {

  private static final FieldPath CITY = FieldPath.parse("city");
  private static final FieldPath COLOR = FieldPath.parse("color");
  private static final FieldPath PRICE = FieldPath.parse("price");

  /** Price ascending, then rating descending, as the query layer would have built it. */
  private static final Sort COMPOUND_SORT =
      new Sort(
          new SortField("price", SortField.Type.LONG),
          new SortField("rating", SortField.Type.LONG, true));

  private static final List<Integer> COMPOUND_DIRECTIONS = List.of(1, -1);

  @Test
  public void sortedFirstPartials_carryValueAndPositionalSortKey() {
    FirstAggregationResult result =
        new FirstAggregationResult(
            "cheapest", Optional.of(COMPOUND_SORT), winner(new BsonInt64(10), new BsonInt64(4)));

    assertEquals(
        BsonDocument.parse(
            """
            {"first_has_cheapest": true, "first_value_cheapest": "red",
             "first_key_cheapest": {"_0": {"$numberLong": "10"}, "_1": {"$numberLong": "4"}}}
            """),
        result.toIntermediateMetricPartials());
  }

  /**
   * {@code last} collects under the inverted sort, but the key it emits is the query's way round --
   * raw sort values carry no direction -- so both operators put the same numbers on the wire, and
   * only the merge accumulator differs.
   */
  @Test
  public void sortedLastPartials_carryTheSameKeyAsFirst() {
    Sort invertedSort =
        new Sort(
            new SortField("price", SortField.Type.LONG, true),
            new SortField("rating", SortField.Type.LONG));
    LastAggregationResult result =
        new LastAggregationResult(
            "dearest", Optional.of(invertedSort), winner(new BsonInt64(10), new BsonInt64(4)));

    assertEquals(
        BsonDocument.parse(
            """
            {"last_has_dearest": true, "last_value_dearest": "red",
             "last_key_dearest": {"_0": {"$numberLong": "10"}, "_1": {"$numberLong": "4"}}}
            """),
        result.toIntermediateMetricPartials());
  }

  @Test
  public void unsortedPartials_carryTheValueAlone() {
    FirstAggregationResult result =
        new FirstAggregationResult("newest", Optional.empty(), winner());

    assertEquals(
        new BsonDocument("first_has_newest", BsonBoolean.TRUE)
            .append("first_value_newest", new BsonString("red")),
        result.toIntermediateMetricPartials());
  }

  /** A winning document with nothing at the metric's path reports null, as it does standalone. */
  @Test
  public void missingValuePartial_isExplicitNull() {
    FirstAggregationResult result =
        new FirstAggregationResult(
            "newest",
            Optional.empty(),
            Optional.of(
                new FirstLastAggregationResultBase.Candidate(
                    0, new FieldDoc(7, 0f, new Object[0]), Optional.empty())));

    assertEquals(
        new BsonDocument("first_has_newest", BsonBoolean.TRUE)
            .append("first_value_newest", BsonNull.VALUE),
        result.toIntermediateMetricPartials());
  }

  /**
   * A {@code $nullness} component is dropped and the remaining components close the gap, so the key
   * lines up with the query's own sort fields -- which is all the merge pipeline is built from.
   */
  @Test
  public void nullnessExpandedSort_emitsOneComponentPerQuerySortField() {
    Sort expandedSort =
        new Sort(
            new SortField("$meta/nullness/price", SortField.Type.LONG),
            new SortField("price", SortField.Type.LONG),
            new SortField("$meta/nullness/rating", SortField.Type.LONG, true),
            new SortField("rating", SortField.Type.LONG, true));
    FirstAggregationResult result =
        new FirstAggregationResult(
            "cheapest",
            Optional.of(expandedSort),
            winner(new BsonInt64(0), new BsonInt64(10), new BsonInt64(0), new BsonInt64(4)));

    assertEquals(
        BsonDocument.parse(
            """
            {"first_has_cheapest": true, "first_value_cheapest": "red",
             "first_key_cheapest": {"_0": {"$numberLong": "10"}, "_1": {"$numberLong": "4"}}}
            """),
        result.toIntermediateMetricPartials());
  }

  /**
   * Sort keys reach the wire as order-preserving BSON so mongod can compare two shards' keys
   * directly. A raw Lucene value would be silently unmergeable, so it fails loudly instead.
   */
  @Test
  public void nonBsonSortKeyComponent_throws() {
    FirstAggregationResult result =
        new FirstAggregationResult(
            "cheapest",
            Optional.of(new Sort(new SortField("price", SortField.Type.LONG))),
            Optional.of(
                new FirstLastAggregationResultBase.Candidate(
                    0,
                    new FieldDoc(7, 0f, new Object[] {10L}),
                    Optional.of(new BsonString("red")))));

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, result::toIntermediateMetricPartials);
    assertEquals("Sort key component 0 is not a BsonValue: 10", exception.getMessage());
  }

  /** A bucket without a winner has no key to order it by, so it must not reach the wire. */
  @Test
  public void bucketWithoutAWinner_announcesItHoldsNoCandidate() {
    // Boundary grouping emits every bucket on every shard, so an empty range legitimately reaches
    // the wire. The row still carries the bucket's doc count, so it says it has no candidate and
    // the merge sorts it behind the shards that do rather than letting a keyless row win a $top.
    FirstAggregationResult result =
        new FirstAggregationResult("cheapest", Optional.of(COMPOUND_SORT), Optional.empty());

    assertEquals(
        BsonDocument.parse(
            """
            {"first_has_cheapest": false}
            """),
        result.toIntermediateMetricPartials());
  }

  @Test
  public void sortedFirstMergePipeline_takesTheTopKeyPerBucket() {
    List<BsonDocument> pipeline =
        pipelineFor(new FirstAggregationSpec("cheapest", COLOR), COMPOUND_DIRECTIONS);

    assertEquals(
        BsonDocument.parse(
            """
            {"$group": {
              "_id": {
                "group_name": {"$getField": {
                  "field": {"$literal": "$meta/group_name"}, "input": "$$ROOT"}},
                "keys": {"$getField": {"field": {"$literal": "$meta/keys"}, "input": "$$ROOT"}}
              },
              "count_value": {"$sum": "$count"},
              "m3": {"$top": {
                "output": {"$ifNull": ["$m0", null]},
                "sortBy": {
                  "m1": -1,
                  "m2._0": 1,
                  "m2._1": -1
                }
              }}
            }}
            """),
        pipeline.get(0));
    assertEquals(new BsonString("$m3"), leafProjection(pipeline, "cheapest"));
  }

  /** The same key read from the other end: {@code last} wants the bottom document under it. */
  @Test
  public void sortedLastMergePipeline_takesTheBottomKeyPerBucket() {
    List<BsonDocument> pipeline =
        pipelineFor(new LastAggregationSpec("dearest", COLOR), COMPOUND_DIRECTIONS);

    assertEquals(
        BsonDocument.parse(
            """
            {"$bottom": {
              "output": {"$ifNull": ["$m0", null]},
              "sortBy": {
                "m1": 1,
                "m2._0": 1,
                "m2._1": -1
              }
            }}
            """),
        mergeAccumulator(pipeline, "m3"));
    assertEquals(new BsonString("$m3"), leafProjection(pipeline, "dearest"));
  }

  /**
   * With no sort every candidate is an equally good answer, so the merge only has to pick one of
   * the shards' winners rather than compare a key it was never given.
   */
  @Test
  public void unsortedMergePipeline_picksAShardWinnerArbitrarily() {
    assertEquals(
        BsonDocument.parse(
            """
            {"$top": {
              "output": {"$ifNull": ["$m0", null]},
              "sortBy": {"m1": -1}
            }}
            """),
        mergeAccumulator(pipelineFor(new FirstAggregationSpec("newest", COLOR), List.of()), "m2"));
    assertEquals(
        BsonDocument.parse(
            """
            {"$bottom": {
              "output": {"$ifNull": ["$m0", null]},
              "sortBy": {"m1": 1}
            }}
            """),
        mergeAccumulator(pipelineFor(new LastAggregationSpec("oldest", COLOR), List.of()), "m2"));
  }

  /**
   * Under nested grouping the merged winner is pushed through the facet-arm {@code $group} by name
   * like any other partial, and the projection reading it is rescoped to the row variable, so a
   * winning value survives bucket reconstruction rather than being dropped at the outer level.
   */
  @Test
  public void nestedGrouping_pushesTheMergedWinnerThroughBucketReconstruction() {
    AggregationSpec leaf = new FirstAggregationSpec("cheapest", PRICE);
    AggregationSpec byColor = new TermsSpec("by_color", COLOR, List.of(leaf));
    AggregationSpec byCity = new TermsSpec("by_city", CITY, List.of(byColor));

    TopAggregationSpec top = new TopAggregationSpec(List.of(byCity));
    List<BsonDocument> pipeline = top.newShardMergePipelineBuilder(List.of(1)).build("lowerBound");

    BsonArray expectedByCityArm =
        BsonArray.parse(
            """
            [
              {"$match": {"$expr": {"$eq": [
                {"$getField": {"field": "group_name", "input": "$_id"}},
                "f0"
              ]}}},
              {"$group": {
                "_id": {"$getField": {"field": "k0",
                  "input": {"$getField": {"field": "keys", "input": "$_id"}}}},
                "count_value": {"$sum": "$count_value"},
                "_rows": {"$push": {
                  "k1": {"$getField": {"field": "k1",
                    "input": {"$getField": {"field": "keys", "input": "$_id"}}}},
                  "count_value": "$count_value",
                  "m3": "$m3"
                }}
              }},
              {"$project": {
                "_id": "$_id",
                "by_color": {"buckets": {"$map": {
                  "input": "$_rows",
                  "as": "row",
                  "in": {
                    "_id": "$$row.k1",
                    "cheapest": "$$row.m3"
                  }
                }}}
              }}
            ]
            """);
    assertEquals(expectedByCityArm, pipeline.get(1).getDocument("$facet").getArray("f0"));
  }

  private static List<BsonDocument> pipelineFor(AggregationSpec leaf, List<Integer> directions) {
    TopAggregationSpec top =
        new TopAggregationSpec(List.of(new TermsSpec("by_city", CITY, List.of(leaf))));
    return top.newShardMergePipelineBuilder(directions).build("lowerBound");
  }

  private static BsonDocument mergeAccumulator(List<BsonDocument> pipeline, String field) {
    return pipeline.get(0).getDocument("$group").getDocument(field);
  }

  private static BsonValue leafProjection(List<BsonDocument> pipeline, String name) {
    BsonArray arm = pipeline.get(1).getDocument("$facet").getArray("f0");
    return arm.get(arm.size() - 1).asDocument().getDocument("$project").get(name);
  }

  /** A winner holding {@code "red"} at the metric's path, with the given sort key. */
  private static Optional<FirstLastAggregationResultBase.Candidate> winner(BsonValue... sortKey) {
    return Optional.of(
        new FirstLastAggregationResultBase.Candidate(
            0, new FieldDoc(7, 0f, sortKey), Optional.of(new BsonString("red"))));
  }

  private static final class TermsSpec extends GroupAggregationSpec {
    TermsSpec(String name, FieldPath path, List<AggregationSpec> children) {
      super(path, name, children);
    }

    @Override
    protected Accumulator createAccumulator(
        AggregationContext context, List<Accumulator> childAccumulators) {
      throw new UnsupportedOperationException("not exercised in pipeline test");
    }
  }
}
