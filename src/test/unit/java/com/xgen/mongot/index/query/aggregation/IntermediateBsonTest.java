package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.Test;

/** Validates {@link AggregationResult#toIntermediateBson()} wire format for sharded execution. */
public class IntermediateBsonTest extends AggregationTestSupport {

  private static final FieldPath CITY = FieldPath.parse("city");
  private static final FieldPath COLOR = FieldPath.parse("color");

  @Test
  public void flatSingleLevelGroup_emitsGroupNamedRowsWithKeysAndSumPartials() {
    TermsResult byCity =
        new TermsResult(
            "by_city",
            CITY,
            List.of(
                new TermsResult.TermsBucket(
                    "NYC",
                    AggregationResults.from(List.of(new SumAggregationResult("sum_price", 30.0)))),
                new TermsResult.TermsBucket(
                    "SF",
                    AggregationResults.from(
                        List.of(new SumAggregationResult("sum_price", 50.0))))));

    BsonArray intermediate = new TopLevelAggregationResult(List.of(byCity)).toIntermediateBson();

    BsonArray expected =
        BsonArray.parse(
            """
            [
              {"$meta/group_name": "by_city", "$meta/keys": {"city": "NYC"}, "sum_sum_price": 30.0},
              {"$meta/group_name": "by_city", "$meta/keys": {"city": "SF"}, "sum_sum_price": 50.0}
            ]
            """);
    assertEquals(expected, intermediate);
  }

  @Test
  public void flatSingleLevelGroup_emitsMinAndMaxPartials() {
    TermsResult byCity =
        new TermsResult(
            "by_city",
            CITY,
            List.of(
                new TermsResult.TermsBucket(
                    "NYC",
                    AggregationResults.from(
                        List.of(
                            new MinAggregationResult("m", Optional.of(new BsonInt64(10))),
                            new MaxAggregationResult("x", Optional.of(new BsonInt64(50))))))));

    BsonArray intermediate = new TopLevelAggregationResult(List.of(byCity)).toIntermediateBson();

    BsonArray expected =
        BsonArray.parse(
            """
            [
              {
                "$meta/group_name": "by_city",
                "$meta/keys": {"city": "NYC"},
                "min_m": {"$numberLong": "10"},
                "max_x": {"$numberLong": "50"}
              }
            ]
            """);
    assertEquals(expected, intermediate);
  }

  @Test
  public void bucketWithNoValue_omitsMinAndMaxPartialsEntirely() {
    // The cross-shard merge runs $min/$max over these partials. An absent partial is skipped, but
    // an explicit null would be compared -- and BSON null sorts below every number, so a shard that
    // saw nothing would hijack every other shard's minimum. The omission is the whole contract.
    TermsResult byCity =
        new TermsResult(
            "by_city",
            CITY,
            List.of(
                new TermsResult.TermsBucket(
                    "NYC",
                    AggregationResults.from(
                        List.of(
                            new MinAggregationResult("m", Optional.empty()),
                            new MaxAggregationResult("x", Optional.empty()))))));

    BsonArray intermediate = new TopLevelAggregationResult(List.of(byCity)).toIntermediateBson();

    BsonArray expected =
        BsonArray.parse(
            """
            [{"$meta/group_name": "by_city", "$meta/keys": {"city": "NYC"}}]
            """);
    assertEquals(expected, intermediate);
  }

  @Test
  public void nestedDepth2Group_flattensKeysIntoLeafRows() {
    TermsResult nycColors =
        new TermsResult(
            "by_color",
            COLOR,
            List.of(
                new TermsResult.TermsBucket(
                    "red",
                    AggregationResults.from(
                        List.of(new AvgAggregationResult("avg_price", 30.0, 2)))),
                new TermsResult.TermsBucket(
                    "blue",
                    AggregationResults.from(
                        List.of(new AvgAggregationResult("avg_price", 30.0, 1))))));

    TermsResult laColors =
        new TermsResult(
            "by_color",
            COLOR,
            List.of(
                new TermsResult.TermsBucket(
                    "red",
                    AggregationResults.from(
                        List.of(new AvgAggregationResult("avg_price", 5.0, 1)))),
                new TermsResult.TermsBucket(
                    "green",
                    AggregationResults.from(
                        List.of(new AvgAggregationResult("avg_price", 15.0, 1))))));

    TermsResult byCity =
        new TermsResult(
            "by_city",
            CITY,
            List.of(
                new TermsResult.TermsBucket("NYC", AggregationResults.from(List.of(nycColors))),
                new TermsResult.TermsBucket("LA", AggregationResults.from(List.of(laColors)))));

    BsonArray intermediate = new TopLevelAggregationResult(List.of(byCity)).toIntermediateBson();

    BsonArray expected =
        BsonArray.parse(
            """
            [
              {
                "$meta/group_name": "by_city",
                "$meta/keys": {"city": "NYC", "color": "red"},
                "avg_sum_avg_price": 30.0,
                "avg_count_avg_price": {"$numberLong": "2"}
              },
              {
                "$meta/group_name": "by_city",
                "$meta/keys": {"city": "NYC", "color": "blue"},
                "avg_sum_avg_price": 30.0,
                "avg_count_avg_price": {"$numberLong": "1"}
              },
              {
                "$meta/group_name": "by_city",
                "$meta/keys": {"city": "LA", "color": "red"},
                "avg_sum_avg_price": 5.0,
                "avg_count_avg_price": {"$numberLong": "1"}
              },
              {
                "$meta/group_name": "by_city",
                "$meta/keys": {"city": "LA", "color": "green"},
                "avg_sum_avg_price": 15.0,
                "avg_count_avg_price": {"$numberLong": "1"}
              }
            ]
            """);
    assertEquals(expected, intermediate);
  }

  @Test
  public void multipleTopLevelBranches_groupNamesDoNotCrossContaminate() {
    TermsResult byCity =
        new TermsResult(
            "by_city",
            CITY,
            List.of(
                new TermsResult.TermsBucket(
                    "NYC",
                    AggregationResults.from(
                        List.of(new SumAggregationResult("sum_price", 10.0))))));

    TermsResult byColor =
        new TermsResult(
            "by_color",
            COLOR,
            List.of(
                new TermsResult.TermsBucket(
                    "red",
                    AggregationResults.from(
                        List.of(new SumAggregationResult("sum_price", 35.0))))));

    BsonArray intermediate =
        new TopLevelAggregationResult(List.of(byCity, byColor)).toIntermediateBson();

    BsonArray expected =
        BsonArray.parse(
            """
            [
              {"$meta/group_name": "by_city", "$meta/keys": {"city": "NYC"}, \
              "sum_sum_price": 10.0},
              {"$meta/group_name": "by_color", "$meta/keys": {"color": "red"}, \
              "sum_sum_price": 35.0}
            ]
            """);
    assertEquals(expected, intermediate);
  }

  @Test
  public void topLevelStamping_rejectsConflictingRoutingFieldInChildDoc() {
    AggregationResult childWithConflictingField =
        new AggregationResult() {
          @Override
          public String getName() {
            return "by_city";
          }

          @Override
          public AggregationResultBuilder toBuilder() {
            throw new UnsupportedOperationException();
          }

          @Override
          public AggregationResult applyLimit() {
            throw new UnsupportedOperationException();
          }

          @Override
          public BsonValue toStandaloneBson() {
            throw new UnsupportedOperationException();
          }

          @Override
          public BsonArray toIntermediateBson() {
            return new BsonArray(
                List.of(
                    new BsonDocument()
                        .append(
                            IntermediateAggregationMetaField.GROUP_NAME, new BsonString("wrong"))
                        .append(
                            IntermediateAggregationMetaField.KEYS,
                            new BsonDocument("city", new BsonString("NYC")))
                        .append("sum_sum_price", new BsonDouble(10.0))));
          }
        };

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () ->
                new TopLevelAggregationResult(List.of(childWithConflictingField))
                    .toIntermediateBson());
    assertEquals(
        "Child intermediate document must not set routing field $meta/group_name",
        error.getMessage());
  }

  @Test
  public void leafDoc_rejectsMetricPartialsThatSetReservedKeysField() {
    AggregationResult badMetric =
        new AggregationResult() {
          @Override
          public String getName() {
            return "sum_price";
          }

          @Override
          public AggregationResultBuilder toBuilder() {
            throw new UnsupportedOperationException();
          }

          @Override
          public AggregationResult applyLimit() {
            throw new UnsupportedOperationException();
          }

          @Override
          public BsonValue toStandaloneBson() {
            throw new UnsupportedOperationException();
          }

          @Override
          public BsonArray toIntermediateBson() {
            return new BsonArray();
          }

          @Override
          public BsonDocument toIntermediateMetricPartials() {
            return new BsonDocument()
                .append(
                    IntermediateAggregationMetaField.KEYS,
                    new BsonDocument("city", new BsonString("NYC")))
                .append("sum_sum_price", new BsonDouble(10.0));
          }
        };

    TermsResult byCity =
        new TermsResult(
            "by_city",
            CITY,
            List.of(
                new TermsResult.TermsBucket("NYC", AggregationResults.from(List.of(badMetric)))));

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () -> new TopLevelAggregationResult(List.of(byCity)).toIntermediateBson());
    assertEquals("Document must not set grouping-keys field $meta/keys", error.getMessage());
  }

  @Test
  public void mergeGroupingKeys_rejectsConflictingValues() {
    BsonDocument parent = new BsonDocument("city", new BsonString("NYC"));
    BsonDocument child = new BsonDocument("city", new BsonString("LA"));

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () -> IntermediateAggregationMetaField.mergeGroupingKeys(parent, child));
    assertEquals("Conflicting grouping key 'city': \"NYC\" vs \"LA\"", error.getMessage());
  }

  @Test
  public void toStandaloneBson_unchangedForFlatGroup() {
    TermsResult byCity =
        new TermsResult(
            "by_city",
            CITY,
            List.of(
                new TermsResult.TermsBucket(
                    "NYC",
                    AggregationResults.from(
                        List.of(new SumAggregationResult("sum_price", 30.0))))));

    BsonDocument standalone = byCity.toStandaloneBson().asDocument();
    BsonDocument expected =
        BsonDocument.parse(
            """
            {
              "buckets": [
                {"_id": "NYC", "sum_price": 30.0}
              ]
            }
            """);
    assertEquals(expected, standalone);
  }
}
