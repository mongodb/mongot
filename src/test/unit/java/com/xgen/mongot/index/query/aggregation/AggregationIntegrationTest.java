package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.util.FieldPath;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.Test;

/**
 * Verifies {@link AggregationResult#toIntermediateBson()} field names align with the merge pipeline
 * produced by {@link AggregationSpec#buildShardMergePipeline(ShardMergePipelineBuilder)}.
 */
public class AggregationIntegrationTest extends AggregationTestSupport {

  private static final FieldPath CITY = FieldPath.parse("city");
  private static final FieldPath COLOR = FieldPath.parse("color");
  private static final FieldPath REGION = FieldPath.parse("region");
  private static final FieldPath COUNTRY = FieldPath.parse("country");
  private static final FieldPath PRICE = FieldPath.parse("price");

  @Test
  public void flatSum_intermediateBsonMatchesGeneratedPipeline() {
    AggregationSpec specTree =
        new TopAggregationSpec(
            List.of(
                new TermsSpec(
                    "by_city", CITY, List.of(new SumAggregationSpec("sum_price", PRICE)))));

    AggregationResult resultTree =
        new TopLevelAggregationResult(
            List.of(
                new TermsResult(
                    "by_city",
                    CITY,
                    List.of(
                        new TermsResult.TermsBucket(
                            "NYC",
                            AggregationResults.from(
                                List.of(new SumAggregationResult("sum_price", 30.0)))),
                        new TermsResult.TermsBucket(
                            "SF",
                            AggregationResults.from(
                                List.of(new SumAggregationResult("sum_price", 50.0))))))));

    BsonArray intermediate = resultTree.toIntermediateBson();
    List<BsonDocument> pipeline = buildPipeline(specTree);

    assertIntermediateDocHasFields(
        intermediate.get(0).asDocument(),
        IntermediateAggregationMetaField.GROUP_NAME,
        IntermediateAggregationMetaField.KEYS,
        "sum_sum_price");
    assertGroupStageReferencesFields(pipeline.get(0), "sum_sum_price", "count_value");
    assertEquals(
        BsonDocument.parse(
            """
            {"$facet": {
              "by_city": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "by_city"
                ]}}},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "sum_price": "$sum_sum_price"
                }}
              ],
              "count": [{"$match": {"$expr": {"$eq": [
                {"$ifNull": [{"$getField": {"field": "group_name", "input": "$_id"}}, null]},
                null
              ]}}}]
            }}
            """),
        pipeline.get(1));
  }

  @Test
  public void flatAvg_intermediateBsonMatchesGeneratedPipeline() {
    AggregationSpec specTree =
        new TopAggregationSpec(
            List.of(
                new TermsSpec(
                    "by_city", CITY, List.of(new AvgAggregationSpec("avg_price", PRICE)))));

    AggregationResult resultTree =
        new TopLevelAggregationResult(
            List.of(
                new TermsResult(
                    "by_city",
                    CITY,
                    List.of(
                        new TermsResult.TermsBucket(
                            "NYC",
                            AggregationResults.from(
                                List.of(new AvgAggregationResult("avg_price", 30.0, 2))))))));

    BsonArray intermediate = resultTree.toIntermediateBson();
    List<BsonDocument> pipeline = buildPipeline(specTree);

    assertIntermediateDocHasFields(
        intermediate.get(0).asDocument(),
        IntermediateAggregationMetaField.GROUP_NAME,
        IntermediateAggregationMetaField.KEYS,
        "avg_sum_avg_price",
        "avg_count_avg_price");
    assertGroupStageReferencesFields(
        pipeline.get(0), "avg_sum_avg_price", "avg_count_avg_price", "count_value");
    assertEquals(
        BsonDocument.parse(
            """
            {"$facet": {
              "by_city": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "by_city"
                ]}}},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "avg_price": {"$cond": {
                    "if": {"$eq": ["$avg_count_avg_price", 0]},
                    "then": null,
                    "else": {"$divide": ["$avg_sum_avg_price", "$avg_count_avg_price"]}
                  }}
                }}
              ],
              "count": [{"$match": {"$expr": {"$eq": [
                {"$ifNull": [{"$getField": {"field": "group_name", "input": "$_id"}}, null]},
                null
              ]}}}]
            }}
            """),
        pipeline.get(1));
  }

  @Test
  public void nestedAvg_intermediateBsonAndPipelineUseReconstructionStages() {
    AggregationSpec specTree =
        new TopAggregationSpec(
            List.of(
                new TermsSpec(
                    "by_city",
                    CITY,
                    List.of(
                        new TermsSpec(
                            "by_color",
                            COLOR,
                            List.of(new AvgAggregationSpec("avg_price", PRICE)))))));

    TermsResult laColors =
        new TermsResult(
            "by_color",
            COLOR,
            List.of(
                new TermsResult.TermsBucket(
                    "red",
                    AggregationResults.from(
                        List.of(new AvgAggregationResult("avg_price", 5.0, 1))))));

    AggregationResult resultTree =
        new TopLevelAggregationResult(
            List.of(
                new TermsResult(
                    "by_city",
                    CITY,
                    List.of(
                        new TermsResult.TermsBucket(
                            "LA", AggregationResults.from(List.of(laColors)))))));

    BsonArray intermediate = resultTree.toIntermediateBson();
    List<BsonDocument> pipeline = buildPipeline(specTree);

    assertEquals(
        "by_city",
        intermediate
            .get(0)
            .asDocument()
            .getString(IntermediateAggregationMetaField.GROUP_NAME)
            .getValue());
    assertIntermediateDocHasFields(
        intermediate.get(0).asDocument(),
        IntermediateAggregationMetaField.GROUP_NAME,
        IntermediateAggregationMetaField.KEYS,
        "avg_sum_avg_price",
        "avg_count_avg_price");

    assertEquals(
        BsonArray.parse(
            """
            [
              {"$match": {"$expr": {"$eq": [
                {"$getField": {"field": "group_name", "input": "$_id"}},
                "by_city"
              ]}}},
              {"$group": {
                "_id": {
                  "$getField": {
                    "field": "k0",
                    "input": {"$getField": {"field": "keys", "input": "$_id"}}
                  }
                },
                "_rows": {"$push": {
                  "k1": {
                    "$getField": {
                      "field": "k1",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "avg_sum_avg_price": "$avg_sum_avg_price",
                  "avg_count_avg_price": "$avg_count_avg_price"
                }}
              }},
              {"$project": {
                "_id": "$_id",
                "by_color": {"buckets": {"$map": {
                  "input": "$_rows",
                  "as": "row",
                  "in": {
                    "_id": "$$row.k1",
                    "avg_price": {"$cond": {
                      "if": {"$eq": ["$$row.avg_count_avg_price", 0]},
                      "then": null,
                      "else": {"$divide": ["$$row.avg_sum_avg_price", "$$row.avg_count_avg_price"]}
                    }}
                  }
                }}}
              }}
            ]
            """),
        pipeline.get(1).getDocument("$facet").getArray("by_city"));
  }

  @Test
  public void nestedDepth4_intermediateBsonAndPipelineUseThreeReconstructionGroups() {
    AggregationSpec byColor =
        new TermsSpec("by_color", COLOR, List.of(new AvgAggregationSpec("avg_price", PRICE)));
    AggregationSpec byCity = new TermsSpec("by_city", CITY, List.of(byColor));
    AggregationSpec byRegion = new TermsSpec("by_region", REGION, List.of(byCity));
    AggregationSpec specTree =
        new TopAggregationSpec(List.of(new TermsSpec("by_country", COUNTRY, List.of(byRegion))));

    TermsResult laColors =
        new TermsResult(
            "by_color",
            COLOR,
            List.of(
                new TermsResult.TermsBucket(
                    "red",
                    AggregationResults.from(
                        List.of(new AvgAggregationResult("avg_price", 5.0, 1))))));

    TermsResult laCities =
        new TermsResult(
            "by_city",
            CITY,
            List.of(new TermsResult.TermsBucket("LA", AggregationResults.from(List.of(laColors)))));

    TermsResult westRegions =
        new TermsResult(
            "by_region",
            REGION,
            List.of(
                new TermsResult.TermsBucket("West", AggregationResults.from(List.of(laCities)))));

    AggregationResult resultTree =
        new TopLevelAggregationResult(
            List.of(
                new TermsResult(
                    "by_country",
                    COUNTRY,
                    List.of(
                        new TermsResult.TermsBucket(
                            "US", AggregationResults.from(List.of(westRegions)))))));

    BsonArray intermediate = resultTree.toIntermediateBson();
    List<BsonDocument> pipeline = buildPipeline(specTree);

    assertEquals(
        "by_country",
        intermediate
            .get(0)
            .asDocument()
            .getString(IntermediateAggregationMetaField.GROUP_NAME)
            .getValue());
    assertIntermediateDocHasFields(
        intermediate.get(0).asDocument(),
        IntermediateAggregationMetaField.GROUP_NAME,
        IntermediateAggregationMetaField.KEYS,
        "avg_sum_avg_price",
        "avg_count_avg_price");
    BsonDocument keys =
        intermediate.get(0).asDocument().getDocument(IntermediateAggregationMetaField.KEYS);
    assertEquals("US", keys.getString("k0").getValue());
    assertEquals("West", keys.getString("k1").getValue());
    assertEquals("LA", keys.getString("k2").getValue());
    assertEquals("red", keys.getString("k3").getValue());

    assertEquals(
        BsonArray.parse(
            """
            [
              {"$match": {"$expr": {"$eq": [
                {"$getField": {"field": "group_name", "input": "$_id"}},
                "by_country"
              ]}}},
              {"$group": {
                "_id": {
                  "k0": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "k1": {
                    "$getField": {
                      "field": "k1",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "k2": {
                    "$getField": {
                      "field": "k2",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  }
                },
                "_rows": {"$push": {
                  "k3": {
                    "$getField": {
                      "field": "k3",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "avg_sum_avg_price": "$avg_sum_avg_price",
                  "avg_count_avg_price": "$avg_count_avg_price"
                }}
              }},
              {"$group": {
                "_id": {
                  "k0": "$_id.k0",
                  "k1": "$_id.k1"
                },
                "_rows": {"$push": {
                  "k2": "$_id.k2",
                  "_leaf_rows": "$_rows"
                }}
              }},
              {"$group": {
                "_id": "$_id.k0",
                "_rows": {"$push": {
                  "k1": "$_id.k1",
                  "_leaf_rows": "$_rows"
                }}
              }},
              {"$project": {
                "_id": "$_id",
                "by_region": {"buckets": {"$map": {
                  "input": "$_rows",
                  "as": "row",
                  "in": {
                    "_id": "$$row.k1",
                    "by_city": {"buckets": {"$map": {
                      "input": "$$row._leaf_rows",
                      "as": "leaf",
                      "in": {
                        "_id": "$$leaf.k2",
                        "by_color": {"buckets": {"$map": {
                          "input": "$$leaf._leaf_rows",
                          "as": "leaf",
                          "in": {
                            "_id": "$$leaf.k3",
                            "avg_price": {"$cond": {
                              "if": {"$eq": ["$$leaf.avg_count_avg_price", 0]},
                              "then": null,
                              "else": {
                                "$divide": [
                                  "$$leaf.avg_sum_avg_price",
                                  "$$leaf.avg_count_avg_price"
                                ]
                              }
                            }}
                          }
                        }}}
                      }
                    }}}
                  }
                }}}
              }}
            ]
            """),
        pipeline.get(1).getDocument("$facet").getArray("by_country"));
  }

  @Test
  public void nestedAndFlatFacets_intermediateBsonAndPipelineAlign() {
    AggregationSpec nestedColor =
        new TermsSpec("by_color", COLOR, List.of(new AvgAggregationSpec("avg_price", PRICE)));
    AggregationSpec nestedCity = new TermsSpec("by_city", CITY, List.of(nestedColor));
    AggregationSpec flatColor =
        new TermsSpec("by_color", COLOR, List.of(new AvgAggregationSpec("avg_price", PRICE)));
    AggregationSpec specTree = new TopAggregationSpec(List.of(nestedCity, flatColor));

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

    TermsResult nycColors =
        new TermsResult(
            "by_color",
            COLOR,
            List.of(
                new TermsResult.TermsBucket(
                    "red",
                    AggregationResults.from(
                        List.of(new AvgAggregationResult("avg_price", 30.0, 2))))));

    TermsResult byCityResult =
        new TermsResult(
            "by_city",
            CITY,
            List.of(
                new TermsResult.TermsBucket("LA", AggregationResults.from(List.of(laColors))),
                new TermsResult.TermsBucket("NYC", AggregationResults.from(List.of(nycColors)))));

    TermsResult byColorResult =
        new TermsResult(
            "by_color",
            COLOR,
            List.of(
                new TermsResult.TermsBucket(
                    "red",
                    AggregationResults.from(
                        List.of(new AvgAggregationResult("avg_price", 35.0, 3))))));

    AggregationResult resultTree =
        new TopLevelAggregationResult(List.of(byCityResult, byColorResult));

    BsonArray intermediate = resultTree.toIntermediateBson();
    List<BsonDocument> pipeline = buildPipeline(specTree);

    assertEquals(4, intermediate.size());
    assertEquals(
        BsonArray.parse(
            """
            [
              {
                "$meta/group_name": "by_city",
                "$meta/keys": {"k0": "LA", "k1": "red"},
                "count": {"$numberLong": "1"},
                "avg_sum_avg_price": 5.0,
                "avg_count_avg_price": {"$numberLong": "1"}
              },
              {
                "$meta/group_name": "by_city",
                "$meta/keys": {"k0": "LA", "k1": "green"},
                "count": {"$numberLong": "1"},
                "avg_sum_avg_price": 15.0,
                "avg_count_avg_price": {"$numberLong": "1"}
              },
              {
                "$meta/group_name": "by_city",
                "$meta/keys": {"k0": "NYC", "k1": "red"},
                "count": {"$numberLong": "1"},
                "avg_sum_avg_price": 30.0,
                "avg_count_avg_price": {"$numberLong": "2"}
              },
              {
                "$meta/group_name": "by_color",
                "$meta/keys": {"k0": "red"},
                "count": {"$numberLong": "1"},
                "avg_sum_avg_price": 35.0,
                "avg_count_avg_price": {"$numberLong": "3"}
              }
            ]
            """),
        intermediate);

    assertEquals(
        BsonDocument.parse(
            """
            {"$facet": {
              "by_city": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "by_city"
                ]}}},
                {"$group": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "_rows": {"$push": {
                    "k1": {
                      "$getField": {
                        "field": "k1",
                        "input": {"$getField": {"field": "keys", "input": "$_id"}}
                      }
                    },
                    "avg_sum_avg_price": "$avg_sum_avg_price",
                    "avg_count_avg_price": "$avg_count_avg_price"
                  }}
                }},
                {"$project": {
                  "_id": "$_id",
                  "by_color": {"buckets": {"$map": {
                    "input": "$_rows",
                    "as": "row",
                    "in": {
                      "_id": "$$row.k1",
                      "avg_price": {"$cond": {
                        "if": {"$eq": ["$$row.avg_count_avg_price", 0]},
                        "then": null,
                        "else": {
                          "$divide": ["$$row.avg_sum_avg_price", "$$row.avg_count_avg_price"]
                        }
                      }}
                    }
                  }}}
                }}
              ],
              "by_color": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "by_color"
                ]}}},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "avg_price": {"$cond": {
                    "if": {"$eq": ["$avg_count_avg_price", 0]},
                    "then": null,
                    "else": {"$divide": ["$avg_sum_avg_price", "$avg_count_avg_price"]}
                  }}
                }}
              ],
              "count": [{"$match": {"$expr": {"$eq": [
                {"$ifNull": [{"$getField": {"field": "group_name", "input": "$_id"}}, null]},
                null
              ]}}}]
            }}
            """),
        pipeline.get(1));

    assertEquals(
        BsonDocument.parse(
            """
            {"$replaceWith": {
              "count": {"lowerBound": {"$first": "$count.count_value"}},
              "facet": {
                "by_city": {"buckets": "$by_city"},
                "by_color": {"buckets": "$by_color"}
              }
            }}
            """),
        pipeline.get(2));
  }

  @Test
  public void multipleTopLevelFacets_intermediateBsonAndPipelineUseDistinctGroupNames() {
    AggregationSpec specTree =
        new TopAggregationSpec(
            List.of(
                new TermsSpec("by_city", CITY, List.of(new SumAggregationSpec("sum_price", PRICE))),
                new TermsSpec(
                    "by_color", COLOR, List.of(new SumAggregationSpec("sum_price", PRICE)))));

    AggregationResult resultTree =
        new TopLevelAggregationResult(
            List.of(
                new TermsResult(
                    "by_city",
                    CITY,
                    List.of(
                        new TermsResult.TermsBucket(
                            "NYC",
                            AggregationResults.from(
                                List.of(new SumAggregationResult("sum_price", 10.0)))))),
                new TermsResult(
                    "by_color",
                    COLOR,
                    List.of(
                        new TermsResult.TermsBucket(
                            "red",
                            AggregationResults.from(
                                List.of(new SumAggregationResult("sum_price", 35.0))))))));

    BsonArray intermediate = resultTree.toIntermediateBson();
    List<BsonDocument> pipeline = buildPipeline(specTree);

    assertEquals(
        "by_city",
        intermediate
            .get(0)
            .asDocument()
            .getString(IntermediateAggregationMetaField.GROUP_NAME)
            .getValue());
    assertEquals(
        "by_color",
        intermediate
            .get(1)
            .asDocument()
            .getString(IntermediateAggregationMetaField.GROUP_NAME)
            .getValue());
    assertEquals(
        BsonDocument.parse(
            """
            {"$facet": {
              "by_city": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "by_city"
                ]}}},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "sum_price": "$sum_sum_price"
                }}
              ],
              "by_color": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "by_color"
                ]}}},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "sum_price": "$sum_sum_price"
                }}
              ],
              "count": [{"$match": {"$expr": {"$eq": [
                {"$ifNull": [{"$getField": {"field": "group_name", "input": "$_id"}}, null]},
                null
              ]}}}]
            }}
            """),
        pipeline.get(1));
  }

  private static List<BsonDocument> buildPipeline(AggregationSpec specTree) {
    ShardMergePipelineBuilder builder = ShardMergePipelineBuilder.unsorted();
    specTree.buildShardMergePipeline(builder);
    return builder.build("lowerBound");
  }

  private static void assertIntermediateDocHasFields(BsonDocument doc, String... fieldNames) {
    for (String fieldName : fieldNames) {
      assertTrue("missing field: " + fieldName, doc.containsKey(fieldName));
    }
  }

  private static void assertGroupStageReferencesFields(
      BsonDocument groupStage, String... accumulatorFieldNames) {
    BsonDocument groupBody = groupStage.getDocument("$group");
    for (String fieldName : accumulatorFieldNames) {
      assertTrue("group stage missing field: " + fieldName, groupBody.containsKey(fieldName));
    }
    BsonDocument id = groupBody.getDocument("_id");
    assertTrue(id.containsKey(IntermediateAggregationMetaField.ID_GROUP_NAME));
    assertTrue(id.containsKey(IntermediateAggregationMetaField.ID_KEYS));
  }

  // ===== inline spec fixtures =====

  private static final class TermsSpec extends GroupAggregationSpec {
    TermsSpec(String name, FieldPath path, List<AggregationSpec> children) {
      super(path, name, children);
    }

    @Override
    protected Accumulator createAccumulator(
        AggregationContext context, List<Accumulator> childAccumulators) {
      throw new UnsupportedOperationException("not exercised");
    }
  }
}
