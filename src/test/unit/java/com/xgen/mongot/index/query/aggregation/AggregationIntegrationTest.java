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

    BsonArray intermediate =
        aliasedIntermediate((TopAggregationSpec) specTree, resultTree.toIntermediateBson());
    List<BsonDocument> pipeline = buildPipeline(specTree);

    assertIntermediateDocHasFields(
        intermediate.get(0).asDocument(),
        IntermediateAggregationMetaField.GROUP_NAME,
        IntermediateAggregationMetaField.KEYS,
        "m0");
    assertGroupStageReferencesFields(pipeline.get(0), "m0", "count_value");
    assertEquals(
        BsonDocument.parse(
            """
            {"$facet": {
              "f0": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "f0"
                ]}}},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "sum_price": "$m0"
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

    BsonArray intermediate =
        aliasedIntermediate((TopAggregationSpec) specTree, resultTree.toIntermediateBson());
    List<BsonDocument> pipeline = buildPipeline(specTree);

    assertIntermediateDocHasFields(
        intermediate.get(0).asDocument(),
        IntermediateAggregationMetaField.GROUP_NAME,
        IntermediateAggregationMetaField.KEYS,
        "m0",
        "m1");
    assertGroupStageReferencesFields(pipeline.get(0), "m0", "m1", "count_value");
    assertEquals(
        BsonDocument.parse(
            """
            {"$facet": {
              "f0": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "f0"
                ]}}},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "avg_price": {"$cond": {
                    "if": {"$eq": ["$m1", 0]},
                    "then": null,
                    "else": {"$divide": ["$m0", "$m1"]}
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

    BsonArray intermediate =
        aliasedIntermediate((TopAggregationSpec) specTree, resultTree.toIntermediateBson());
    List<BsonDocument> pipeline = buildPipeline(specTree);

    assertEquals(
        "f0",
        intermediate
            .get(0)
            .asDocument()
            .getString(IntermediateAggregationMetaField.GROUP_NAME)
            .getValue());
    assertIntermediateDocHasFields(
        intermediate.get(0).asDocument(),
        IntermediateAggregationMetaField.GROUP_NAME,
        IntermediateAggregationMetaField.KEYS,
        "m0",
        "m1");

    assertEquals(
        BsonArray.parse(
            """
            [
              {"$match": {"$expr": {"$eq": [
                {"$getField": {"field": "group_name", "input": "$_id"}},
                "f0"
              ]}}},
              {"$group": {
                "_id": {
                  "$getField": {
                    "field": "k0",
                    "input": {"$getField": {"field": "keys", "input": "$_id"}}
                  }
                },
                "count_value": {"$sum": "$count_value"},
                "_rows": {"$push": {
                  "k1": {
                    "$getField": {
                      "field": "k1",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "count_value": "$count_value",
                  "m0": "$m0",
                  "m1": "$m1"
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
                      "if": {"$eq": ["$$row.m1", 0]},
                      "then": null,
                      "else": {"$divide": ["$$row.m0", "$$row.m1"]}
                    }}
                  }
                }}}
              }}
            ]
            """),
        pipeline.get(1).getDocument("$facet").getArray("f0"));
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

    BsonArray intermediate =
        aliasedIntermediate((TopAggregationSpec) specTree, resultTree.toIntermediateBson());
    List<BsonDocument> pipeline = buildPipeline(specTree);

    assertEquals(
        "f0",
        intermediate
            .get(0)
            .asDocument()
            .getString(IntermediateAggregationMetaField.GROUP_NAME)
            .getValue());
    assertIntermediateDocHasFields(
        intermediate.get(0).asDocument(),
        IntermediateAggregationMetaField.GROUP_NAME,
        IntermediateAggregationMetaField.KEYS,
        "m0",
        "m1");
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
                "f0"
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
                "count_value": {"$sum": "$count_value"},
                "_rows": {"$push": {
                  "k3": {
                    "$getField": {
                      "field": "k3",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "count_value": "$count_value",
                  "m0": "$m0",
                  "m1": "$m1"
                }}
              }},
              {"$group": {
                "_id": {
                  "k0": "$_id.k0",
                  "k1": "$_id.k1"
                },
                "count_value": {"$sum": "$count_value"},
                "_rows": {"$push": {
                  "k2": "$_id.k2",
                  "count_value": "$count_value",
                  "_leaf_rows": "$_rows"
                }}
              }},
              {"$group": {
                "_id": "$_id.k0",
                "count_value": {"$sum": "$count_value"},
                "_rows": {"$push": {
                  "k1": "$_id.k1",
                  "count_value": "$count_value",
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
                              "if": {"$eq": ["$$leaf.m1", 0]},
                              "then": null,
                              "else": {
                                "$divide": [
                                  "$$leaf.m0",
                                  "$$leaf.m1"
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
        pipeline.get(1).getDocument("$facet").getArray("f0"));
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

    BsonArray intermediate =
        aliasedIntermediate((TopAggregationSpec) specTree, resultTree.toIntermediateBson());
    List<BsonDocument> pipeline = buildPipeline(specTree);

    assertEquals(4, intermediate.size());
    assertEquals(
        BsonArray.parse(
            """
            [
              {
                "$meta/group_name": "f0",
                "$meta/keys": {"k0": "LA", "k1": "red"},
                "count": {"$numberLong": "1"},
                "m0": 5.0,
                "m1": {"$numberLong": "1"}
              },
              {
                "$meta/group_name": "f0",
                "$meta/keys": {"k0": "LA", "k1": "green"},
                "count": {"$numberLong": "1"},
                "m0": 15.0,
                "m1": {"$numberLong": "1"}
              },
              {
                "$meta/group_name": "f0",
                "$meta/keys": {"k0": "NYC", "k1": "red"},
                "count": {"$numberLong": "1"},
                "m0": 30.0,
                "m1": {"$numberLong": "2"}
              },
              {
                "$meta/group_name": "f1",
                "$meta/keys": {"k0": "red"},
                "count": {"$numberLong": "1"},
                "m0": 35.0,
                "m1": {"$numberLong": "3"}
              }
            ]
            """),
        intermediate);

    assertEquals(
        BsonDocument.parse(
            """
            {"$facet": {
              "f0": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "f0"
                ]}}},
                {"$group": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "count_value": {"$sum": "$count_value"},
                  "_rows": {"$push": {
                    "k1": {
                      "$getField": {
                        "field": "k1",
                        "input": {"$getField": {"field": "keys", "input": "$_id"}}
                      }
                    },
                    "count_value": "$count_value",
                    "m0": "$m0",
                    "m1": "$m1"
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
                        "if": {"$eq": ["$$row.m1", 0]},
                        "then": null,
                        "else": {
                          "$divide": ["$$row.m0", "$$row.m1"]
                        }
                      }}
                    }
                  }}}
                }}
              ],
              "f1": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "f1"
                ]}}},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "avg_price": {"$cond": {
                    "if": {"$eq": ["$m1", 0]},
                    "then": null,
                    "else": {"$divide": ["$m0", "$m1"]}
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
                "by_city": {"buckets": "$f0"},
                "by_color": {"buckets": "$f1"}
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

    BsonArray intermediate =
        aliasedIntermediate((TopAggregationSpec) specTree, resultTree.toIntermediateBson());
    List<BsonDocument> pipeline = buildPipeline(specTree);

    assertEquals(
        "f0",
        intermediate
            .get(0)
            .asDocument()
            .getString(IntermediateAggregationMetaField.GROUP_NAME)
            .getValue());
    assertEquals(
        "f1",
        intermediate
            .get(1)
            .asDocument()
            .getString(IntermediateAggregationMetaField.GROUP_NAME)
            .getValue());
    assertEquals(
        BsonDocument.parse(
            """
            {"$facet": {
              "f0": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "f0"
                ]}}},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "sum_price": "$m0"
                }}
              ],
              "f1": [
                {"$match": {"$expr": {"$eq": [
                  {"$getField": {"field": "group_name", "input": "$_id"}},
                  "f1"
                ]}}},
                {"$project": {
                  "_id": {
                    "$getField": {
                      "field": "k0",
                      "input": {"$getField": {"field": "keys", "input": "$_id"}}
                    }
                  },
                  "sum_price": "$m0"
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
    TopAggregationSpec top = (TopAggregationSpec) specTree;
    return top.newShardMergePipelineBuilder(List.of()).build("lowerBound");
  }

  private static BsonArray aliasedIntermediate(TopAggregationSpec spec, BsonArray raw) {
    return ShardMergeAliasPlan.from(spec, List.of()).rewriteIntermediateBson(raw);
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
