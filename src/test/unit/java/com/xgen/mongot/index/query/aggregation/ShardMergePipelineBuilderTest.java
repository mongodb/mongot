package com.xgen.mongot.index.query.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.BoundaryArgs;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.TermsArgs;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.Test;

/**
 * Validates the merge pipeline produced by ShardMergePipelineBuilder for representative spec trees.
 * Pipeline BSON is asserted verbatim so accidental changes are caught.
 */
public class ShardMergePipelineBuilderTest {

  private static final FieldPath CITY = FieldPath.parse("city");
  private static final FieldPath COLOR = FieldPath.parse("color");
  private static final FieldPath REGION = FieldPath.parse("region");
  private static final FieldPath COUNTRY = FieldPath.parse("country");
  private static final FieldPath PRICE = FieldPath.parse("price");

  private static List<BsonDocument> buildPipeline(TopAggregationSpec top, String countType) {
    return top.newShardMergePipelineBuilder(List.of()).build(countType);
  }

  private static List<BsonDocument> buildPipeline(TopAggregationSpec top) {
    return buildPipeline(top, "lowerBound");
  }

  @Test
  public void singleGroupWithMetricLowerBound() {
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec group = new TermsSpec("by_city", CITY, List.of(leaf));
    TopAggregationSpec top = new TopAggregationSpec(List.of(group));

    List<BsonDocument> pipeline = buildPipeline(top);

    assertEquals(3, pipeline.size());

    BsonDocument expectedGroup =
        BsonDocument.parse(
            """
            {"$group": {
              "_id": {
                "group_name": {
                  "$getField": {
                    "field": {"$literal": "$meta/group_name"},
                    "input": "$$ROOT"
                  }
                },
                "keys": {"$getField": {"field": {"$literal": "$meta/keys"}, "input": "$$ROOT"}}
              },
              "count_value": {"$sum": "$count"},
              "m0": {"$sum": "$m0"}
            }}
            """);
    assertEquals(expectedGroup, pipeline.get(0));

    BsonDocument expectedFacet =
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
            """);
    assertEquals(expectedFacet, pipeline.get(1));

    BsonDocument expectedReplaceWith =
        BsonDocument.parse(
            """
            {"$replaceWith": {
              "count": {"lowerBound": {"$first": "$count.count_value"}},
              "facet": {
                "by_city": {"buckets": "$f0"}
              }
            }}
            """);
    assertEquals(expectedReplaceWith, pipeline.get(2));
  }

  @Test
  public void singleGroupWithAvgMetricFinalizesInFacetProject() {
    AggregationSpec leaf = new AvgAggregationSpec("avg_price", PRICE);
    AggregationSpec group = new TermsSpec("by_city", CITY, List.of(leaf));
    TopAggregationSpec top = new TopAggregationSpec(List.of(group));

    List<BsonDocument> pipeline = buildPipeline(top);

    BsonDocument expectedGroup =
        BsonDocument.parse(
            """
            {"$group": {
              "_id": {
                "group_name": {
                  "$getField": {
                    "field": {"$literal": "$meta/group_name"},
                    "input": "$$ROOT"
                  }
                },
                "keys": {"$getField": {"field": {"$literal": "$meta/keys"}, "input": "$$ROOT"}}
              },
              "count_value": {"$sum": "$count"},
              "m0": {"$sum": "$m0"},
              "m1": {"$sum": "$m1"}
            }}
            """);
    assertEquals(expectedGroup, pipeline.get(0));

    BsonDocument projectStage =
        pipeline
            .get(1)
            .getDocument("$facet")
            .getArray("f0")
            .get(1)
            .asDocument()
            .getDocument("$project");
    BsonDocument expectedAvgPrice =
        BsonDocument.parse(
            """
            {"$cond": {
              "if": {"$eq": ["$m1", 0]},
              "then": null,
              "else": {"$divide": ["$m0", "$m1"]}
            }}
            """);
    assertEquals(expectedAvgPrice, projectStage.get("avg_price"));
  }

  @Test
  public void countTypeTotalChangesCountField() {
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec group = new TermsSpec("by_city", CITY, List.of(leaf));
    TopAggregationSpec top = new TopAggregationSpec(List.of(group));

    List<BsonDocument> pipeline = buildPipeline(top, "total");

    BsonDocument replaceWith = pipeline.get(2);
    BsonDocument count = replaceWith.getDocument("$replaceWith").getDocument("count");
    assertEquals(BsonDocument.parse("{\"total\": {\"$first\": \"$count.count_value\"}}"), count);
  }

  @Test
  public void nestedGroupNamedCount_rejectedAtRegistration() {
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec innerGroup = new TermsSpec("count", COLOR, List.of(leaf));
    AggregationSpec outerGroup = new TermsSpec("by_city", CITY, List.of(innerGroup));
    TopAggregationSpec top = new TopAggregationSpec(List.of(outerGroup));

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> buildPipeline(top));

    assertTrue(error.getMessage().contains("'count'"));
  }

  @Test
  public void nestedGroupsReconstructChildBranchInFacetArm() {
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec innerGroup = new TermsSpec("by_color", COLOR, List.of(leaf));
    AggregationSpec outerGroup = new TermsSpec("by_city", CITY, List.of(innerGroup));
    TopAggregationSpec top = new TopAggregationSpec(List.of(outerGroup));

    List<BsonDocument> pipeline = buildPipeline(top);

    BsonArray expectedByCityArm =
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
                  "m0": "$m0"
                }}
              }},
              {"$project": {
                "_id": "$_id",
                "count": {"total": "$count_value"},
                "by_color": {"buckets": {"$map": {
                  "input": "$_rows",
                  "as": "row",
                  "in": {
                    "_id": "$$row.k1",
                    "sum_price": "$$row.m0"
                  }
                }}}
              }}
            ]
            """);
    assertEquals(expectedByCityArm, pipeline.get(1).getDocument("$facet").getArray("f0"));

    BsonDocument expectedReplaceWith =
        BsonDocument.parse(
            """
            {"$replaceWith": {
              "count": {"lowerBound": {"$first": "$count.count_value"}},
              "facet": {
                "by_city": {"buckets": "$f0"}
              }
            }}
            """);
    assertEquals(expectedReplaceWith, pipeline.get(2));
  }

  /**
   * Nested grouping with {@code min} and {@code max} siblings. Unlike {@code sum}, these merge with
   * selective accumulators, and their partials have to survive the same flat-row push and bucket
   * reconstruction -- the combination the flat terms coverage in {@code ShardedSearchPlannerTest}
   * never exercises.
   */
  @Test
  public void nestedGroupsCarryMinAndMaxPartialsThroughReconstruction() {
    AggregationSpec min = new MinAggregationSpec("cheapest", PRICE);
    AggregationSpec max = new MaxAggregationSpec("priciest", PRICE);
    AggregationSpec innerGroup = new TermsSpec("by_color", COLOR, List.of(min, max));
    AggregationSpec outerGroup = new TermsSpec("by_city", CITY, List.of(innerGroup));
    TopAggregationSpec top = new TopAggregationSpec(List.of(outerGroup));

    List<BsonDocument> pipeline = buildPipeline(top);

    BsonDocument expectedGroup =
        BsonDocument.parse(
            """
            {"$group": {
              "_id": {
                "group_name": {
                  "$getField": {
                    "field": {"$literal": "$meta/group_name"},
                    "input": "$$ROOT"
                  }
                },
                "keys": {"$getField": {"field": {"$literal": "$meta/keys"}, "input": "$$ROOT"}}
              },
              "count_value": {"$sum": "$count"},
              "m0": {"$min": "$m0"},
              "m1": {"$max": "$m1"}
            }}
            """);
    assertEquals(expectedGroup, pipeline.get(0));

    BsonArray expectedByCityArm =
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
                "count": {"total": "$count_value"},
                "by_color": {"buckets": {"$map": {
                  "input": "$_rows",
                  "as": "row",
                  "in": {
                    "_id": "$$row.k1",
                    "cheapest": {"$cond": {
                      "if": {"$eq": [{"$type": "$$row.m0"}, "minKey"]},
                      "then": null,
                      "else": "$$row.m0"
                    }},
                    "priciest": {"$cond": {
                      "if": {"$eq": [{"$type": "$$row.m1"}, "minKey"]},
                      "then": null,
                      "else": "$$row.m1"
                    }}
                  }
                }}}
              }}
            ]
            """);
    assertEquals(expectedByCityArm, pipeline.get(1).getDocument("$facet").getArray("f0"));
  }

  @Test
  public void nestedDepth4_emitsThreeGroupStagesAndTripleNestedProject() {
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec byColor = new TermsSpec("by_color", COLOR, List.of(leaf));
    AggregationSpec byCity = new TermsSpec("by_city", CITY, List.of(byColor));
    AggregationSpec byRegion = new TermsSpec("by_region", REGION, List.of(byCity));
    AggregationSpec byCountry = new TermsSpec("by_country", COUNTRY, List.of(byRegion));
    TopAggregationSpec top = new TopAggregationSpec(List.of(byCountry));

    List<BsonDocument> pipeline = buildPipeline(top);

    BsonArray expectedByCountryArm =
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
                  "m0": "$m0"
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
                "count": {"total": "$count_value"},
                "by_region": {"buckets": {"$map": {
                  "input": "$_rows",
                  "as": "row",
                  "in": {
                    "_id": "$$row.k1",
                    "count": {"total": "$$row.count_value"},
                    "by_city": {"buckets": {"$map": {
                      "input": "$$row._leaf_rows",
                      "as": "leaf",
                      "in": {
                        "_id": "$$leaf.k2",
                        "count": {"total": "$$leaf.count_value"},
                        "by_color": {"buckets": {"$map": {
                          "input": "$$leaf._leaf_rows",
                          "as": "leaf",
                          "in": {
                            "_id": "$$leaf.k3",
                            "sum_price": "$$leaf.m0"
                          }
                        }}}
                      }
                    }}}
                  }
                }}}
              }}
            ]
            """);
    assertEquals(expectedByCountryArm, pipeline.get(1).getDocument("$facet").getArray("f0"));

    BsonDocument expectedReplaceWith =
        BsonDocument.parse(
            """
            {"$replaceWith": {
              "count": {"lowerBound": {"$first": "$count.count_value"}},
              "facet": {
                "by_country": {"buckets": "$f0"}
              }
            }}
            """);
    assertEquals(expectedReplaceWith, pipeline.get(2));
  }

  @Test
  public void flatTerms_appliesSortAndLimitBeforeProject() {
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec group =
        new TermsGroupAggregationSpec(
            "by_city", CITY, List.of(leaf), new TermsArgs(true, TypeField.TOKEN, 2));
    TopAggregationSpec top = new TopAggregationSpec(List.of(group));

    List<BsonDocument> pipeline = buildPipeline(top);

    BsonArray byCityArm = pipeline.get(1).getDocument("$facet").getArray("f0");
    assertEquals(4, byCityArm.size());
    assertEquals(
        BsonDocument.parse("{\"$sort\": {\"count_value\": -1, \"_id.keys.k0\": 1}}"),
        byCityArm.get(1).asDocument());
    assertEquals(BsonDocument.parse("{\"$limit\": 2}"), byCityArm.get(2).asDocument());
  }

  @Test
  public void nestedTerms_appliesPerLevelTopN_outerSortLimit_innerSlice() {
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec inner = termsGroup("by_color", COLOR, List.of(leaf), 1);
    AggregationSpec outer = termsGroup("by_city", CITY, List.of(inner), 2);
    TopAggregationSpec top = new TopAggregationSpec(List.of(outer));

    List<BsonDocument> pipeline = buildPipeline(top);
    BsonArray arm = pipeline.get(1).getDocument("$facet").getArray("f0");

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
                  "m0": "$m0"
                }}
              }},
              {"$sort": {"count_value": -1, "_id": 1}},
              {"$limit": 2},
              {"$project": {
                "_id": "$_id",
                "count": {"total": "$count_value"},
                "by_color": {"buckets": {"$map": {
                  "input": {"$slice": [
                    {"$sortArray": {
                      "input": "$_rows",
                      "sortBy": {"count_value": -1, "k1": 1}
                    }},
                    1
                  ]},
                  "as": "row",
                  "in": {
                    "_id": "$$row.k1",
                    "sum_price": "$$row.m0"
                  }
                }}}
              }}
            ]
            """),
        arm);
    BsonDocument innerRow =
        arm.get(4)
            .asDocument()
            .getDocument("$project")
            .getDocument("by_color")
            .getDocument("buckets")
            .getDocument("$map")
            .getDocument("in");
    assertFalse(innerRow.containsKey("count_value"));
  }

  @Test
  public void nestedDepth4_appliesDistinctTopNAtEachTermsLevel() {
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec byColor = termsGroup("by_color", COLOR, List.of(leaf), 2);
    AggregationSpec byCity = termsGroup("by_city", CITY, List.of(byColor), 3);
    AggregationSpec byRegion = termsGroup("by_region", REGION, List.of(byCity), 4);
    AggregationSpec byCountry = termsGroup("by_country", COUNTRY, List.of(byRegion), 5);
    TopAggregationSpec top = new TopAggregationSpec(List.of(byCountry));

    BsonArray arm = buildPipeline(top).get(1).getDocument("$facet").getArray("f0");
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
                  "m0": "$m0"
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
              {"$sort": {"count_value": -1, "_id": 1}},
              {"$limit": 5},
              {"$project": {
                "_id": "$_id",
                "count": {"total": "$count_value"},
                "by_region": {"buckets": {"$map": {
                  "input": {"$slice": [
                    {"$sortArray": {
                      "input": "$_rows",
                      "sortBy": {"count_value": -1, "k1": 1}
                    }},
                    4
                  ]},
                  "as": "row",
                  "in": {
                    "_id": "$$row.k1",
                    "count": {"total": "$$row.count_value"},
                    "by_city": {"buckets": {"$map": {
                      "input": {"$slice": [
                        {"$sortArray": {
                          "input": "$$row._leaf_rows",
                          "sortBy": {"count_value": -1, "k2": 1}
                        }},
                        3
                      ]},
                      "as": "leaf",
                      "in": {
                        "_id": "$$leaf.k2",
                        "count": {"total": "$$leaf.count_value"},
                        "by_color": {"buckets": {"$map": {
                          "input": {"$slice": [
                            {"$sortArray": {
                              "input": "$$leaf._leaf_rows",
                              "sortBy": {"count_value": -1, "k3": 1}
                            }},
                            2
                          ]},
                          "as": "leaf",
                          "in": {
                            "_id": "$$leaf.k3",
                            "sum_price": "$$leaf.m0"
                          }
                        }}}
                      }
                    }}}
                  }
                }}}
              }}
            ]
            """),
        arm);
  }

  @Test
  public void nestedTermsOuterBoundaryInner_sortsInnerByKey_notTermsTopN() {
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec inner = boundaryGroup("by_price", PRICE, List.of(leaf));
    AggregationSpec outer = termsGroup("by_city", CITY, List.of(inner), 2);
    TopAggregationSpec top = new TopAggregationSpec(List.of(outer));

    BsonArray arm = buildPipeline(top).get(1).getDocument("$facet").getArray("f0");
    assertEquals(
        BsonDocument.parse("{\"$sort\": {\"count_value\": -1, \"_id\": 1}}"),
        arm.get(2).asDocument());
    assertEquals(BsonDocument.parse("{\"$limit\": 2}"), arm.get(3).asDocument());
    assertEquals(
        BsonDocument.parse(
            """
            {"$project": {
              "_id": "$_id",
              "count": {"total": "$count_value"},
              "by_price": {"buckets": {"$map": {
                "input": {"$sortArray": {"input": "$_rows", "sortBy": {"k1": 1}}},
                "as": "row",
                "in": {
                  "_id": "$$row.k1",
                  "sum_price": "$$row.m0"
                }
              }}}
            }}
            """),
        arm.get(4).asDocument());
  }

  @Test
  public void nestedBoundaryOuterTermsInner_sortsOuterByKey_slicesInnerTopN() {
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec inner = termsGroup("by_color", COLOR, List.of(leaf), 1);
    AggregationSpec outer = boundaryGroup("by_price", PRICE, List.of(inner));
    TopAggregationSpec top = new TopAggregationSpec(List.of(outer));

    BsonArray arm = buildPipeline(top).get(1).getDocument("$facet").getArray("f0");
    assertEquals(BsonDocument.parse("{\"$sort\": {\"_id\": 1}}"), arm.get(2).asDocument());
    assertEquals(
        BsonDocument.parse(
            """
            {"$project": {
              "_id": "$_id",
              "count": {"total": "$count_value"},
              "by_color": {"buckets": {"$map": {
                "input": {"$slice": [
                  {"$sortArray": {
                    "input": "$_rows",
                    "sortBy": {"count_value": -1, "k1": 1}
                  }},
                  1
                ]},
                "as": "row",
                "in": {
                  "_id": "$$row.k1",
                  "sum_price": "$$row.m0"
                }
              }}}
            }}
            """),
        arm.get(3).asDocument());
  }

  @Test
  public void nestedTermsBoundaryTerms_sortsMiddleBoundaryOnParentRows() {
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec byColor = termsGroup("by_color", COLOR, List.of(leaf), 1);
    AggregationSpec byPrice = boundaryGroup("by_price", PRICE, List.of(byColor));
    AggregationSpec byCountry = termsGroup("by_country", COUNTRY, List.of(byPrice), 2);
    TopAggregationSpec top = new TopAggregationSpec(List.of(byCountry));

    BsonArray arm = buildPipeline(top).get(1).getDocument("$facet").getArray("f0");
    assertEquals(
        BsonDocument.parse("{\"$sort\": {\"count_value\": -1, \"_id\": 1}}"),
        arm.get(3).asDocument());
    assertEquals(BsonDocument.parse("{\"$limit\": 2}"), arm.get(4).asDocument());
    assertEquals(
        BsonDocument.parse(
            """
            {"$project": {
              "_id": "$_id",
              "count": {"total": "$count_value"},
              "by_price": {"buckets": {"$map": {
                "input": {"$sortArray": {"input": "$_rows", "sortBy": {"k1": 1}}},
                "as": "row",
                "in": {
                  "_id": "$$row.k1",
                  "count": {"total": "$$row.count_value"},
                  "by_color": {"buckets": {"$map": {
                    "input": {"$slice": [
                      {"$sortArray": {
                        "input": "$$row._leaf_rows",
                        "sortBy": {"count_value": -1, "k2": 1}
                      }},
                      1
                    ]},
                    "as": "leaf",
                    "in": {
                      "_id": "$$leaf.k2",
                      "sum_price": "$$leaf.m0"
                    }
                  }}}
                }
              }}}
            }}
            """),
        arm.get(5).asDocument());
  }

  @Test
  public void nestedTermsBoundaryTerms_defaultBucket_ranksParentKey() {
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec byColor = termsGroup("by_color", COLOR, List.of(leaf), 1);
    List<BsonValue> rangeKeys = List.of(new BsonInt32(0), new BsonInt32(100));
    AggregationSpec byPrice =
        new BoundaryGroupAggregationSpec(
            "by_price",
            PRICE,
            List.of(byColor),
            new long[] {0L, 100L, 200L},
            rangeKeys,
            false,
            new BoundaryArgs(
                rangeKeys,
                Optional.of(new BsonString("other")),
                Optional.of(TypeField.NUMBER_INT64_V2),
                Optional.empty()));
    AggregationSpec byCountry = termsGroup("by_country", COUNTRY, List.of(byPrice), 2);
    TopAggregationSpec top = new TopAggregationSpec(List.of(byCountry));

    BsonDocument project =
        buildPipeline(top).get(1).getDocument("$facet").getArray("f0").get(5).asDocument();
    assertEquals(
        BsonDocument.parse(
            """
            {"$project": {
              "_id": "$_id",
              "count": {"total": "$count_value"},
              "by_price": {"buckets": {"$map": {
                "input": {"$map": {
                  "input": {"$sortArray": {
                    "input": {"$map": {
                      "input": "$_rows",
                      "as": "row",
                      "in": {"$mergeObjects": [
                        "$$row",
                        {"__boundarySortRank": {"$cond": [
                          {"$eq": ["$$row.k1", "other"]},
                          1,
                          0
                        ]}}
                      ]}
                    }},
                    "sortBy": {"__boundarySortRank": 1, "k1": 1}
                  }},
                  "as": "row",
                  "in": {"$arrayToObject": {"$filter": {
                    "input": {"$objectToArray": "$$row"},
                    "as": "entry",
                    "cond": {"$ne": ["$$entry.k", "__boundarySortRank"]}
                  }}}
                }},
                "as": "row",
                "in": {
                  "_id": "$$row.k1",
                  "count": {"total": "$$row.count_value"},
                  "by_color": {"buckets": {"$map": {
                    "input": {"$slice": [
                      {"$sortArray": {
                        "input": "$$row._leaf_rows",
                        "sortBy": {"count_value": -1, "k2": 1}
                      }},
                      1
                    ]},
                    "as": "leaf",
                    "in": {
                      "_id": "$$leaf.k2",
                      "sum_price": "$$leaf.m0"
                    }
                  }}}
                }
              }}}
            }}
            """),
        project);
  }

  @Test
  public void multipleTopLevelGroupsProduceSeparateFacets() {
    AggregationSpec leaf1 = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec leaf2 = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec group1 = new TermsSpec("by_city", CITY, List.of(leaf1));
    AggregationSpec group2 = new TermsSpec("by_color", COLOR, List.of(leaf2));
    TopAggregationSpec top = new TopAggregationSpec(List.of(group1, group2));

    List<BsonDocument> pipeline = buildPipeline(top);

    BsonDocument expectedFacet =
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
            """);
    assertEquals(expectedFacet, pipeline.get(1));
  }

  @Test
  public void dottedGroupingPath_usesKeyAliasInProject() {
    FieldPath dottedPath = FieldPath.parse("meta.color");
    AggregationSpec leaf = new SumAggregationSpec("sum_price", PRICE);
    AggregationSpec group = new TermsSpec("by_meta", dottedPath, List.of(leaf));
    TopAggregationSpec top = new TopAggregationSpec(List.of(group));

    String facetJson = buildPipeline(top).get(1).toJson();

    assertEquals(-1, facetJson.indexOf("_id.keys.meta.color"));
    assertTrueContains(facetJson, "\"field\": \"k0\"");
  }

  @Test
  public void dottedFacetAndMetricNames_useFacetAndMetricAliases() {
    AggregationSpec leaf = new SumAggregationSpec("sum.Amt", PRICE);
    AggregationSpec group = new TermsSpec("by.city", CITY, List.of(leaf));
    TopAggregationSpec top = new TopAggregationSpec(List.of(group));

    String pipelineJson = buildPipeline(top).toString();

    assertTrue(pipelineJson.contains("\"f0\""));
    assertTrue(pipelineJson.contains("\"m0\""));
    assertTrue(pipelineJson.contains("\"by.city\""));
    assertTrue(pipelineJson.contains("\"sum.Amt\""));
    assertEquals(-1, pipelineJson.indexOf("sum_sum.Amt"));
    assertEquals(-1, pipelineJson.indexOf("$by.city"));
  }

  private static void assertTrueContains(String haystack, String needle) {
    if (!haystack.contains(needle)) {
      throw new AssertionError("Expected to contain: " + needle);
    }
  }

  private static TermsGroupAggregationSpec termsGroup(
      String name, FieldPath path, List<AggregationSpec> children, int numBuckets) {
    return new TermsGroupAggregationSpec(
        name, path, children, new TermsArgs(true, TypeField.TOKEN, numBuckets));
  }

  private static BoundaryGroupAggregationSpec boundaryGroup(
      String name, FieldPath path, List<AggregationSpec> children) {
    List<BsonValue> rangeKeys = List.of(new BsonInt32(0), new BsonInt32(100));
    return new BoundaryGroupAggregationSpec(
        name,
        path,
        children,
        new long[] {0L, 100L, 200L},
        rangeKeys,
        false,
        new BoundaryArgs(
            rangeKeys, Optional.empty(), Optional.of(TypeField.NUMBER_INT64_V2), Optional.empty()));
  }

  private static final class TermsSpec extends GroupAggregationSpec {
    TermsSpec(String name, FieldPath path, List<AggregationSpec> children) {
      super(path, name, children);
    }

    @Override
    public int groupingBucketCount() {
      return 1;
    }

    @Override
    protected Accumulator createAccumulator(
        AggregationContext context, List<Accumulator> childAccumulators) {
      throw new UnsupportedOperationException("not exercised in pipeline test");
    }
  }
}
