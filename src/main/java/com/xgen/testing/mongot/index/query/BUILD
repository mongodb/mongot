load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "query",
    testonly = True,
    srcs = [
        "ApproximateVectorQueryCriteriaBuilder.java",
        "CollectorQueryBuilder.java",
        "ExactVectorCriteriaBuilder.java",
        "OperatorQueryBuilder.java",
        "QueryOptimizationFlagsBuilder.java",
        "VectorQueryBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/query",
        "//src/main/java/com/xgen/mongot/index/query/operators",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/query",
        "//src/main/java/com/xgen/mongot/index/query/collectors",
        "//src/main/java/com/xgen/mongot/index/query/counts",
        "//src/main/java/com/xgen/mongot/index/query/highlights",
        "//src/main/java/com/xgen/mongot/index/query/operators",
        "//src/main/java/com/xgen/mongot/index/query/sort",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:org_mongodb_bson",
    ],
)
