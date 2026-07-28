load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "range",
    srcs = [
        "InclusiveRangeFactory.java",
        "RangeBoundToDateTransformer.java",
        "RangeBoundToDoubleTransformer.java",
        "RangeBoundToLongTransformer.java",
        "RangeQueryUtils.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_apache_lucene_lucene_core"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/lucene/query/util",
        "//src/main/java/com/xgen/mongot/index/query/operators/bound",
        "//src/main/java/com/xgen/mongot/index/query/points",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
