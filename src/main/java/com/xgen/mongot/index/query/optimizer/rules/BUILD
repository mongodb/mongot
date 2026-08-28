load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "rules",
    srcs = [
        "DefaultRuleRegistry.java",
        "RangeBoundsMergeMqlAndClauseRule.java",
        "RangeBoundsMergeMqlSimpleClauseRule.java",
        "RangeBoundsMergeOperatorRule.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/featureflag/dynamic",
        "//src/main/java/com/xgen/mongot/index/query/operators",
        "//src/main/java/com/xgen/mongot/index/query/operators/mql",
        "//src/main/java/com/xgen/mongot/index/query/optimizer",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/featureflag/dynamic",
        "//src/main/java/com/xgen/mongot/index/query/operators",
        "//src/main/java/com/xgen/mongot/index/query/operators/bound",
        "//src/main/java/com/xgen/mongot/index/query/operators/mql",
        "//src/main/java/com/xgen/mongot/index/query/operators/type",
        "//src/main/java/com/xgen/mongot/index/query/optimizer",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:com_google_errorprone_error_prone_annotations",
    ],
)
