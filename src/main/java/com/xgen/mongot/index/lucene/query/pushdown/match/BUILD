load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "match",
    srcs = [
        "DocumentMatcher.java",
        "MatchStage.java",
    ],
    visibility = ["//:__subpackages__"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/query/pushdown",
        "//src/main/java/com/xgen/mongot/index/query/operators/bound",
        "//src/main/java/com/xgen/mongot/index/query/operators/mql",
        "//src/main/java/com/xgen/mongot/index/query/operators/value",
        "//src/main/java/com/xgen/mongot/index/query/points",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
)
