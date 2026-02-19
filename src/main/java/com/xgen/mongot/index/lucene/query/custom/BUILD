load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "custom",
    srcs = [
        "ExactVectorSearchQuery.java",
        "MongotKnnByteQuery.java",
        "MongotKnnFloatQuery.java",
        "WrappedKnnQuery.java",
        "WrappedQuery.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "@maven//:org_apache_lucene_lucene_core",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/lucene/util",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
