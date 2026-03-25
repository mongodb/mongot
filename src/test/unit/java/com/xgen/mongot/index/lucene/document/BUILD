load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "document",
    srcs = ["DefaultIndexingPolicyTest.java"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/ingestion",
        "//src/main/java/com/xgen/mongot/index/lucene/document",
        "//src/main/java/com/xgen/mongot/index/lucene/document/block",
        "//src/main/java/com/xgen/mongot/index/lucene/document/builder",
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/lucene/util",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing/mongot/index/definition",
        "//src/main/java/com/xgen/testing/mongot/mock/index",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
)
