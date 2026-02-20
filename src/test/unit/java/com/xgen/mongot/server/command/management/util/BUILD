load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "util",
    srcs = [
        "IndexEntryMapperTest.java",
        "IndexMapperTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/catalogservice",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/status",
        "//src/main/java/com/xgen/mongot/index/synonym",
        "//src/main/java/com/xgen/mongot/server/command/management/definition",
        "//src/main/java/com/xgen/mongot/server/command/management/util",
        "//src/main/java/com/xgen/testing/mongot/index/definition",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
