load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "definition",
    srcs = [
        "ListSearchIndexesResponseDefinitionTest.java",
        "ManagementCommandDefinitionTest.java",
    ],
    data = ["//src/test/unit/resources/server/command/management/definition:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/server/command/management/definition",
        "//src/main/java/com/xgen/mongot/server/command/management/definition/common",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/definition",
        "//src/main/java/com/xgen/testing/mongot/server/command/management/definition",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
