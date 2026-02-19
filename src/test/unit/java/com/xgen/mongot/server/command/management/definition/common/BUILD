load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "common",
    srcs = [
        "UserSearchIndexDefinitionTest.java",
        "UserVectorIndexDefinitionTest.java",
    ],
    data = ["//src/test/unit/resources/server/command/management/definition/common:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/server/command/management/definition/common",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/definition",
        "//src/main/java/com/xgen/testing/mongot/server/command/management/definition",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
