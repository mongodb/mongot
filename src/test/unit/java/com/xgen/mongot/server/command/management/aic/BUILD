load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "aic",
    srcs = [
        "AicCreateSearchIndexesCommandTest.java",
        "AicDropSearchIndexCommandTest.java",
        "AicListSearchIndexesCommandTest.java",
        "AicUpdateSearchIndexCommandTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/catalogservice",
        "//src/main/java/com/xgen/mongot/config/manager",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/status",
        "//src/main/java/com/xgen/mongot/server/command/management/aic",
        "//src/main/java/com/xgen/mongot/server/command/management/definition",
        "//src/main/java/com/xgen/mongot/server/command/management/definition/common",
        "//src/main/java/com/xgen/mongot/server/command/management/util",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/mongot/util/mongodb",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/definition",
        "//src/main/java/com/xgen/testing/mongot/server/command/management/definition",
        "@maven//:junit_junit",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
