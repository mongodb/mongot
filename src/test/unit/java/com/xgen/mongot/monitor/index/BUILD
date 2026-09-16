load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "index",
    srcs = ["OnDiskIndexSizeSamplerTest.java"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/catalog",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/monitor/index",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mongodb_bson",
    ],
)
