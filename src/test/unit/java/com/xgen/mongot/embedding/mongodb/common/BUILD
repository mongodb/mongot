load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "common",
    srcs = [
        "AutoEmbeddingMongoClientTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/embedding/mongodb/common",
        "//src/main/java/com/xgen/mongot/util/mongodb",
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mongodb_mongodb_driver_sync",
    ],
)
