load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "definition",
    srcs = [
        "EmbeddingModelCatalogTestUtilsTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/embedding/providers/configs",
        "//src/main/java/com/xgen/testing/mongot/index/definition",
        "@maven//:junit_junit",
    ],
)
