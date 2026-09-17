load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "version",
    srcs = [
        "FormatVersionTest.java",
        "GenerationIdTest.java",
        "GenerationTest.java",
        "IndexFormatVersionTest.java",
        "UserIndexVersionTest.java",
        "VectorliteFormatVersionTest.java",
    ],
    data = ["//src/test/unit/resources/index/version:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/util/functionalinterfaces",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/version",
        "@maven//:com_google_guava_guava",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
