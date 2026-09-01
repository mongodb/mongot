load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "util",
    srcs = [
        "LuceneCodecUtilsTest.java",
        "LuceneDocumentIdEncoderTest.java",
        "LuceneDoubleConversionUtilsTest.java",
        "VectorSearchUtilTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/definition/quantization",
        "//src/main/java/com/xgen/mongot/index/lucene/util",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/definition",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
)
