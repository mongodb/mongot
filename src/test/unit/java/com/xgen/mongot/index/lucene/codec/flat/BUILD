load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "flat",
    srcs = [
        "BinaryQuantizedFlatVectorsFormatTest.java",
        "FlatBitVectorsFormatTest.java",
        "Float32AndByteFlatVectorsFormatTest.java",
        "FullScanFlatVectorsReaderDecoratorTest.java",
        "NativeFlatVectorsScoringParityTest.java",
        "ScalarQuantizedFlatVectorsFormatTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/featureflag/dynamic",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/codec/flat",
        "//src/main/java/com/xgen/mongot/index/lucene/quantization",
        "//src/main/java/com/xgen/mongot/index/lucene/vector",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing/util",
        "@lucene_fork//:org_apache_lucene_lucene_backward_codecs",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_test_framework",
        "@maven//:org_mongodb_bson",
    ],
)
