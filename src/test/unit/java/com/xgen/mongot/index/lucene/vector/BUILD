load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "vector",
    srcs = [
        "HnswWriteParamsTest.java",
        "Lucene99NativeHnswVectorsFormatTest.java",
        "Lucene99NativeMemorySegmentVectorScorerTest.java",
        "Lucene99NativeTestCodec.java",
        "NativeHnswScalarQuantizedVectorsFormatTest.java",
        "NativeScalarQuantizedScoringParityTest.java",
        "NativeScalarQuantizedTestCodec.java",
        "SimilarityTest.java",
    ],
    javacopts = ["--enable-preview"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/featureflag/dynamic",
        "//src/main/java/com/xgen/mongot/index/lucene/vector",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing/mongot/index/lucene/vector",
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_backward_codecs",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_test_framework",
        "@maven//:org_mongodb_bson",
    ],
)
