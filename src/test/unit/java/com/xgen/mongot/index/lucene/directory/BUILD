load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "directory",
    srcs = [
        "ByteReadCollectorTest.java",
        "CacheWarmerTimerTest.java",
        "FileSystemDirectoryTest.java",
        "IndexDirectoryFactoryTest.java",
        "IndexDirectoryHelperTest.java",
        "InstrumentedRandomAccessInputTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/config",
        "//src/main/java/com/xgen/mongot/index/lucene/directory",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/testing/mongot/index/lucene",
        "//src/main/java/com/xgen/testing/mongot/mock/index",
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mongodb_bson",
    ],
)
