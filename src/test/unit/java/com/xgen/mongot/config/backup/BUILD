load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "backup",
    srcs = [
        "ConfigJournalV1Test.java",
        "DynamicFeatureFlagJournalTest.java",
        "DynamicFeatureFlagJournalWriterTest.java",
        "JournalEditorTest.java",
        "JournalEntryIdentityTest.java",
    ],
    data = ["//src/test/unit/resources/config/backup:srcs"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/config/backup",
        "//src/main/java/com/xgen/mongot/config/util",
        "//src/main/java/com/xgen/mongot/featureflag/dynamic",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/config/backup",
        "//src/main/java/com/xgen/testing/mongot/index/analyzer/definition",
        "//src/main/java/com/xgen/testing/mongot/index/definition",
        "//src/main/java/com/xgen/testing/mongot/mock/index",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
