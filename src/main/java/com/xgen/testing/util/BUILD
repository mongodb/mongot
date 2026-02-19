load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "util",
    testonly = True,
    srcs = [
        "BsonDocumentUtil.java",
        "ByteArrayUtil.java",
        "FunctionalTestUtils.java",
        "ManuallyUpdatedClock.java",
        "MockRuntimeBuilder.java",
        "MongoDbUtil.java",
        "RandomSegmentingIndexWriter.java",
        "VectorTestUtils.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/mongot/util/functionalinterfaces",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_test_framework",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_sync",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
