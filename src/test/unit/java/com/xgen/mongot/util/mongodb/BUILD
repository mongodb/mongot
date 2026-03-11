load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "mongodb",
    srcs = [
        "BatchMongoClientTest.java",
        "ChangeStreamResponseTest.java",
        "ConnectionStringBuilderTest.java",
        "ConnectionStringUtilTest.java",
        "FindCommandResponseTest.java",
        "GetMoreResponseTest.java",
        "InstrumentedConnectionPoolListenerFactoryTest.java",
        "MongoClientBuilderTest.java",
        "MongoCredentialsTest.java",
        "MongoDbMetadataClientTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/replication/mongodb/common",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/mongodb",
        "//src/main/java/com/xgen/mongot/util/mongodb/serialization",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_fasterxml_jackson_dataformat_jackson_dataformat_yaml",
        "@maven//:com_google_flogger_flogger_slf4j_backend",  # keep
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_hamcrest_hamcrest",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
        "@maven//:org_mongodb_mongodb_driver_sync",
    ],
)
