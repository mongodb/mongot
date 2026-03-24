load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "grpc",
    srcs = [
        "BsonMessageCallHandlerTest.java",
        "CommandManagerTest.java",
        "HealthManagerTest.java",
        "MessageMarshallerTest.java",
        "RawBsonMarshallerTest.java",
        "SearchEnvoyMetadataInterceptorTest.java",
        "StreamingMessageHandlerTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/config/manager",
        "//src/main/java/com/xgen/mongot/cursor",
        "//src/main/java/com/xgen/mongot/server/command",
        "//src/main/java/com/xgen/mongot/server/command/registry",
        "//src/main/java/com/xgen/mongot/server/executors",
        "//src/main/java/com/xgen/mongot/server/grpc",
        "//src/main/java/com/xgen/mongot/server/message",
        "//src/proto/searchenvoy:searchenvoy_java_proto",
        "@io_grpc_grpc_java//api",
        "@io_grpc_grpc_java//stub",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_truth_truth",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:io_netty_netty_buffer",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mongodb_bson",
    ],
)
