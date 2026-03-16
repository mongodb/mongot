load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "clients",
    srcs = [
        "VoyageClientTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/embedding",
        "//src/main/java/com/xgen/mongot/embedding/exceptions",
        "//src/main/java/com/xgen/mongot/embedding/providers/clients",
        "//src/main/java/com/xgen/mongot/embedding/providers/configs",
        "//src/main/java/com/xgen/mongot/embedding/providers/congestion",
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/util/bson",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mockito_mockito_subclass",  #keep
        "@maven//:org_mongodb_bson",
    ],
)
