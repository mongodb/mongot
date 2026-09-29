load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "clients",
    srcs = [
        "ClientInterface.java",
        "EmbeddingClientFactory.java",
        "VoyageClient.java",
    ],
    visibility = ["//:__subpackages__"],
    exports = ["//src/main/java/com/xgen/mongot/embedding"],
    deps = [
        "//src/main/java/com/xgen/mongot/embedding",
        "//src/main/java/com/xgen/mongot/embedding/exceptions",
        "//src/main/java/com/xgen/mongot/embedding/providers/configs",
        "//src/main/java/com/xgen/mongot/embedding/providers/congestion",
        "//src/main/java/com/xgen/mongot/index/definition/quantization",
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/mongot/util/concurrent",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_mongodb_bson",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
