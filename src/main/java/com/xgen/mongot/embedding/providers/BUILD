load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "providers",
    srcs = [
        "EmbeddingProviderManager.java",
        "EmbeddingServiceManager.java",
        "EmbeddingServiceRegistry.java",
    ],
    visibility = ["//:__subpackages__"],
    exports = ["//src/main/java/com/xgen/mongot/embedding"],
    deps = [
        "//src/main/java/com/xgen/mongot/embedding",
        "//src/main/java/com/xgen/mongot/embedding/exceptions",
        "//src/main/java/com/xgen/mongot/embedding/providers/clients",
        "//src/main/java/com/xgen/mongot/embedding/providers/configs",
        "//src/main/java/com/xgen/mongot/embedding/providers/congestion",
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/concurrent",
        "//src/main/java/com/xgen/mongot/util/retry",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:net_jodah_failsafe",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
