load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "exceptions",
    srcs = [
        "EmbeddingProviderBatchingException.java",
        "EmbeddingProviderNonTransientException.java",
        "EmbeddingProviderRateLimitException.java",
        "EmbeddingProviderTransientException.java",
        "MaterializedViewNonTransientException.java",
        "MaterializedViewTransientException.java",
    ],
    visibility = ["//:__subpackages__"],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
    ],
)
