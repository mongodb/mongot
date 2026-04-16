load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "embedding",
    srcs = [
        "AutoEmbedFieldMapping.java",
        "AutoEmbeddingMemoryBudget.java",
        "EmbeddingRequestContext.java",
        "MongotMetadata.java",
        "VectorAutoEmbedFieldMapping.java",
        "VectorOrError.java",
    ],
    visibility = ["//:__subpackages__"],
    exports = ["//src/main/java/com/xgen/mongot/util"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/definition/quantization",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_flogger_flogger",
        "@maven//:com_google_guava_guava",
    ],
)
