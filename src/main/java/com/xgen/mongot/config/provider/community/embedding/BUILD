load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "embedding",
    srcs = [
        "AutoEmbeddingIndexValidator.java",
        "EmbeddingConfig.java",
        "EmbeddingServiceManagerConfig.java",
    ],
    resources = ["//src/main/resources/config/community:embedding-service-configs"],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/config/provider/community/parser",
        "//src/main/java/com/xgen/mongot/embedding/providers/configs",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:org_mongodb_bson",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
