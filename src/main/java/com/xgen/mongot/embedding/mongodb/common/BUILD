load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "common",
    srcs = ["AutoEmbeddingMongoClient.java"],
    visibility = ["//:__subpackages__"],
    exports = [
        "//src/main/java/com/xgen/mongot/util/mongodb",
        "@maven//:org_mongodb_mongodb_driver_sync",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/util/mongodb",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:org_mongodb_mongodb_driver_core",
        "@maven//:org_mongodb_mongodb_driver_sync",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
