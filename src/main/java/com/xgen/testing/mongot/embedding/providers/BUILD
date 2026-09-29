load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "providers",
    srcs = [
        "FakeEmbeddingClientFactory.java",
        "FakeEmbeddingProviderClient.java",
    ],
    visibility = ["//:__subpackages__"],
    exports = [
        "//src/main/java/com/xgen/mongot/embedding",
        "//src/main/java/com/xgen/mongot/embedding/providers/clients",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/embedding",
        "//src/main/java/com/xgen/mongot/embedding/exceptions",
        "//src/main/java/com/xgen/mongot/embedding/providers/clients",
        "//src/main/java/com/xgen/mongot/embedding/providers/configs",
        "//src/main/java/com/xgen/mongot/embedding/providers/congestion",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
    ],
)
