load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "abortable",
    srcs = [
        "AbortableDirectory.java",
        "AbortableRandomVectorScorerSupplier.java",
        "MergeCancellationContext.java",
    ],
    visibility = ["//:__subpackages__"],
    exports = ["@maven//:org_apache_lucene_lucene_core"],
    deps = [
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
