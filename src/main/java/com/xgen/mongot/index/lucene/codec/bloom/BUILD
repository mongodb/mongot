load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "bloom",
    srcs = [
        "BloomCodecPolicy.java",
        "MongotBloomFieldsProducerRegistry.java",
        "MongotBloomFilteringPostingsFormat.java",
        "MongotBloomHeapEvictor.java",
        "MongotBloomReadPolicy.java",
    ],
    # Cloned from Lucene; exclude from Error Prone (paths use slashes, not package dots).
    javacopts = ["-XepExcludedPaths:.*/codec/bloom/MongotBloomFilteringPostingsFormat\\.java"],
    visibility = ["//:__subpackages__"],
    exports = [
        "@maven//:org_apache_lucene_lucene_codecs",
        "@maven//:org_apache_lucene_lucene_core",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/featureflag/dynamic",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/status",
        "@maven//:com_google_auto_service_auto_service_annotations",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:org_apache_lucene_lucene_codecs",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
