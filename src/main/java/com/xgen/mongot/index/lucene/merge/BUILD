load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "merge",
    srcs = [
        "DiskUtilizationAwareMergePolicy.java",
        "InstrumentedConcurrentMergeScheduler.java",
        "MergePolicyFactory.java",
        "PausableDirectory.java",
        "VectorMergePolicy.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/monitor",
        "@maven//:org_apache_lucene_lucene_core",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/featureflag",
        "//src/main/java/com/xgen/mongot/index/lucene/abortable",
        "//src/main/java/com/xgen/mongot/index/lucene/codec/bloom",
        "//src/main/java/com/xgen/mongot/index/lucene/config",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/monitor",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/concurrent",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
