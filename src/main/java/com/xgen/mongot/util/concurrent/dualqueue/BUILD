load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "dualqueue",
    srcs = [
        "DualBlockingQueue.java",
        "DualQueueExecutor.java",
    ],
    visibility = ["//:__subpackages__"],
    exports = ["//src/main/java/com/xgen/mongot/util/concurrent"],
    deps = [
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/util/concurrent",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:org_jetbrains_annotations",
    ],
)
