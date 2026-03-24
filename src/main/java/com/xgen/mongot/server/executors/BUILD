load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "executors",
    srcs = [
        "BulkheadCommandExecutor.java",
        "CancelledStreamSkipException.java",
        "ExecutorManager.java",
        "LoadSheddingRejectedException.java",
        "RegularBlockingRequestSettings.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:io_netty_netty_transport",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/server/command",
        "//src/main/java/com/xgen/mongot/server/util",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/concurrent",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:io_netty_netty_transport",
        "@maven//:org_mongodb_bson",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
