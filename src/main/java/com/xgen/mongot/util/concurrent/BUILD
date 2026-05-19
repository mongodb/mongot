load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "concurrent",
    srcs = [
        "DefaultNamedExecutorService.java",
        "DefaultNamedScheduledExecutorService.java",
        "Executors.java",
        "FileLockGuard.java",
        "LiveThreadIdsRegistry.java",
        "LockGuard.java",
        "MeteredCallerRunsPolicy.java",
        "NamedExecutorService.java",
        "NamedScheduledExecutorService.java",
        "OneShotSingleThreadExecutor.java",
        "RecurringTimer.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:io_micrometer_micrometer_core"],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
