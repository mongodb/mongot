load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "cache",
    srcs = [
        "IntervalThrottle.java",
        "ScrapeCache.java",
        "ScrapeCacheConfig.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/mongot/util/concurrent",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:io_micrometer_micrometer_registry_prometheus",
        "@maven//:org_mongodb_bson",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
