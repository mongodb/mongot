load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "prometheus",
    srcs = ["PrometheusServer.java"],
    visibility = ["//visibility:public"],
    exports = ["@maven//:io_micrometer_micrometer_registry_prometheus"],
    deps = [
        "//src/main/java/com/xgen/mongot/featureflag",
        "//src/main/java/com/xgen/mongot/metrics/cache",
        "//src/main/java/com/xgen/mongot/util/retry",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:io_micrometer_micrometer_registry_prometheus",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
