load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "http",
    srcs = ["HttpServers.java"],
    visibility = ["//:__subpackages__"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/retry",
        "@maven//:com_google_guava_guava",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
