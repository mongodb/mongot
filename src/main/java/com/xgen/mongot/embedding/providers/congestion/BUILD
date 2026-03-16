load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "congestion",
    srcs = [
        "AimdCongestionControl.java",
        "DynamicSemaphore.java",
        "DynamicSemaphorePolicy.java",
    ],
    visibility = ["//:__subpackages__"],
    deps = [
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
