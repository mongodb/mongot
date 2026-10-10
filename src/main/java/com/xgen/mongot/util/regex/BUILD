load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "regex",
    srcs = [
        "RegexBudget.java",
        "RegexInterruptedException.java",
        "RegexLimitExceededException.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:org_jetbrains_annotations",
    ],
)
