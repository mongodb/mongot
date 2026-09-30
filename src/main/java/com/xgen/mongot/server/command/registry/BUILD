load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "registry",
    srcs = [
        "CommandNotFoundException.java",
        "CommandRegistry.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/server/command",
        "//src/main/java/com/xgen/mongot/server/command/builtin",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:io_micrometer_micrometer_core",
    ],
)
