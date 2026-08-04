load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "builtin",
    srcs = [
        "BuildInfoCommand.java",
        "HelloCommand.java",
        "IsMasterCommand.java",
        "PingCommand.java",
        "WireProtocolCapabilities.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/server/command",
        "//src/main/java/com/xgen/mongot/server/message",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
