load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "community",
    testonly = True,
    srcs = [
        "CustomOidcConfigBuilder.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["//src/main/java/com/xgen/mongot/config/provider/community"],
    deps = [
        "//src/main/java/com/xgen/mongot/config/provider/community",
    ],
)
