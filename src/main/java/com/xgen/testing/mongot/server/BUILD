load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "server",
    testonly = True,
    srcs = [
        "ClientAndServer.java",
        "Mocks.java",
    ],
    visibility = ["//visibility:public"],
    exports = ["@maven//:org_mongodb_bson"],
    deps = [
        "//src/main/java/com/xgen/mongot/catalog",
        "//src/main/java/com/xgen/mongot/config/manager",
        "//src/main/java/com/xgen/mongot/cursor",
        "//src/main/java/com/xgen/mongot/embedding/providers",
        "//src/main/java/com/xgen/mongot/featureflag/dynamic",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mongodb_bson",
    ],
)
