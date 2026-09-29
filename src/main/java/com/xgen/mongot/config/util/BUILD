load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "util",
    srcs = [
        "AuthMode.java",
        "HysteresisConfig.java",
        "IndexDefinitions.java",
        "IndexIdentity.java",
        "Invariants.java",
        "TlsMode.java",
        "ViewValidator.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/version",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/catalog",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/analyzer",
        "//src/main/java/com/xgen/mongot/index/analyzer/definition",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/mongot/util/functionalinterfaces",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_commons_commons_collections4",
        "@maven//:org_mongodb_bson",
    ],
)
