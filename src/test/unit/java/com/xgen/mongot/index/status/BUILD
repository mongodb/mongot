load("//bazel/java:test.bzl", "mongot_java_unit_test_suite")

# gazelle:ignore

mongot_java_unit_test_suite(
    name = "lib",
    deps = [
        "//src/main/java/com/xgen/mongot/index/status",
        "@maven//:org_mongodb_bson",
    ],
)
