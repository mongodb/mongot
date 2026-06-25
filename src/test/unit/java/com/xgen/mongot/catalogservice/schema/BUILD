load("//bazel/java:test.bzl", "mongot_java_unit_test_suite")

# gazelle:ignore

mongot_java_unit_test_suite(
    name = "lib",
    resources = True,
    deps = [
        "//src/main/java/com/xgen/mongot/catalogservice/schema",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "//src/main/java/com/xgen/testing",
        "//src/main/java/com/xgen/testing/mongot/index/definition",
        "@maven//:org_mongodb_bson",
    ],
)
