load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "cli",
    srcs = ["CliOptionRegistryTest.java"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/config/provider/cli",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
