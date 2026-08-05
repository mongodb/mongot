load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "message",
    srcs = [
        "MessageMessageTest.java",
        "MessageUtilsTest.java",
        "OpCodeTest.java",
        "QueryMessageTest.java",
        "ReplyMessageTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/server/message",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/mongodb",
        "@maven//:com_google_truth_truth",
        "@maven//:io_netty_netty_buffer",
        "@maven//:io_netty_netty_codec",
        "@maven//:junit_junit",
        "@maven//:org_mongodb_bson",
    ],
)
