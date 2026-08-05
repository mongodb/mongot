load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "message",
    srcs = [
        "InboundMessage.java",
        "MessageHeader.java",
        "MessageLimits.java",
        "MessageMessage.java",
        "MessageSection.java",
        "MessageSectionBody.java",
        "MessageSectionDocumentSequence.java",
        "MessageUtils.java",
        "OpCode.java",
        "OutboundMessage.java",
        "QueryMessage.java",
        "ReplyMessage.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:io_netty_netty_buffer",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/mongodb",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:io_netty_netty_buffer",
        "@maven//:io_netty_netty_codec",
        "@maven//:org_mongodb_bson",
    ],
)
