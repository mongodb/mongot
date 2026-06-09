load("@com_google_protobuf//bazel:java_proto_library.bzl", "java_proto_library")
load("@rules_buf//buf:defs.bzl", "buf_lint_test")
load("@rules_proto//proto:defs.bzl", "proto_library")

proto_library(
    name = "searchenvoy_proto",
    srcs = ["search-envoy.proto"],
    visibility = ["//visibility:public"],
)

buf_lint_test(
    name = "searchenvoy_proto_lint",
    config = ":buf.yaml",
    tags = ["lint"],
    targets = [":searchenvoy_proto"],
)

java_proto_library(
    name = "searchenvoy_java_proto",
    visibility = ["//visibility:public"],
    deps = [":searchenvoy_proto"],
)
