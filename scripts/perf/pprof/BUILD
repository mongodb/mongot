load("@com_google_protobuf//bazel:py_proto_library.bzl", "py_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_python//python:defs.bzl", "py_binary")

proto_library(
    name = "profile_proto",
    srcs = ["profile.proto"],
    visibility = ["//visibility:private"],
)

py_proto_library(
    name = "profile_py_proto",
    visibility = ["//visibility:private"],
    deps = [":profile_proto"],
)

py_binary(
    name = "parse_pprof",
    srcs = ["parse_pprof.py"],
    main = "parse_pprof.py",
    python_version = "PY3",
    visibility = ["//visibility:private"],
    deps = [":profile_py_proto"],
)
