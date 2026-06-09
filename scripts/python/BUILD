load("@mongot_pip//:requirements.bzl", "requirement")
load("@rules_python//python:defs.bzl", "py_binary")

py_binary(
    name = "example",
    srcs = ["example.py"],
    deps = [requirement("requests")],
)
