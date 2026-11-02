load("@rules_python//python:defs.bzl", "py_library", "py_test")

py_library(
    name = "affected_tests_lib",
    srcs = ["affected_tests.py"],
)

py_test(
    name = "affected_tests_test",
    size = "small",
    srcs = ["affected_tests_test.py"],
    tags = ["unit"],
    deps = [":affected_tests_lib"],
)
