load("@rules_python//python:defs.bzl", "py_binary")

py_binary(
    name = "combine_test_results",
    srcs = ["combine_test_results.py"],
    data = ["//:test-results"],
    python_version = "PY3",
)
