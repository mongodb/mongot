load("@rules_python//python:defs.bzl", "py_binary")

py_binary(
    name = "compatibility_test",
    srcs = ["compatibility_test.py"],
    data = [
        ":mongod.conf",
        ":mongodSetupStage1.js",
        ":mongodSetupStage2.js",
        ":mongot.conf",
    ],
    python_version = "PY3",
)
