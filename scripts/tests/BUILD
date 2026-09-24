load("@rules_python//python:defs.bzl", "py_binary", "py_test")

py_binary(
    name = "combine_test_results",
    srcs = ["combine_test_results.py"],
    data = ["//:test-results"],
    python_version = "PY3",
)

py_test(
    name = "combine_test_results_test",
    srcs = [
        "combine_test_results.py",
        "combine_test_results_test.py",
    ],
    main = "combine_test_results_test.py",
    python_version = "PY3",
    tags = ["lint"],
)
