load("@mongot_pip//:requirements.bzl", "requirement")
load("@rules_python//python:defs.bzl", "py_library", "py_test")

py_library(
    name = "tools",
    srcs = glob(
        ["*.py"],
        exclude = [
            "check_catalog_workflow_drift.py",
            "test_check_catalog_workflow_drift.py",
        ],
    ),
    srcs_version = "PY3",
    visibility = ["//visibility:public"],
)

py_test(
    name = "check_catalog_workflow_drift_unit_test",
    srcs = [
        "check_catalog_workflow_drift.py",
        "test_check_catalog_workflow_drift.py",
    ],
    main = "test_check_catalog_workflow_drift.py",
    tags = ["lint"],
    deps = [requirement("PyYAML")],
)

py_test(
    name = "catalog_workflow_drift_test",
    srcs = ["check_catalog_workflow_drift.py"],
    args = ["$(location //:catalog-info.yaml)"],
    data = ["//:catalog-info.yaml"],
    main = "check_catalog_workflow_drift.py",
    tags = ["lint"],
    deps = [requirement("PyYAML")],
)
