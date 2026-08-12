load("@rules_python//python:pip.bzl", "compile_pip_requirements")
load("@rules_shell//shell:sh_test.bzl", "sh_test")

compile_pip_requirements(
    "requirements",
    src = "requirements.in",
    python_version = "3.11",
    requirements_txt = "requirements_lock.txt",
)

sh_test(
    name = "ruff_check_test",
    srcs = ["ruff_check_test.sh"],
    args = [
        "$(rootpath @aspect_rules_lint//lint:ruff_bin)",
        "$(rootpath //:MODULE.bazel)",
    ],
    data = [
        "//:MODULE.bazel",
        "@aspect_rules_lint//lint:ruff_bin",
    ],
    # The script scans the repository directly rather than declared inputs.
    local = True,
    # Run as part of make lint, but NEVER cache since it depends on external state.
    tags = [
        "external",
        "lint",
    ],
)

alias(
    name = "codeowners",
    actual = "@bazel_rules_mongo//codeowners:codeowners",
    visibility = ["//visibility:public"],
)
