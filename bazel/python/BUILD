load("@rules_python//python:pip.bzl", "compile_pip_requirements")

compile_pip_requirements(
    "requirements",
    src = "requirements.in",
    requirements_txt = "requirements_lock.txt",
)

alias(
    name = "codeowners",
    actual = "@bazel_rules_mongo//codeowners:codeowners",
    visibility = ["//visibility:public"],
)
