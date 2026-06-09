load("@buildifier_prebuilt//:rules.bzl", "buildifier", "buildifier_test")

buildifier(
    name = "buildifier-fix",
    exclude_patterns = [
        "./.git/*",
        "./.ijwb/*",
        "./.bazelbsp/*",
    ],
    lint_mode = "fix",
    lint_warnings = [
        "+unsorted-dict-items",
    ],
    mode = "fix",
)

buildifier_test(
    name = "buildifier-check",
    exclude_patterns = [
        "./.git/*",
        "./.ijwb/*",
        "./.bazelbsp/*",
    ],
    mode = "check",
    no_sandbox = True,
    tags = ["lint"],
    workspace = "//:MODULE.bazel",
)
