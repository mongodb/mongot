"""Format targets for Java and Rust source files."""

load("@aspect_rules_lint//format:defs.bzl", "format_multirun")
load("@rules_java//java:defs.bzl", "java_binary")
load("@rules_shell//shell:sh_test.bzl", "sh_test")

package(default_visibility = ["//visibility:public"])

java_binary(
    name = "java-format",
    jvm_flags = [
        "--add-exports jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED",
        "--add-exports jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED",
        "--add-exports jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED",
        "--add-exports jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED",
        "--add-exports jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED",
    ],
    main_class = "com.google.googlejavaformat.java.Main",
    runtime_deps = ["@google-java-format//jar"],
)

# Per-language configuration for all format and lint checks provided by aspect_rules_lint
# Use `bazel run //bazel/tools/format` to fix formatting in all files of the current subtree.
# Use `bazel run //bazel/tools/format/format.check` to only validate formatting in the current subtree.
format_multirun(
    name = "format",
    java = ":java-format",
    python = "@aspect_rules_lint//lint:ruff_bin",
    rust = "@rules_rust//tools/upstream_wrapper:rustfmt",
    rust_check_args = [
        "--edition",
        "2024",
    ],
    rust_fix_args = [
        "--edition",
        "2024",
    ],
    starlark = "@buildifier_prebuilt//:buildifier",
)

# Wrap :format.check so it can be used via bazel test in CI.
sh_test(
    name = "format_test",
    srcs = ["format_test.sh"],
    # The script will invoke :format.check from the root of the repo
    args = [
        "$(rootpath :format.check)",
        "$(rootpath //:MODULE.bazel)",
    ],
    data = [
        ":format.check",
        "//:MODULE.bazel",
    ],
    # local = True ensures it can see your repository files directly
    local = True,
    # Run as part of make lint, but NEVER cache since it depends on external state.
    tags = [
        "external",
        "lint",
    ],
)
