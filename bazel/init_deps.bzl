load("//bazel/java:init_deps.bzl", "java_init_deps")
load("//bazel/rust:init_deps.bzl", "rust_init_deps")

def mongot_init_deps():
    java_init_deps()
    rust_init_deps()
