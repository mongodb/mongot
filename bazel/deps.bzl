load("//bazel/buildifier:deps.bzl", "buildifier_deps")
load("//bazel/docker:deps.bzl", "docker_deps")
load("//bazel/java:deps.bzl", "java_deps")
load("//bazel/python:deps.bzl", "python_deps")
load("//bazel/releases:deps.bzl", "release_deps")
load("//bazel/rust:deps.bzl", "rust_deps")

def mongot_deps():
    buildifier_deps()
    docker_deps()
    java_deps()
    python_deps()
    release_deps()
    rust_deps()
