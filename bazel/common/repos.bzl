load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def common_repos():
    http_archive(
        name = "platforms",
        urls = [
            "https://mirror.bazel.build/github.com/bazelbuild/platforms/releases/download/1.0.0/platforms-1.0.0.tar.gz",
            "https://github.com/bazelbuild/platforms/releases/download/1.0.0/platforms-1.0.0.tar.gz",
        ],
        sha256 = "3384eb1c30762704fbe38e440204e114154086c8fc8a8c2e3e28441028c019a8",
    )

    http_archive(
        name = "rules_pkg",
        urls = [
            "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
            "https://github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
        ],
        sha256 = "8a298e832762eda1830597d64fe7db58178aa84cd5926d76d5b744d6558941c2",
    )

    # Required to generate a pom.xml file
    http_archive(
        name = "com_github_bazel_common",
        sha256 = "d8c9586b24ce4a5513d972668f94b62eb7d705b92405d4bc102131f294751f1d",
        strip_prefix = "bazel-common-413b433b91f26dbe39cdbc20f742ad6555dd1e27",
        url = "https://github.com/google/bazel-common/archive/413b433b91f26dbe39cdbc20f742ad6555dd1e27.zip",
    )

    http_archive(
        name = "rules_cc",
        urls = ["https://github.com/bazelbuild/rules_cc/releases/download/0.0.15/rules_cc-0.0.15.tar.gz"],
        sha256 = "f4aadd8387f381033a9ad0500443a52a0cea5f8ad1ede4369d3c614eb7b2682e",
        strip_prefix = "rules_cc-0.0.15",
    )
