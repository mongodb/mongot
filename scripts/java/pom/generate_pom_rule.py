#!/usr/bin/env python
"""
prints a BUILD file containing one pom_file rule
made up from a list of targets supplied to stdin,
(for instance, output of `bazel query //...`)
"""

import json
from sys import stdin

_TEMPLATE = """
load("@com_github_bazel_common//tools/maven:pom_file.bzl", "pom_file")
load("@rules_java//java:defs.bzl", "java_library")

# Add dummy java_library wrapper because the pom_file only include dependencies of its targets, not
# the targets itself.
java_library(
    name = "all_maven_deps",
    testonly = True,
    exports = {},
)

pom_file(
    testonly = True,
    targets = [":all_maven_deps"],
    name = "pom",
    template_file = "pom_template.xml"
)
"""


def main():
    targets = stdin.read().splitlines()

    rule = _TEMPLATE.format(json.dumps(targets))
    print(rule)


if __name__ == "__main__":
    main()
