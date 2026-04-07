#!/usr/bin/env python3
"""Determines which unit test targets are affected by changed files in the current PR
and writes them to test_filter.targets.

Exit codes:
  0 - --output-file written; caller should run:
        bazel test --test_output=errors --target_pattern_file=<output-file>
  1 - caller should run all unit tests (detached HEAD, no changed files, or no
      affected targets found)
"""

import argparse
import os
import subprocess
import sys


__CRITICAL_FILES = [".bazelrc", ".evergreen.yml", "conf/evergreen"]


def run(cmd):
    result = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def is_detached_head():
    rc, _, _ = run(["git", "symbolic-ref", "--quiet", "HEAD"])
    return rc != 0


def get_changed_files(base_branch):
    rc, merge_base, err = run(["git", "merge-base", "HEAD", base_branch])
    if rc != 0:
        print(f"Could not determine merge base: {err}", file=sys.stderr)
        return []
    rc, stdout, _ = run(["git", "diff", "--name-only", merge_base])
    if rc != 0:
        return []
    return [f for f in stdout.splitlines() if f]


def query_bazel_target(bazel, filepath):
    rc, stdout, _ = run([bazel, "query", filepath])
    if rc != 0:
        return None
    return stdout or None


def query_affected_tests(bazel, tag, affected_targets_expr):
    query = f'attr(tags, "{tag}", tests(rdeps(//src/..., {affected_targets_expr})))'
    rc, stdout, err = run([bazel, "query", query])
    if rc != 0:
        print(f"Bazel query for affected tests failed: {err}", file=sys.stderr)
        return []
    return [t for t in stdout.splitlines() if t]


def main():
    parser = argparse.ArgumentParser(
        description="Find unit tests affected by PR changes and write them to test_filter.targets."
    )
    parser.add_argument(
        "--base-branch",
        default="master",
        help="Base branch to diff against (default: master)",
    )
    parser.add_argument(
        "--bazel",
        default="bazel",
        help="Path to bazel or bazelisk (default: bazel)",
    )
    parser.add_argument(
        "--output-file",
        default=None,
        required=True,
        help="Path to write the target pattern file",
    )
    parser.add_argument(
        "--tag",
        default=None,
        required=True,
        help="Bazel tag to filter tests by (e.g. 'unit')",
    )
    parser.add_argument(
        "--changed-files",
        default="",
        help="Comma-separated list of changed files to use instead of deriving from --base-branch",
    )
    parser.add_argument(
        "--force-all",
        action="store_true",
        help="Run all tests without filtering (also set by FORCE_ALL_TESTS=1 or FORCE_ALL_TESTS=true)",
    )
    args = parser.parse_args()

    if not args.tag:
        print("--tag is required", file=sys.stderr)
        return 1

    force_all_env = os.environ.get("FORCE_ALL_TESTS", "").lower() in ("1", "true")
    if args.force_all or force_all_env:
        print("Force-all tests enabled, running all unit tests", file=sys.stderr)
        return 1

    if args.changed_files:
        changed_files = [f.strip() for f in args.changed_files.split(",") if f.strip()]
    else:
        if is_detached_head():
            print("Detached HEAD detected, running all unit tests", file=sys.stderr)
            return 1
        changed_files = get_changed_files(args.base_branch)
    if not changed_files:
        print("No changed files detected, running all unit tests", file=sys.stderr)
        return 1

    print("Changed files:", file=sys.stderr)
    for f in changed_files:
        print(f"  {f}", file=sys.stderr)

    affected_targets = []
    for filepath in changed_files:
        for cf in __CRITICAL_FILES:
            if cf in filepath:
                print(
                    f"Critical file {filepath} affected (pattern: {cf}); running all tests"
                )
                return 1

        target = query_bazel_target(args.bazel, filepath)
        if target:
            affected_targets.extend(target.splitlines())

    if not affected_targets:
        print(
            "No bazel targets found for changed files, running all tests",
            file=sys.stderr,
        )
        return 1

    targets_expr = " + ".join(affected_targets)
    print("Querying unit tests for affected targets...", file=sys.stderr)
    test_targets = query_affected_tests(args.bazel, args.tag, targets_expr)

    if not test_targets:
        print(
            "No unit tests found for affected targets, running all unit tests",
            file=sys.stderr,
        )
        return 1

    with open(args.output_file, "w") as f:
        f.write("\n".join(test_targets) + "\n")

    print(
        f"Written {len(test_targets)} test target(s) to {args.output_file}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
