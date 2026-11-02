#!/usr/bin/env python3
"""Determines which unit test targets are affected by changed files in the current PR
and writes them to test_filter.targets.

Exit codes:
  0 - --output-file written; caller should run:
        bazel test --test_output=errors --target_pattern_file=<output-file>
  1 - caller should run all unit tests. Every change that cannot be narrowed
      safely lands here: a non-narrowing requester, detached HEAD, no changed
      files, a deleted or renamed file, a __CRITICAL_FILES match, no affected
      targets, an affected target outside the query universe, or no tests
      covering the affected targets.
"""

import argparse
import os
import subprocess
import sys

# A change to any of these can redirect the build graph in ways a per-file
# `bazel query` cannot see, so they force the full suite. The Bazel entries earn
# their place for different reasons: `bazel query <path>` on a BUILD file returns
# the BUILD target itself, which nothing depends on, and on a .bzl file it errors
# outright. Either way the change contributes no reverse dependencies, so a PR
# touching one of them alongside a source file would select only the source
# file's tests.
__CRITICAL_FILES = [
    ".bazelrc",
    ".evergreen.yml",
    "conf/evergreen",
    "BUILD",
    ".bzl",
    "MODULE.bazel",
]

# Evergreen requesters that may narrow. An allow-list rather than excluding the merge queue, so
# that an unrecognised requester runs everything. The merge queue (github_merge_request) is left
# out deliberately: it is the last gate before master and the only place the combination of
# queued PRs is tested.
__NARROWING_REQUESTERS = ["github_pull_request", "patch_request"]

# rdeps needs a universe and it cannot be //..., because unlike tests(//...),
# which loads fine, rdeps must load the universe's full transitive closure. That
# fails two ways: the rules_oci image targets need registry credentials to fetch
# their manifests, and //docker:evergreen_mongo_artifact selects on a gitignored
# package that only exists after a download script runs (CLOUDP-456063).
# //src/... + //scripts/... was tried and hits the rules_oci failure too.
# Targets outside this universe are handled by running everything.
__QUERY_UNIVERSE = "//src/..."
__QUERY_UNIVERSE_PREFIX = "//src/"


def run(cmd):
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def is_detached_head():
    rc, _, _ = run(["git", "symbolic-ref", "--quiet", "HEAD"])
    return rc != 0


def get_changed_files(base_branch):
    """Return (changed_files, run_all_reason). A reason means select nothing and run all."""
    rc, merge_base, err = run(["git", "merge-base", "HEAD", base_branch])
    if rc != 0:
        print(f"Could not determine merge base: {err}", file=sys.stderr)
        return [], "could not determine merge base"
    rc, stdout, _ = run(["git", "diff", "--name-status", merge_base])
    if rc != 0:
        return [], "git diff failed"

    # Neither of these survives the per-file `bazel query` below: a deleted path
    # has no target left to query, and --name-status reports a rename as the
    # destination only, so the source path's reverse dependencies are invisible.
    # Both would be dropped in silence, so hand the whole run back to the caller.
    files = []
    for line in stdout.splitlines():
        if not line:
            continue
        fields = line.split("\t")
        status, paths = fields[0], fields[1:]
        if status.startswith("D"):
            return [], f"{paths[0]} was deleted"
        if status.startswith("R"):
            return [], f"{paths[0]} was renamed to {paths[-1]}"
        files.extend(paths)
    return files, None


def query_bazel_target(bazel, filepath):
    rc, stdout, _ = run([bazel, "query", filepath])
    if rc != 0:
        return None
    return stdout or None


def tag_regex(tag):
    """Regex matching `tag` in Bazel's stringified rendering of a list attribute.

    Bazel renders tags as "[unit]" or "[no-java-checkstyle, unit]" before matching,
    so an anchored "^unit$" matches nothing at all. A bare "unit" over-matches,
    because "e2e_community" contains it. Bound the tag by Bazel's own delimiters.
    """
    return rf"[\[ ]{tag}[,\]]"


def narrowing_is_allowed(requester):
    """Whether this requester may run a narrowed suite.

    Unset outside Evergreen is local development, which may narrow. Unset inside
    Evergreen is a misconfiguration (a new function forgot to set
    EVERGREEN_REQUESTER, a bad expansion) and must fall through to the full
    suite rather than silently narrow in CI.
    """
    if requester in __NARROWING_REQUESTERS:
        return True
    return not requester and not os.environ.get("EVERGREEN_TASK_ID")


def targets_outside_universe(targets):
    """Targets rdeps cannot reason about, because they fall outside __QUERY_UNIVERSE."""
    return [t for t in targets if not t.startswith(__QUERY_UNIVERSE_PREFIX)]


def query_affected_tests(bazel, tag, affected_targets_expr):
    query = (
        f'attr(tags, "{tag_regex(tag)}", tests(rdeps({__QUERY_UNIVERSE}, {affected_targets_expr})))'
    )
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

    requester = os.environ.get("EVERGREEN_REQUESTER", "")
    if not narrowing_is_allowed(requester):
        print(f"Requester is {requester}, running all unit tests", file=sys.stderr)
        return 1

    if args.changed_files:
        changed_files = [f.strip() for f in args.changed_files.split(",") if f.strip()]
    else:
        if is_detached_head():
            print("Detached HEAD detected, running all unit tests", file=sys.stderr)
            return 1
        changed_files, run_all_reason = get_changed_files(args.base_branch)
        if run_all_reason:
            print(f"Running all unit tests: {run_all_reason}", file=sys.stderr)
            return 1
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
                print(f"Critical file {filepath} affected (pattern: {cf}); running all tests")
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

    outside = targets_outside_universe(affected_targets)
    if outside:
        print(
            f"Affected targets outside {__QUERY_UNIVERSE} ({', '.join(outside)}), "
            "running all tests",
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
