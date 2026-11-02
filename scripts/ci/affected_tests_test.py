#!/usr/bin/env python3
"""Tests for affected_tests.py.

Covers the two ways this script can silently narrow a test run too far: a tag
pattern that does not match what Bazel actually feeds the regex, and a changed
file whose reverse dependencies never reach the query.
"""

import os
import re
import subprocess
import tempfile
import unittest
from unittest import mock

from scripts.ci.affected_tests import (
    get_changed_files,
    narrowing_is_allowed,
    tag_regex,
    targets_outside_universe,
)

# How Bazel renders a tags attribute before matching a regex against it. Taken
# from real targets in this repo rather than invented.
RENDERED_UNIT = "[unit]"
RENDERED_UNIT_WITH_OTHERS = "[no-java-checkstyle, no-java-spotbugs, unit]"
RENDERED_E2E_COMMUNITY = "[e2e_community]"
RENDERED_FUZZ = "[fuzz]"


def git(*args, cwd):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


class TagRegexTest(unittest.TestCase):
    def test_matches_a_lone_tag(self):
        self.assertRegex(RENDERED_UNIT, tag_regex("unit"))

    def test_matches_a_tag_among_others(self):
        self.assertRegex(RENDERED_UNIT_WITH_OTHERS, tag_regex("unit"))

    def test_does_not_match_a_tag_that_merely_contains_it(self):
        # The reason a bare "unit" is not good enough: this would pull every
        # e2e_community target into the unit run.
        self.assertNotRegex(RENDERED_E2E_COMMUNITY, tag_regex("unit"))

    def test_does_not_match_an_unrelated_tag(self):
        self.assertNotRegex(RENDERED_FUZZ, tag_regex("unit"))

    def test_anchored_pattern_would_match_nothing(self):
        # The original bug, kept as a regression guard: Bazel never sees a bare
        # "unit", so anchoring to the whole string can never match.
        for rendered in (RENDERED_UNIT, RENDERED_UNIT_WITH_OTHERS):
            self.assertIsNone(re.search("^unit$", rendered))


class TargetsOutsideUniverseTest(unittest.TestCase):
    def test_targets_in_the_universe_are_not_flagged(self):
        self.assertEqual([], targets_outside_universe(["//src/main/java/a:b", "//src/test:c"]))

    def test_targets_outside_the_universe_are_flagged(self):
        # rdeps returns empty with exit 0 for these rather than failing, so
        # without this check they would be silently dropped.
        self.assertEqual(
            ["//deploy/atlas:manifest_test", "//scripts/release:x"],
            targets_outside_universe(
                ["//src/main/java/a:b", "//deploy/atlas:manifest_test", "//scripts/release:x"]
            ),
        )

    def test_a_prefix_lookalike_is_flagged(self):
        self.assertEqual(["//srcfoo:bar"], targets_outside_universe(["//srcfoo:bar"]))


class GetChangedFilesTest(unittest.TestCase):
    def setUp(self):
        self.repo = tempfile.mkdtemp()
        self.addCleanup(os.chdir, os.getcwd())
        git("init", "-q", cwd=self.repo)
        git("config", "user.email", "t@t", cwd=self.repo)
        git("config", "user.name", "t", cwd=self.repo)
        os.makedirs(os.path.join(self.repo, "pkg"))
        for name in ("kept.java", "moved.java", "removed.java"):
            with open(os.path.join(self.repo, "pkg", name), "w") as f:
                f.write("original\n")
        git("add", "-A", cwd=self.repo)
        git("commit", "-q", "-m", "init", cwd=self.repo)
        git("branch", "-q", "base", cwd=self.repo)
        os.chdir(self.repo)

    def test_modified_files_are_returned(self):
        with open("pkg/kept.java", "w") as f:
            f.write("changed\n")
        git("commit", "-q", "-am", "modify", cwd=self.repo)
        files, reason = get_changed_files("base")
        self.assertIsNone(reason)
        self.assertEqual(["pkg/kept.java"], files)

    def test_a_deleted_file_runs_everything(self):
        # bazel query cannot resolve a path that no longer exists, so the
        # deleted file's dependents would be dropped without a word.
        git("rm", "-q", "pkg/removed.java", cwd=self.repo)
        git("commit", "-q", "-m", "delete", cwd=self.repo)
        files, reason = get_changed_files("base")
        self.assertEqual([], files)
        self.assertIn("removed.java", reason)

    def test_a_renamed_file_runs_everything(self):
        # git reports a rename as the destination path only, so the source
        # path's dependents are invisible to a per-file query.
        git("mv", "pkg/moved.java", "pkg/renamed.java", cwd=self.repo)
        git("commit", "-q", "-m", "rename", cwd=self.repo)
        files, reason = get_changed_files("base")
        self.assertEqual([], files)
        self.assertIn("moved.java", reason)

    def test_an_unknown_base_branch_runs_everything(self):
        files, reason = get_changed_files("no-such-branch")
        self.assertEqual([], files)
        self.assertIn("merge base", reason)


class NarrowingIsAllowedTest(unittest.TestCase):
    # Requester strings read off the Evergreen API, not invented: a wrong one here would
    # silently re-enable narrowing in the merge queue.
    def test_pull_requests_narrow(self):
        self.assertTrue(narrowing_is_allowed("github_pull_request"))

    def test_patches_narrow(self):
        self.assertTrue(narrowing_is_allowed("patch_request"))

    def test_local_development_narrows(self):
        # Local = no Evergreen env vars set.
        env = {k: v for k, v in os.environ.items() if k != "EVERGREEN_TASK_ID"}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertTrue(narrowing_is_allowed(""))

    def test_the_merge_queue_runs_everything(self):
        self.assertFalse(narrowing_is_allowed("github_merge_request"))

    def test_an_unrecognised_requester_runs_everything(self):
        # Fail safe: a requester Evergreen adds later should not silently narrow.
        self.assertFalse(narrowing_is_allowed("some_future_requester"))

    def test_empty_requester_in_evergreen_does_not_narrow(self):
        # Guards against a future Evergreen function forgetting to set
        # EVERGREEN_REQUESTER: ${requester}. EVERGREEN_TASK_ID is always set
        # inside Evergreen, so its presence is the "in CI" signal.
        with mock.patch.dict(os.environ, {"EVERGREEN_TASK_ID": "task_abc"}):
            self.assertFalse(narrowing_is_allowed(""))


if __name__ == "__main__":
    unittest.main()
