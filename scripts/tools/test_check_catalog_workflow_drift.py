"""Unit tests for check_catalog_workflow_drift.py (default-sync + task overrides)."""

import tempfile
import unittest
from pathlib import Path

import yaml

from scripts.tools import check_catalog_workflow_drift as drift_check

# A task deliberately NOT in TASK_SYNC_POLICY -> fully synced by default.
_DEFAULT_NAME = "TEST_DEFAULT_TASK"
# Any task the policy exempts entirely (empty field set).
_EXEMPT_NAME = next(n for n, f in drift_check.TASK_SYNC_POLICY.items() if not f)
# Partially-synced policy tasks (only ExecutionMode/Activity are guarded).
_PARTIAL_SYNC_NAME = "VERIFY_ARTIFACTS"
_PARTIAL_SYNC_NAME_2 = "EVERGREEN_WATERFALL_CHECKS"


def _catalog(non_patch_tasks, patch_tasks):
    return {
        "spec": {
            "releaseConfig": {
                "WorkflowTypes": [
                    {"Name": "NonPatchRelease", "Tasks": non_patch_tasks},
                    {"Name": "PatchRelease", "Tasks": patch_tasks},
                ]
            }
        }
    }


def _task(name, **fields):
    task = {"Name": name}
    task.update(fields)
    return task


class CheckDriftTest(unittest.TestCase):
    def setUp(self):
        # Per-test temp dir, removed automatically when the test finishes so
        # catalog YAML fixtures are not left behind locally or in CI.
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)

    def _write(self, catalog):
        path = Path(self._tmpdir.name) / "catalog.yaml"
        with open(path, "w", encoding="utf-8") as f:
            yaml.safe_dump(catalog, f)
        return path

    def _check(self, non_patch_tasks, patch_tasks):
        return drift_check.check_drift(self._write(_catalog(non_patch_tasks, patch_tasks)))

    # --- policy invariants ---------------------------------------------------

    def test_policy_shape(self):
        # Unlisted tasks default to full sync (every field must match).
        self.assertEqual(
            drift_check._fields_to_compare_for_task(_DEFAULT_NAME),
            set(drift_check._COMPARED_FIELDS),
        )
        # Exempt tasks compare nothing; partial tasks guard everything except
        # the free-text Notes/Description fields.
        self.assertEqual(drift_check._fields_to_compare_for_task(_EXEMPT_NAME), set())
        self.assertEqual(
            drift_check.TASK_SYNC_POLICY[_PARTIAL_SYNC_NAME],
            set(drift_check._COMPARED_FIELDS) - {"Notes", "Description"},
        )

    # --- default (unlisted) task: fully synced, protected automatically ------

    def test_default_task_in_sync_passes(self):
        task = _task(
            _DEFAULT_NAME,
            Activity="A",
            Notes="N",
            DisplayName="D",
            Description="Desc",
            ExecutionMode="manual",
            Type="T",
            Data="X",
        )
        self.assertTrue(self._check([task], [dict(task)]))

    def test_default_task_drift_in_any_field_fails(self):
        # A new/unlisted shared task is guarded on ALL fields by default.
        base = dict(
            Activity="A",
            Notes="N",
            DisplayName="D",
            Description="Desc",
            ExecutionMode="manual",
            Type="T",
            Data="X",
        )
        for field in drift_check._COMPARED_FIELDS:
            with self.subTest(field=field):
                np_task = _task(_DEFAULT_NAME, **base)
                p_fields = dict(base)
                p_fields[field] = "DRIFTED"
                p_task = _task(_DEFAULT_NAME, **p_fields)
                self.assertFalse(self._check([np_task], [p_task]))

    def test_default_task_missing_from_one_workflow_fails(self):
        task = _task(_DEFAULT_NAME, ExecutionMode="manual", Activity="A")
        self.assertFalse(self._check([task], []))

    # --- exempt task: allowed to diverge entirely ----------------------------

    def test_exempt_task_drift_is_allowed(self):
        np_task = _task(
            _EXEMPT_NAME, ExecutionMode="manual", Activity="A", Notes="a", Description="a"
        )
        p_task = _task(
            _EXEMPT_NAME, ExecutionMode="automatic", Activity="B", Notes="b", Description="b"
        )
        self.assertTrue(self._check([np_task], [p_task]))

    def test_exempt_task_missing_from_one_workflow_is_allowed(self):
        task = _task(_EXEMPT_NAME, ExecutionMode="manual", Activity="A")
        self.assertTrue(self._check([task], []))

    # --- partial-sync task ---------------------------------------------------

    def test_partial_sync_descriptive_drift_passes(self):
        # Notes AND Description may differ; the guarded fields match -> allowed.
        np_task = _task(
            _PARTIAL_SYNC_NAME,
            ExecutionMode="manual",
            Activity="A",
            DisplayName="D",
            Notes="non-patch notes",
            Description="np",
        )
        p_task = _task(
            _PARTIAL_SYNC_NAME,
            ExecutionMode="manual",
            Activity="A",
            DisplayName="D",
            Notes="patch notes",
            Description="p",
        )
        self.assertTrue(self._check([np_task], [p_task]))

    def test_partial_sync_structural_drift_fails(self):
        # A non-descriptive field (e.g. DisplayName) is still guarded.
        np_task = _task(
            _PARTIAL_SYNC_NAME, ExecutionMode="manual", Activity="A", DisplayName="Non-patch label"
        )
        p_task = _task(
            _PARTIAL_SYNC_NAME, ExecutionMode="manual", Activity="A", DisplayName="Patch label"
        )
        self.assertFalse(self._check([np_task], [p_task]))

    def test_partial_sync_execution_mode_drift_fails(self):
        np_task = _task(_PARTIAL_SYNC_NAME, ExecutionMode="manual", Activity="A")
        p_task = _task(_PARTIAL_SYNC_NAME, ExecutionMode="automatic", Activity="A")
        self.assertFalse(self._check([np_task], [p_task]))

    def test_partial_sync_activity_drift_fails(self):
        np_task = _task(_PARTIAL_SYNC_NAME_2, ExecutionMode="manual", Activity="X")
        p_task = _task(_PARTIAL_SYNC_NAME_2, ExecutionMode="manual", Activity="Y")
        self.assertFalse(self._check([np_task], [p_task]))

    def test_partial_sync_missing_from_one_workflow_fails(self):
        # A partial task is still a shared task we check -> must exist in both.
        task = _task(_PARTIAL_SYNC_NAME, ExecutionMode="manual", Activity="A")
        self.assertFalse(self._check([task], []))


if __name__ == "__main__":
    unittest.main()
