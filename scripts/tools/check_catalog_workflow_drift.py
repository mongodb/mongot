#!/usr/bin/env python3
"""
Verify that shared tasks in catalog-info.yaml stay synchronized between the
NonPatchRelease and PatchRelease WorkflowTypes.

Sync is on by default: unless a task is listed in TASK_SYNC_POLICY, it must
exist in both workflows with identical _COMPARED_FIELDS. This keeps drift
protection from silently degrading — a newly added shared task is checked
automatically, without anyone having to remember to opt it in.

TASK_SYNC_POLICY is a per-task override list. A task is listed only to relax
the default, either fully (allowed to diverge) or partially (guard just the
structural/execution fields while its free-text Notes/Description differ per
workflow).
"""

import sys
from pathlib import Path

import yaml

# The fields a fully-synced task must match across the two workflows.
_COMPARED_FIELDS = (
    "Activity",
    "Notes",
    "DisplayName",
    "Description",
    "ExecutionMode",
    "Type",
    "Data",
)

# Free-text fields that are written per workflow (patch vs. non-patch give
# different human instructions) and are therefore allowed to differ for tasks
# opted into partial sync. Everything else — structure and execution semantics
# must still match.
_SYNC_EXCEPT_DESCRIPTIVE = set(_COMPARED_FIELDS) - {"Notes", "Description"}

# Same as partial sync, but the collected Data payload is also intentionally
# per-workflow. Structural/execution fields still must match.
_SYNC_EXCEPT_DESCRIPTIVE_AND_DATA = _SYNC_EXCEPT_DESCRIPTIVE - {"Data"}

# Per-task overrides on the default "must match on every field, in both
# workflows" policy. A task NOT listed here is fully synced. Map a task to:
#   * an empty set              -> allowed to diverge entirely: present in only
#                                  one workflow, or intentionally different.
#   * _SYNC_EXCEPT_DESCRIPTIVE  -> partial sync: Notes/Description may differ per
#                                  workflow, but all structural/execution fields
#                                  must match.
# Every entry should have a short comment explaining why it deviates.
TASK_SYNC_POLICY = {
    # --- Allowed to diverge entirely (empty set) ---
    # NonPatchRelease-only: full-release preparation and tagging.
    "RELEASE_PREPARATION": set(),
    "BRANCH_AND_RELEASE_INIT": set(),
    "BRANCHING": set(),
    "SEND_BRANCH_CUT_SLACK": set(),
    "CREATE_TAG": set(),
    "JIRA_BULK_UPDATE": set(),
    # PatchRelease-only: patch preparation and cherry-pick flow.
    "PATCH_RELEASE_PREPARATION": set(),
    "PATCH_PREPARATION_AND_CHERRY_PICK": set(),
    "PATCH_CHERRY_PICK_TO_RELEASE_BRANCH": set(),
    "ENSURE_CHANGES_MERGED_TO_MASTER": set(),
    "ANNOUNCE_PATCH_INITIATION": set(),
    "CREATE_PATCH_TAG": set(),
    "UPDATE_BACKPORTED_TICKETS_FIXVERSION": set(),
    # Present in both workflows, but the collected inputs differ: PatchRelease
    # also gathers release_major_minor and gives release_sha branch-specific
    # guidance, so Data/Description/Notes are intentionally workflow-specific.
    # Structure and execution semantics must still stay aligned.
    "SET_RELEASE_INPUTS": _SYNC_EXCEPT_DESCRIPTIVE_AND_DATA,
    # Partial sync: same task in both workflows with per-workflow Notes/
    # Description instructions, but structure and execution must stay
    # aligned.
    "ANNOUNCE_PRE_PROD_DEPLOYMENTS": _SYNC_EXCEPT_DESCRIPTIVE,
    "CLOUD_DEV_RELEASE": _SYNC_EXCEPT_DESCRIPTIVE,
    "CREATE_JIRA_RELEASE": _SYNC_EXCEPT_DESCRIPTIVE,
    "EVERGREEN_WATERFALL_CHECKS": _SYNC_EXCEPT_DESCRIPTIVE,
    "NON_PROD_ENV_PVM_ROLLOUT": _SYNC_EXCEPT_DESCRIPTIVE,
    "RELEASE_TO_10_PERCENT_PROD": _SYNC_EXCEPT_DESCRIPTIVE,
    "RELEASE_TO_50_PERCENT_PROD": _SYNC_EXCEPT_DESCRIPTIVE,
    "RELEASE_TO_100_PERCENT_PROD": _SYNC_EXCEPT_DESCRIPTIVE,
    "VERIFY_ARTIFACTS": _SYNC_EXCEPT_DESCRIPTIVE,
}


def _fields_to_compare_for_task(task_name):
    """Fields that must match for a task; empty set means "do not compare".

    Unlisted tasks default to full sync (every field in _COMPARED_FIELDS).
    """
    return TASK_SYNC_POLICY.get(task_name, set(_COMPARED_FIELDS))


def _tasks_by_name(task_list):
    return {task["Name"]: task for task in task_list}


def check_drift(catalog_path):
    with open(catalog_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    workflow_types = data.get("spec", {}).get("releaseConfig", {}).get("WorkflowTypes", [])
    by_workflow = {wf["Name"]: _tasks_by_name(wf.get("Tasks", [])) for wf in workflow_types}

    non_patch = by_workflow.get("NonPatchRelease", {})
    patch = by_workflow.get("PatchRelease", {})

    drifted = []
    missing = []
    for task_name in sorted(set(non_patch) | set(patch)):
        fields = _fields_to_compare_for_task(task_name)
        if not fields:
            continue  # allowed to diverge entirely
        np_task = non_patch.get(task_name)
        p_task = patch.get(task_name)
        if np_task is None:
            missing.append((task_name, "NonPatchRelease"))
            continue
        if p_task is None:
            missing.append((task_name, "PatchRelease"))
            continue
        # Iterate _COMPARED_FIELDS (not the set) for stable, ordered output.
        for field in _COMPARED_FIELDS:
            if field in fields and np_task.get(field) != p_task.get(field):
                drifted.append((task_name, field))

    if missing or drifted:
        print(
            "ERROR: catalog-info.yaml — NonPatchRelease and PatchRelease WorkflowTypes "
            "have diverged:",
            file=sys.stderr,
        )
        for task_name, workflow in missing:
            print(f"  Task '{task_name}': missing from {workflow}", file=sys.stderr)
        for task_name, field in drifted:
            print(f"  Task '{task_name}': field '{field}' differs", file=sys.stderr)
        print(file=sys.stderr)
        print(
            "Update both WorkflowTypes together so they stay in sync.\n"
            "If the divergence is intentional, relax the task's entry in\n"
            "TASK_SYNC_POLICY in scripts/tools/check_catalog_workflow_drift.py\n"
            "(empty set to allow full divergence, or _SYNC_EXCEPT_DESCRIPTIVE to\n"
            "allow only Notes/Description to differ) with a comment explaining why.",
            file=sys.stderr,
        )
        return False

    return True


if __name__ == "__main__":
    # Accept an explicit path so the Bazel test can pass $(location //:catalog-info.yaml)
    # from the sandboxed runfiles tree; fall back to cwd for manual local runs.
    catalog = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("catalog-info.yaml")
    if not catalog.exists():
        sys.exit(0)
    sys.exit(0 if check_drift(catalog) else 1)
