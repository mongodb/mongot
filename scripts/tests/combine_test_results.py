#!/usr/bin/env python

import argparse
import os
import sys
import xml.etree.ElementTree as ElementTree

BYPASS_PREFIX = "BYPASSED (Atlas E2E bypass applied): "


def find_test_xml(path):
    contents = os.listdir(path)
    results = []
    for item in contents:
        item_path = os.path.join(path, item)
        if item == "test.xml":
            results.append(item_path)
        elif os.path.isdir(item_path):
            results = results + find_test_xml(item_path)
    return results


def _to_skipped(element, outcome):
    message = BYPASS_PREFIX + (outcome.get("message") or outcome.tag)
    skipped = ElementTree.SubElement(element, "skipped", {"message": message})
    skipped.text = outcome.text


def mark_failures_skipped(combined_xml):
    """Rewrites every failed test as skipped so Evergreen does not fail the task.

    Evergreen fails a task when any attached test case has a <failure> or <error> child, which
    would undo an applied E2E bypass. Skipped results keep the failures visible in the Tests tab.
    Returns the number of rewritten results.
    """
    rewritten = 0
    for suite in list(combined_xml.iter("testsuite")):
        suite_rewritten = 0
        test_cases = suite.findall("testcase")
        for test_case in test_cases:
            outcomes = [c for c in test_case if c.tag in ("failure", "error")]
            for outcome in outcomes:
                test_case.remove(outcome)
            if outcomes:
                _to_skipped(test_case, outcomes[0])
                suite_rewritten += 1
        # A suite-level <error> with no test cases is reported by Evergreen as a failed test.
        for suite_error in suite.findall("error"):
            suite.remove(suite_error)
            if not test_cases:
                test_case = ElementTree.SubElement(
                    suite, "testcase", {"name": suite.get("name", "")}
                )
                _to_skipped(test_case, suite_error)
                suite_rewritten += 1
        if suite_rewritten:
            skipped = int(suite.get("skipped") or 0) + suite_rewritten
            suite.set("failures", "0")
            suite.set("errors", "0")
            suite.set("skipped", str(skipped))
        rewritten += suite_rewritten
    return rewritten


def combine_test_xml(results, bypass_marker=None):
    if len(results) == 0:
        print("No test results", file=sys.stderr)
        exit(1)

    combined_xml = ElementTree.Element("testsuites")

    for test_xml_file in results:
        test_xml = ElementTree.parse(test_xml_file)
        for suite in test_xml.iter("testsuite"):
            combined_xml.append(suite)

    if bypass_marker and os.path.exists(bypass_marker):
        rewritten = mark_failures_skipped(combined_xml)
        print(
            f"Bypass marker {bypass_marker} found: reported {rewritten} failed tests as skipped.",
            file=sys.stderr,
        )

    combined_tree = ElementTree.ElementTree(combined_xml)
    combined_tree.write(sys.stdout, encoding="unicode")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bypass-marker",
        help="If this file exists, failed tests are reported as skipped (see "
        "scripts/evergreen/test/run-e2e-with-bypass-check.sh).",
    )
    args = parser.parse_args()
    combine_test_xml(find_test_xml("bazel-testlogs"), args.bypass_marker)
