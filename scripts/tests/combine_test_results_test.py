"""Unit tests for scripts/tests/combine_test_results."""

import unittest
import xml.etree.ElementTree as ElementTree

from scripts.tests.combine_test_results import BYPASS_PREFIX, mark_failures_skipped


def _parse(xml):
    return ElementTree.fromstring(xml)


class MarkFailuresSkippedTest(unittest.TestCase):
    def test_failures_and_errors_become_skipped(self):
        root = _parse(
            '<testsuites><testsuite name="S" tests="3" failures="1" errors="1">'
            '<testcase name="ok"/>'
            '<testcase name="failed"><failure message="boom">trace</failure></testcase>'
            '<testcase name="errored"><error message="init">trace2</error></testcase>'
            "</testsuite></testsuites>"
        )

        self.assertEqual(mark_failures_skipped(root), 2)

        self.assertEqual(root.findall(".//failure"), [])
        self.assertEqual(root.findall(".//error"), [])
        failed = root.find(".//testcase[@name='failed']/skipped")
        self.assertEqual(failed.get("message"), BYPASS_PREFIX + "boom")
        self.assertEqual(failed.text, "trace")
        self.assertIsNone(root.find(".//testcase[@name='ok']/skipped"))
        suite = root.find("testsuite")
        self.assertEqual(suite.get("failures"), "0")
        self.assertEqual(suite.get("errors"), "0")
        self.assertEqual(suite.get("skipped"), "2")

    def test_suite_level_error_without_test_cases_becomes_skipped_case(self):
        # Bazel writes this shape when a test target crashes before JUnit reports any case.
        root = _parse(
            '<testsuites><testsuite name="//pkg:target" tests="1" errors="1">'
            '<error message="exited with error code 1">log</error>'
            "</testsuite></testsuites>"
        )

        self.assertEqual(mark_failures_skipped(root), 1)

        self.assertEqual(root.findall(".//error"), [])
        skipped = root.find(".//testcase[@name='//pkg:target']/skipped")
        self.assertEqual(skipped.get("message"), BYPASS_PREFIX + "exited with error code 1")

    def test_passing_suite_is_unchanged(self):
        root = _parse(
            '<testsuites><testsuite name="S" tests="1" failures="0" skipped="1">'
            '<testcase name="ok"/><testcase name="skip"><skipped/></testcase>'
            "</testsuite></testsuites>"
        )

        self.assertEqual(mark_failures_skipped(root), 0)
        self.assertEqual(root.find("testsuite").get("skipped"), "1")


if __name__ == "__main__":
    unittest.main()
