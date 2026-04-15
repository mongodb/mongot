"""
This script updates the expected explain info in integration and e2e tests to match the 'actual'
output of a test run. This is intended to help mass update test data after making intentional
changes to a query implementation. The output should still be manually inspected to verify its
correctness. This script will terminate if a test failure outside explain is encountered (e.g.
it will not update returned doc IDs)

Note on duplicate test cases with runFor restrictions:
  Some test cases (mostly for TestQueryIntegration and TestQueryIntegrationV3) are duplicated in
  the golden files to support different explain output across index feature versions.
  For example, a date query test may have two entries with the same name:
    - one with "runFor": { "indexFeatureVersion": { "from": 4 } }  (V4 explain)
    - one with "runFor": { "indexFeatureVersion": { "to": 3 } }    (V3 explain)
  This script matches by name and updates the FIRST match it finds. To ensure the correct copy
  is updated, the "from: 4" copy must be listed FIRST in the JSON file. That way, running this
  script against V4 test logs updates the V4 copy, and V3 test logs won't contain failures for
  those tests.

Sample run:

make test.e2e

python3 scripts/tools/testing/update_json_tests_explain.py $(bazel info bazel-testlogs)/src/test/integration/java/com/xgen/mongot/index/TestQueryIntegration/test.log
"""

import json
import re
import sys
from collections import OrderedDict

# Example structure of test failure
# 167) runTest[equals_long-dynamic-array_FormatVersion-6_FeatureVersion-3](com.xgen.mongot.index.TestQueryIntegration)
# java.lang.AssertionError: long-dynamic-array: explain response expected:
# {"query": {"type": "BooleanQuery", "args": {"must": [], "mustNot": [], "should": [{"type": "IndexOrDocValuesQuery", "args": {"query": [{"type": "PointRangeQuery", "args": {"path": "a", "representation": "int64", "gte": 3, "lte": 3}}, {"type": "SortedNumericDocValuesRangeQuery", "args": {}}]}}, {"type": "PointRangeQuery", "args": {"path": "a", "representation": "int64", "gte": 3, "lte": 3}}, {"type": "PointRangeQuery", "args": {"path": "a", "representation": "double", "gte": 3.0, "lte": 3.0}}, {"type": "IndexOrDocValuesQuery", "args": {"query": [{"type": "PointRangeQuery", "args": {"path": "a", "representation": "double", "gte": 3.0, "lte": 3.0}}, {"type": "SortedNumericDocValuesRangeQuery", "args": {}}]}}], "filter": [], "minimumShouldMatch": 0}}}
# actual:
# {"query": {"type": "ConstantScoreQuery", "args": {"query": {"type": "BooleanQuery", "args": {"must": [], "mustNot": [], "should": [{"type": "IndexOrDocValuesQuery", "args": {"query": [{"type": "PointRangeQuery", "args": {"path": "a", "representation": "double", "gte": 3.0, "lte": 3.0}}, {"type": "SortedNumericDocValuesRangeQuery", "args": {}}]}}, {"type": "PointRangeQuery", "args": {"path": "a", "representation": "int64", "gte": 3, "lte": 3}}, {"type": "IndexOrDocValuesQuery", "args": {"query": [{"type": "PointRangeQuery", "args": {"path": "a", "representation": "int64", "gte": 3, "lte": 3}}, {"type": "SortedNumericDocValuesRangeQuery", "args": {}}]}}, {"type": "PointRangeQuery", "args": {"path": "a", "representation": "double", "gte": 3.0, "lte": 3.0}}], "filter": [], "minimumShouldMatch": 0}}}}, "metadata": {"totalLuceneDocs": 2}}


startRegex = re.compile(r"\d+\) runTest\[(.*?)_(.+)_FormatVersion-\d+")


def main():
  test_output_file, golden_file_directory = sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None

  with open(test_output_file, 'r') as file:
    try:
      line = next(file)

      while not line.startswith("There w") and not line.endswith(":"):
        if line.startswith("[update_json_tests_explain.py]"):
          _, _, golden_file_directory = line.rpartition(' ')
          golden_file_directory = golden_file_directory.strip()
          print("Updating golden files at location:", golden_file_directory)
        line = next(file)

      if golden_file_directory is None:
        print("Golden file location not found in test output or command line arguments")
        return

      for line in file:
        if startRegex.match(line):
          m = startRegex.search(line)
          test_file = m.group(1) + '.json'
          test_case = m.group(2)
          error, expected, actual_literal = next(file), next(file), next(file)
          if "explain" not in error:
            print("Test case ", test_case, " contains unsupported error type:", error, file=sys.stderr)
            continue
          actual = next(file)

          update_test_case(golden_file_directory, test_file, test_case, actual)
        else:
          pass
    except StopIteration:
      print("Done")


def update_test_case(directory, test_file, test_case, actual):
  absolute_file = directory + '/' + test_file
  done = False
  print("Updating ", absolute_file, " -- ", test_case)

  try:
    with open(absolute_file, 'r') as file:
      # Load the JSON data into a Python dictionary
      data = json.load(file)
      for case in data['tests']:
        # First try exact match
        if case["name"] == test_case:
          updated = json.loads(actual, object_pairs_hook=OrderedDict)
          updated.pop('metadata', None)  # Don't include metadata for testing
          case['result']["explain"] = updated
          done = True
          break
        # For tests with shardZoneConfigs, the test name includes the zone config name as a suffix.
        # Try matching by checking if the test_case starts with the case name and has a suffix.
        # e.g., test_case="numCandidates-higher-than-limit_docsOneShard" should match case name "numCandidates-higher-than-limit"
        if test_case.startswith(case["name"] + "_") and "shardZoneConfigs" in case:
          zone_config_suffix = test_case[len(case["name"]) + 1:]
          if zone_config_suffix in case["shardZoneConfigs"]:
            updated = json.loads(actual, object_pairs_hook=OrderedDict)
            updated.pop('metadata', None)  # Don't include metadata for testing
            case['result']["explain"] = updated
            done = True
            break
    if not done:
      print("ERROR: could not find test case in file")
      return
  except:
    print("ERROR: could not parse ", actual)
    sys.exit(1)

  with open(absolute_file, 'w') as file:
    json.dump(data, file, ensure_ascii=False, indent=2)
    file.write('\n')


if __name__ == "__main__":
  main()
