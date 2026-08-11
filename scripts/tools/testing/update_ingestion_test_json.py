"""
This script updates the expected golden files for TestBsonDocumentProcessorIntegration using the
'actual' output of a test run. This is intended to help mass update test data after making intentional
changes to indexing logic (e.g. bumping IFV). The output should still be manually inspected to verify its
correctness.

Sample run:

make test.integration.index

python3 scripts/tools/testing/update_ingestion_test_json.py $(bazel info bazel-testlogs)/src/test/integration/java/com/xgen/mongot/index/ingestion/TestBsonDocumentProcessorIntegration/test.log
"""

import json
import re
import sys
from collections import OrderedDict

# Regex detects start of test failure. e.g:
# 2) runTest[stored:partial_inclusion:formatVersion-6:featureVersion-3](com.xgen.mongot.index.ingestion.TestBsonDocumentProcessorIntegration)
startRegex = re.compile(r"\d+\) runTest\[(.*?)[_:](.+)[_:](FormatVersion|formatVersion)-\d+.*")


def main():
    test_output_file, golden_file_directory = (
        sys.argv[1],
        sys.argv[2] if len(sys.argv) > 2 else None,
    )

    with open(test_output_file, "r") as file:
        try:
            line = next(file)

            # Start of errors is denoted by line: "There was|were \d+ error(s)?:"
            while not line.startswith("There w") and not line.endswith(":"):
                # Infer golden file location if not specified
                if golden_file_directory is None and line.startswith(
                    "[update_json_tests_explain.py]"
                ):
                    _, _, golden_file_directory = line.rpartition(" ")
                    golden_file_directory = golden_file_directory.strip()
                line = next(file)

            if golden_file_directory is None:
                print("Golden file location not found in test output or command line arguments")
                return
            else:
                print("Updating golden files at location:", golden_file_directory)

            # Start parsing test failures. Expected format:
            # 1) runTest[<file_name>:<test_case>:formatVersion-<IFV>]
            # BsonProcessorTestSpecException: <message>
            # expected:
            #
            # <multi-line json>
            #
            # actual:
            #
            # IndexFormatVersion: <IFV>
            #
            # <multi-line json>
            for line in file:
                if startRegex.match(line):
                    m = startRegex.search(line)
                    test_file = m.group(1) + ".json"
                    test_case = m.group(2)
                    error = next(file)
                    if "BsonProcessorTestSpecException" not in error:
                        print("Test case ", test_case, " contains unsupported error type:", error)
                        continue
                    while not next(file).startswith("actual:"):
                        pass
                    ignored_ifv = next(file), next(file)

                    # Read one pretty-printed json object
                    actual = ""
                    for json in file:
                        actual += json
                        if json.startswith("}"):
                            break

                    update_test_case(golden_file_directory, test_file, test_case, actual)
                else:
                    pass
        except StopIteration:
            print("Done")


def update_test_case(directory, test_file, test_case, actual):
    absolute_file = directory + "/" + test_file
    done = False
    print("Updating", absolute_file, " -- ", test_case)

    try:
        with open(absolute_file, "r") as file:
            # Load the JSON data into a Python dictionary
            data = json.load(file)
            for case in data["tests"]:
                if case["name"] == test_case:
                    if done:
                        print("Duplicate test name", test_case, " please rename or remove.")
                        break

                    # Serialize lucene fields in alphabetical order
                    updated = OrderedDict(
                        sorted(json.loads(actual, object_pairs_hook=OrderedDict).items())
                    )
                    case["luceneDocuments"] = updated["luceneDocuments"]
                    done = True
        if not done:
            print("ERROR: could not find test case in file")
            return
    except:
        print("ERROR: could not parse ", actual)
        sys.exit(1)

    with open(absolute_file, "w") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
        file.write("\n")


if __name__ == "__main__":
    main()
