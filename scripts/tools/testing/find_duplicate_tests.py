"""
This file identifies duplicate search query and vector search e2e tests on a best-effort basis. This
is intended to find test cases that become duplicates after mass updating the minimum IFV, but
sometimes engineers inadvertently add duplicate test cases. You should be skeptical of the output:
always verify by diff'ing the test cases on the command line

How to run:
% python3 scripts/tools/testing/find_duplicate_tests.py
Found potential duplicate ('queryVector-empty-doc-in-eq-simple-form', 'queryVector-no-operator-in-simple-clause') in src/test/integration/resources/index/vector/vector-search.json
"""

import glob
import json
import os


def main():
    integration_tests = glob.glob("src/test/integration/resources/index/*.json")
    integration_tests.extend(glob.glob("src/test/integration/resources/index/vector/*.json"))

    for test in integration_tests:
        filename = os.path.basename(test)

        if filename.startswith("subpipeline") or "ingestion" in test:
            print("Skipping", test)
            continue

        fingerprints = {}

        with open(test, "r") as file:
            test_cases = json.load(file)

            for data in test_cases["tests"]:
                name = data["name"]
                fingerprint = (
                    try_search_fingerprint(data)
                    or try_vector_fingerprint(data)
                    or try_basedOnTestSpec(data)
                )
                if fingerprint is None:
                    raise KeyError("Failed to parse test case in", filename, data)

                if fingerprint in fingerprints:
                    print(
                        "Found potential duplicate", (name, fingerprints[fingerprint]), "in", test
                    )
                else:
                    fingerprints[fingerprint] = name


def try_vector_fingerprint(data):
    try:
        docs = data["documents"]
        query = data["filter"]
        index = data.get("index", {})
        mapping = data.get("mappings", {})
        return str(docs) + str(query) + str(index) + str(mapping)
    except KeyError:
        return None


def try_basedOnTestSpec(data):
    spec = data.get("basedOnTestSpec", None)
    return spec


def try_search_fingerprint(data):
    query = data.get("query", None)
    if query is None:
        return None

    explain = data.get("explain", {})
    index = data.get("index", {})
    analyzer = index.get("analyzer", "")
    partitions = index.get("numPartitions", 1)
    mapping = index.get("mappings", index.get("fields", []))
    docs = data.get("documents", [])
    syn = data.get("synonymDocuments", [])

    return (
        str(mapping) + str(docs) + str(query) + str(partitions) + str(explain) + str(syn) + analyzer
    )


if __name__ == "__main__":
    main()
