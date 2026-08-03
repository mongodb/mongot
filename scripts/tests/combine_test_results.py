#!/usr/bin/env python

import os

import sys
import xml.etree.ElementTree as ElementTree


def find_test_xml(path):
    contents = os.listdir(path)
    results = []
    for item in contents:
        item_path = os.path.join(path, item)
        if item == 'test.xml':
            results.append(item_path)
        elif os.path.isdir(item_path):
            results = results + find_test_xml(item_path)
    return results


def combine_test_xml(results):
    if len(results) == 0:
        print("No test results", file=sys.stderr)
        exit(1)

    combined_xml = ElementTree.Element('testsuites')

    for test_xml_file in results:
        test_xml = ElementTree.parse(test_xml_file)
        for suite in test_xml.iter("testsuite"):
            combined_xml.append(suite)

    combined_tree = ElementTree.ElementTree(combined_xml)
    combined_tree.write(sys.stdout, encoding='unicode')


combine_test_xml(find_test_xml("bazel-testlogs"))
