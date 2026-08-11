#!/usr/bin/env python3
"""
Tests for create_request_util.py module.

This test suite verifies the utility functions used by maintenance request scripts.
"""

import argparse
import json
import os
import sys
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add the maintenance directory to the path so we can import the module
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "tools" / "maintenance"))

import create_request_util


class TestAddMetadataArguments(unittest.TestCase):
    """Test suite for add_metadata_arguments function"""

    def test_adds_all_required_metadata_arguments(self):
        """Test that all metadata arguments are added to the parser"""
        parser = argparse.ArgumentParser()
        create_request_util.add_metadata_arguments(parser)

        # Parse with all metadata arguments
        args = parser.parse_args(
            [
                "--requested-at",
                "2026-02-05T10:00:00Z",
                "--requested-by",
                "test-user",
                "--reason",
                "Test reason",
            ]
        )

        self.assertEqual(args.requested_at, "2026-02-05T10:00:00Z")
        self.assertEqual(args.requested_by, "test-user")
        self.assertEqual(args.reason, "Test reason")

    def test_adds_optional_output_argument(self):
        """Test that output argument is added as optional"""
        parser = argparse.ArgumentParser()
        create_request_util.add_metadata_arguments(parser)

        # Parse with output argument
        args = parser.parse_args(
            [
                "--requested-at",
                "2026-02-05T10:00:00Z",
                "--requested-by",
                "test-user",
                "--reason",
                "Test reason",
                "--output",
                "/tmp/test.json",
            ]
        )

        self.assertEqual(args.output, Path("/tmp/test.json"))

    def test_metadata_arguments_are_required(self):
        """Test that each required metadata argument causes failure when missing"""
        # Define all required arguments
        all_required_args = {
            "--requested-at": "2026-02-05T10:00:00Z",
            "--requested-by": "test-user",
            "--reason": "Test reason",
        }

        # Test each required argument by omitting it
        for missing_arg in all_required_args.keys():
            with self.subTest(missing_argument=missing_arg):
                parser = argparse.ArgumentParser()
                create_request_util.add_metadata_arguments(parser)

                # Build args list with all arguments except the one we're testing
                test_args = []
                for arg_name, arg_value in all_required_args.items():
                    if arg_name != missing_arg:
                        test_args.extend([arg_name, arg_value])

                # Verify that the parser exits with an error
                with self.assertRaises(SystemExit) as cm:
                    parser.parse_args(test_args)

                # Verify it's a non-zero exit code (error)
                self.assertNotEqual(
                    cm.exception.code, 0, f"Parser should fail when {missing_arg} is missing"
                )


class TestAddMetadataToRequest(unittest.TestCase):
    """Test suite for add_metadata_to_request function"""

    def test_adds_metadata_to_request_json(self):
        """Test that metadata is correctly added to request JSON"""
        request_json = {"tool": "TEST_TOOL"}

        # Create mock args
        args = MagicMock()
        args.requested_at = "2026-02-05T10:00:00Z"
        args.requested_by = "test-user"
        args.reason = "Test reason"

        create_request_util.add_metadata_to_request(request_json, args)

        self.assertIn("metadata", request_json)
        self.assertEqual(request_json["metadata"]["requestedAt"], "2026-02-05T10:00:00Z")
        self.assertEqual(request_json["metadata"]["requestedBy"], "test-user")
        self.assertEqual(request_json["metadata"]["reason"], "Test reason")

    def test_preserves_existing_fields_in_request(self):
        """Test that existing fields in request JSON are preserved"""
        request_json = {"tool": "TEST_TOOL", "generationId": "test_gen_123", "otherField": "value"}

        args = MagicMock()
        args.requested_at = "2026-02-05T10:00:00Z"
        args.requested_by = "test-user"
        args.reason = "Test reason"

        create_request_util.add_metadata_to_request(request_json, args)

        # Original fields should still be present
        self.assertEqual(request_json["tool"], "TEST_TOOL")
        self.assertEqual(request_json["generationId"], "test_gen_123")
        self.assertEqual(request_json["otherField"], "value")
        # Metadata should be added
        self.assertIn("metadata", request_json)


class TestSetOutputFile(unittest.TestCase):
    """Test suite for set_output_file function"""

    def test_returns_provided_output_path(self):
        """Test that provided output path is returned as-is"""
        output_path = Path("/tmp/custom_output.json")
        result = create_request_util.set_output_file(output_path, "gen_123")

        self.assertEqual(result, output_path)

    def test_generates_default_filename_with_generation_id(self):
        """Test that default filename includes generation ID and is formatted correctly"""
        import re

        with patch("sys.stderr", new_callable=StringIO):
            result = create_request_util.set_output_file(None, "test_gen_456")

        # Should match pattern: request_test_gen_456_YYYYMMDD_HHMMSS.json
        filename_pattern = r"^request_test_gen_456_\d{8}_\d{6}\.json$"
        filename = result.name

        self.assertIsNotNone(
            re.match(filename_pattern, filename),
            f"Filename '{filename}' does not match expected format "
            f"'request_<generation-id>_YYYYMMDD_HHMMSS.json'",
        )

        # Verify the timestamp portion is valid
        # Extract timestamp: YYYYMMDD_HHMMSS
        timestamp_match = re.search(r"_(\d{8})_(\d{6})\.json$", filename)
        self.assertIsNotNone(timestamp_match, "Timestamp not found in filename")

    def test_uses_build_workspace_directory_when_available(self):
        """Test that BUILD_WORKSPACE_DIRECTORY is used when set"""
        with patch.dict("os.environ", {"BUILD_WORKSPACE_DIRECTORY": "/workspace/root"}):
            with patch("sys.stderr", new_callable=StringIO):
                result = create_request_util.set_output_file(None, "gen_789")

        # Result should start with the workspace directory
        self.assertTrue(str(result).startswith("/workspace/root"))

    def test_uses_current_directory_when_build_workspace_not_set(self):
        """Test that current directory is used when BUILD_WORKSPACE_DIRECTORY is not set"""
        # Ensure BUILD_WORKSPACE_DIRECTORY is not set
        env = os.environ.copy()
        env.pop("BUILD_WORKSPACE_DIRECTORY", None)

        with patch.dict("os.environ", env, clear=True):
            with patch("sys.stderr", new_callable=StringIO):
                result = create_request_util.set_output_file(None, "gen_abc")

        # Result should be a relative path (not starting with /workspace)
        self.assertFalse(str(result).startswith("/workspace"))


class TestWriteRequestFile(unittest.TestCase):
    """Test suite for write_request_file function"""

    def setUp(self):
        """Set up temporary directory for test files"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)

    def tearDown(self):
        """Clean up temporary directory"""
        self.temp_dir.cleanup()

    def test_writes_valid_json_file(self):
        """Test that a valid JSON file is written"""
        request_json = {
            "tool": "TEST_TOOL",
            "generationId": "test_gen",
            "metadata": {
                "requestedAt": "2026-02-05T10:00:00Z",
                "requestedBy": "test-user",
                "reason": "Test",
            },
        }
        output_file = self.temp_path / "test_output.json"

        with patch("sys.stdout", new_callable=StringIO):
            create_request_util.write_request_file(request_json, output_file)

        # Verify file was created
        self.assertTrue(output_file.exists())

        # Verify JSON content
        with open(output_file, "r") as f:
            loaded_json = json.load(f)

        self.assertEqual(loaded_json, request_json)

    def test_creates_parent_directories(self):
        """Test that parent directories are created if they don't exist"""
        request_json = {"tool": "TEST_TOOL"}
        output_file = self.temp_path / "nested" / "dir" / "output.json"

        # Verify parent directories don't exist yet
        self.assertFalse(output_file.parent.exists())

        with patch("sys.stdout", new_callable=StringIO):
            create_request_util.write_request_file(request_json, output_file)

        # Verify parent directories were created
        self.assertTrue(output_file.parent.exists())
        self.assertTrue(output_file.exists())

    def test_handles_oserror_gracefully(self):
        """Test that OSError is handled and causes sys.exit(1)"""
        request_json = {"tool": "TEST_TOOL"}
        # Use an invalid path that will cause an OSError
        output_file = Path("/invalid/path/that/cannot/be/created/output.json")

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            with self.assertRaises(SystemExit) as cm:
                create_request_util.write_request_file(request_json, output_file)

            # Verify exit code is 1
            self.assertEqual(cm.exception.code, 1)

            # Verify error message was printed
            error_output = mock_stderr.getvalue()
            self.assertIn("Error: Failed to write output file", error_output)

    def test_overwrites_existing_file(self):
        """Test that existing file is overwritten"""
        request_json_v1 = {"tool": "TEST_TOOL", "version": 1}
        request_json_v2 = {"tool": "TEST_TOOL", "version": 2}
        output_file = self.temp_path / "overwrite.json"

        # Write first version
        with patch("sys.stdout", new_callable=StringIO):
            create_request_util.write_request_file(request_json_v1, output_file)

        # Write second version (should overwrite)
        with patch("sys.stdout", new_callable=StringIO):
            create_request_util.write_request_file(request_json_v2, output_file)

        # Verify file contains second version
        with open(output_file, "r") as f:
            loaded_json = json.load(f)

        self.assertEqual(loaded_json["version"], 2)


if __name__ == "__main__":
    unittest.main()
