#!/usr/bin/env python3
"""
Tests for create_force_merge_deletes_request.py script.

This test suite verifies that the script correctly generates JSON request files
for the ForceMergeDeletesTool maintenance operation.
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

# Add the maintenance directory to the path so we can import the script
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "tools" / "maintenance"))

import create_force_merge_deletes_request


class TestCreateForceMergeDeletesRequest(unittest.TestCase):
    """Test suite for create_force_merge_deletes_request.py"""

    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)

        # Create sample index definition files
        self.search_index_def = {
            "searchIndexDefinition": {
                "indexID": "507f191e810c19729de860ea",
                "name": "testIndex",
                "database": "testDb",
                "lastObservedCollectionName": "testCollection",
                "collectionUUID": "eb6c40ca-f25e-47e8-b48c-02a05b64a5aa",
                "mappings": {"dynamic": False, "fields": {}},
            }
        }

        self.vector_index_def = {
            "vectorIndexDefinition": {
                "indexID": "507f191e810c19729de860eb",
                "name": "vectorIndex",
                "database": "testDb",
                "lastObservedCollectionName": "testCollection",
                "collectionUUID": "eb6c40ca-f25e-47e8-b48c-02a05b64a5aa",
                "type": "vectorSearch",
                "fields": [],
            }
        }

    def tearDown(self):
        """Clean up test fixtures"""
        self.temp_dir.cleanup()

    def _create_index_def_file(self, index_def, filename="index_def.json"):
        """Helper to create an index definition file"""
        index_def_file = self.temp_path / filename
        with open(index_def_file, "w") as f:
            json.dump(index_def, f)
        return index_def_file

    def test_creates_valid_json_with_search_index(self):
        """Test that the script creates a valid JSON file for a search index"""
        index_def_file = self._create_index_def_file(self.search_index_def)
        output_file = self.temp_path / "test_request.json"

        test_args = [
            "create_force_merge_deletes_request.py",
            "--generation-id",
            "test_gen_123_f6_u0_a0",
            "--index-def",
            str(index_def_file),
            "--requested-at",
            "2026-02-05T10:30:00Z",
            "--requested-by",
            "test-engineer",
            "--reason",
            "Testing force merge deletes request",
            "--output",
            str(output_file),
        ]

        with patch.object(sys, "argv", test_args):
            create_force_merge_deletes_request.main()

        # Verify file was created
        self.assertTrue(output_file.exists(), "Output file should be created")

        # Verify JSON structure
        with open(output_file, "r") as f:
            request_json = json.load(f)

        # Check required fields
        self.assertEqual(request_json["tool"], "FORCE_MERGE_DELETES")
        self.assertEqual(request_json["generationId"], "test_gen_123_f6_u0_a0")
        self.assertIn("searchIndexDefinition", request_json)
        self.assertEqual(
            request_json["searchIndexDefinition"], self.search_index_def["searchIndexDefinition"]
        )

        # Check metadata
        self.assertIn("metadata", request_json)
        metadata = request_json["metadata"]
        self.assertEqual(metadata["requestedAt"], "2026-02-05T10:30:00Z")
        self.assertEqual(metadata["requestedBy"], "test-engineer")
        self.assertEqual(metadata["reason"], "Testing force merge deletes request")

    def test_creates_valid_json_with_vector_index(self):
        """Test that the script creates a valid JSON file for a vector index"""
        index_def_file = self._create_index_def_file(self.vector_index_def)
        output_file = self.temp_path / "vector_request.json"

        test_args = [
            "create_force_merge_deletes_request.py",
            "--generation-id",
            "vector_gen_456_f6_u0_a0",
            "--index-def",
            str(index_def_file),
            "--requested-at",
            "2026-02-05T11:00:00Z",
            "--requested-by",
            "test-engineer",
            "--reason",
            "Testing vector index",
            "--output",
            str(output_file),
        ]

        with patch.object(sys, "argv", test_args):
            create_force_merge_deletes_request.main()

        with open(output_file, "r") as f:
            request_json = json.load(f)

        self.assertEqual(request_json["tool"], "FORCE_MERGE_DELETES")
        self.assertIn("vectorIndexDefinition", request_json)
        self.assertEqual(
            request_json["vectorIndexDefinition"], self.vector_index_def["vectorIndexDefinition"]
        )
        self.assertNotIn("searchIndexDefinition", request_json)

    def test_creates_file_with_percentage_option(self):
        """Test that the script correctly includes the percentage option"""
        index_def_file = self._create_index_def_file(self.search_index_def)
        output_file = self.temp_path / "percentage_request.json"

        test_args = [
            "create_force_merge_deletes_request.py",
            "--generation-id",
            "pct_gen_789",
            "--index-def",
            str(index_def_file),
            "--percentage",
            "15.5",
            "--requested-at",
            "2026-02-05T12:00:00Z",
            "--requested-by",
            "automation",
            "--reason",
            "Test percentage option",
            "--output",
            str(output_file),
        ]

        with patch.object(sys, "argv", test_args):
            create_force_merge_deletes_request.main()

        with open(output_file, "r") as f:
            request_json = json.load(f)

        self.assertIn("forceMergeDeletesPctAllowed", request_json)
        self.assertEqual(request_json["forceMergeDeletesPctAllowed"], 15.5)

    def test_creates_file_without_percentage_option(self):
        """Test that the script works without the optional percentage parameter"""
        index_def_file = self._create_index_def_file(self.search_index_def)
        output_file = self.temp_path / "no_percentage_request.json"

        test_args = [
            "create_force_merge_deletes_request.py",
            "--generation-id",
            "no_pct_gen",
            "--index-def",
            str(index_def_file),
            "--requested-at",
            "2026-02-05T13:00:00Z",
            "--requested-by",
            "automation",
            "--reason",
            "Test without percentage",
            "--output",
            str(output_file),
        ]

        with patch.object(sys, "argv", test_args):
            create_force_merge_deletes_request.main()

        with open(output_file, "r") as f:
            request_json = json.load(f)

        # Percentage should not be in the request
        self.assertNotIn("forceMergeDeletesPctAllowed", request_json)

    def test_invalid_percentage_fails(self):
        """Test that invalid percentage values cause the script to fail"""
        index_def_file = self._create_index_def_file(self.search_index_def)

        # Note: Empty string "" is treated as "not provided" by the script
        # because of the `if args.percentage:` check, so it won't fail
        invalid_percentages = ["-5.0", "150.0", "abc"]

        for invalid_pct in invalid_percentages:
            with self.subTest(percentage=invalid_pct):
                # Don't specify --output so the script will try to create a default file
                # We'll mock the environment to prevent actual file creation
                test_args = [
                    "create_force_merge_deletes_request.py",
                    "--generation-id",
                    "invalid_pct",
                    "--index-def",
                    str(index_def_file),
                    "--percentage",
                    invalid_pct,
                    "--requested-at",
                    "2026-02-05T14:00:00Z",
                    "--requested-by",
                    "test",
                    "--reason",
                    "Test invalid percentage",
                    # No --output argument - script should fail before creating any file
                ]

                # Mock BUILD_WORKSPACE_DIRECTORY to use temp directory
                # This ensures if a file is created, it goes to temp and gets cleaned up
                with patch.dict("os.environ", {"BUILD_WORKSPACE_DIRECTORY": str(self.temp_path)}):
                    with patch.object(sys, "argv", test_args):
                        with self.assertRaises(SystemExit) as cm:
                            create_force_merge_deletes_request.main()
                        self.assertNotEqual(cm.exception.code, 0)

                # Verify no output files were created (script should fail before writing)
                generated_files = list(self.temp_path.glob("request_invalid_pct_*.json"))
                self.assertEqual(
                    len(generated_files),
                    0,
                    f"No files should be created for invalid percentage {invalid_pct}",
                )

    def test_empty_string_percentage_treated_as_not_provided(self):
        """Test that empty string percentage is treated as 'not provided'"""
        index_def_file = self._create_index_def_file(self.search_index_def)
        output_file = self.temp_path / "empty_pct.json"

        test_args = [
            "create_force_merge_deletes_request.py",
            "--generation-id",
            "empty_pct",
            "--index-def",
            str(index_def_file),
            "--percentage",
            "",  # Empty string
            "--requested-at",
            "2026-02-05T14:30:00Z",
            "--requested-by",
            "test",
            "--reason",
            "Test empty percentage",
            "--output",
            str(output_file),
        ]

        # This should succeed because empty string is falsy and treated as "not provided"
        with patch.object(sys, "argv", test_args):
            create_force_merge_deletes_request.main()

        with open(output_file, "r") as f:
            request_json = json.load(f)

        # Percentage should not be in the request (treated as not provided)
        self.assertNotIn("forceMergeDeletesPctAllowed", request_json)

    def test_valid_percentage_boundary_values(self):
        """Test that boundary percentage values (0.0 and 100.0) are accepted"""
        index_def_file = self._create_index_def_file(self.search_index_def)

        valid_percentages = ["0.0", "100.0", "50.0"]

        for valid_pct in valid_percentages:
            with self.subTest(percentage=valid_pct):
                output_file = self.temp_path / f"boundary_{valid_pct}.json"
                test_args = [
                    "create_force_merge_deletes_request.py",
                    "--generation-id",
                    f"boundary_{valid_pct}",
                    "--index-def",
                    str(index_def_file),
                    "--percentage",
                    valid_pct,
                    "--requested-at",
                    "2026-02-05T15:00:00Z",
                    "--requested-by",
                    "test",
                    "--reason",
                    "Test boundary percentage",
                    "--output",
                    str(output_file),
                ]

                with patch.object(sys, "argv", test_args):
                    create_force_merge_deletes_request.main()

                with open(output_file, "r") as f:
                    request_json = json.load(f)

                self.assertEqual(request_json["forceMergeDeletesPctAllowed"], float(valid_pct))

    def test_missing_index_definition_file_fails(self):
        """Test that missing index definition file causes the script to fail"""
        nonexistent_file = self.temp_path / "nonexistent.json"

        test_args = [
            "create_force_merge_deletes_request.py",
            "--generation-id",
            "missing_file",
            "--index-def",
            str(nonexistent_file),
            "--requested-at",
            "2026-02-05T16:00:00Z",
            "--requested-by",
            "test",
            "--reason",
            "Test missing file",
        ]

        with patch.object(sys, "argv", test_args):
            with self.assertRaises(SystemExit) as cm:
                create_force_merge_deletes_request.main()
            self.assertNotEqual(cm.exception.code, 0)

    def test_invalid_json_in_index_definition_fails(self):
        """Test that invalid JSON in index definition file causes the script to fail"""
        invalid_json_file = self.temp_path / "invalid.json"
        with open(invalid_json_file, "w") as f:
            f.write("{ invalid json content }")

        test_args = [
            "create_force_merge_deletes_request.py",
            "--generation-id",
            "invalid_json",
            "--index-def",
            str(invalid_json_file),
            "--requested-at",
            "2026-02-05T17:00:00Z",
            "--requested-by",
            "test",
            "--reason",
            "Test invalid JSON",
        ]

        with patch.object(sys, "argv", test_args):
            with self.assertRaises(SystemExit) as cm:
                create_force_merge_deletes_request.main()
            self.assertNotEqual(cm.exception.code, 0)

    def test_index_definition_with_both_types_fails(self):
        """Test that index definition with both search and vector definitions fails"""
        invalid_index_def = {
            "searchIndexDefinition": self.search_index_def["searchIndexDefinition"],
            "vectorIndexDefinition": self.vector_index_def["vectorIndexDefinition"],
        }
        index_def_file = self._create_index_def_file(invalid_index_def)

        test_args = [
            "create_force_merge_deletes_request.py",
            "--generation-id",
            "both_types",
            "--index-def",
            str(index_def_file),
            "--requested-at",
            "2026-02-05T18:00:00Z",
            "--requested-by",
            "test",
            "--reason",
            "Test both types",
        ]

        with patch.object(sys, "argv", test_args):
            with self.assertRaises(SystemExit) as cm:
                create_force_merge_deletes_request.main()
            self.assertNotEqual(cm.exception.code, 0)

    def test_index_definition_with_neither_type_fails(self):
        """Test that index definition with neither search nor vector definition fails"""
        invalid_index_def = {"someOtherField": "value"}
        index_def_file = self._create_index_def_file(invalid_index_def)

        test_args = [
            "create_force_merge_deletes_request.py",
            "--generation-id",
            "neither_type",
            "--index-def",
            str(index_def_file),
            "--requested-at",
            "2026-02-05T19:00:00Z",
            "--requested-by",
            "test",
            "--reason",
            "Test neither type",
        ]

        with patch.object(sys, "argv", test_args):
            with self.assertRaises(SystemExit) as cm:
                create_force_merge_deletes_request.main()
            self.assertNotEqual(cm.exception.code, 0)

    def test_missing_required_argument_fails(self):
        """Test that missing each required argument causes the script to fail"""
        index_def_file = self._create_index_def_file(self.search_index_def)

        # Define all required arguments
        all_required_args = {
            "--generation-id": "test_gen_id",
            "--index-def": str(index_def_file),
            "--requested-at": "2026-02-05T20:00:00Z",
            "--requested-by": "test",
            "--reason": "Test reason",
        }

        # Test each required argument by omitting it
        for missing_arg in all_required_args.keys():
            with self.subTest(missing_argument=missing_arg):
                # Build args list with all arguments except the one we're testing
                test_args = ["create_force_merge_deletes_request.py"]
                for arg_name, arg_value in all_required_args.items():
                    if arg_name != missing_arg:
                        test_args.extend([arg_name, arg_value])

                # Verify that the script exits with an error
                with patch.object(sys, "argv", test_args):
                    with self.assertRaises(SystemExit) as cm:
                        create_force_merge_deletes_request.main()

                    # Verify it's a non-zero exit code (error)
                    self.assertNotEqual(
                        cm.exception.code, 0, f"Script should fail when {missing_arg} is missing"
                    )


if __name__ == "__main__":
    unittest.main()
