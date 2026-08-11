#!/usr/bin/env python3
"""
Tests for create_clear_index_request.py script.

This test suite verifies that the script correctly generates JSON request files
for the ClearIndexTool maintenance operation.
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

# Add the maintenance directory to the path so we can import the script
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "tools" / "maintenance"))

import create_clear_index_request


class TestCreateClearIndexRequest(unittest.TestCase):
    """Test suite for create_clear_index_request.py"""

    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)

    def tearDown(self):
        """Clean up test fixtures"""
        self.temp_dir.cleanup()

    def test_creates_valid_json_with_all_fields(self):
        """Test that the script creates a valid JSON file with all required fields"""
        output_file = self.temp_path / "test_request.json"

        test_args = [
            "create_clear_index_request.py",
            "--generation-id",
            "test_gen_123_f6_u0_a0",
            "--requested-at",
            "2026-02-05T10:30:00Z",
            "--requested-by",
            "test-engineer",
            "--reason",
            "Testing clear index request",
            "--output",
            str(output_file),
        ]

        with patch.object(sys, "argv", test_args):
            create_clear_index_request.main()

        # Verify file was created
        self.assertTrue(output_file.exists(), "Output file should be created")

        # Verify JSON structure
        with open(output_file, "r") as f:
            request_json = json.load(f)

        # Check required fields
        self.assertEqual(request_json["tool"], "CLEAR_INDEX")
        self.assertEqual(request_json["generationId"], "test_gen_123_f6_u0_a0")

        # Check metadata
        self.assertIn("metadata", request_json)
        metadata = request_json["metadata"]
        self.assertEqual(metadata["requestedAt"], "2026-02-05T10:30:00Z")
        self.assertEqual(metadata["requestedBy"], "test-engineer")
        self.assertEqual(metadata["reason"], "Testing clear index request")

    def test_creates_file_with_minimal_args(self):
        """Test that the script works with only required arguments (no --output)"""
        import re

        test_args = [
            "create_clear_index_request.py",
            "--generation-id",
            "minimal_gen_id",
            "--requested-at",
            "2026-02-05T12:00:00Z",
            "--requested-by",
            "automation",
            "--reason",
            "Automated test",
        ]

        # Mock the BUILD_WORKSPACE_DIRECTORY to use our temp directory
        with patch.dict("os.environ", {"BUILD_WORKSPACE_DIRECTORY": str(self.temp_path)}):
            with patch.object(sys, "argv", test_args):
                create_clear_index_request.main()

        # Find the generated file (it will have a timestamp in the name)
        generated_files = list(self.temp_path.glob("request_minimal_gen_id_*.json"))
        self.assertEqual(len(generated_files), 1, "Should create exactly one output file")

        output_file = generated_files[0]

        # Verify the JSON content
        with open(output_file, "r") as f:
            request_json = json.load(f)

        self.assertEqual(request_json["tool"], "CLEAR_INDEX")
        self.assertEqual(request_json["generationId"], "minimal_gen_id")

    def test_generation_id_with_special_characters(self):
        """Test that generation IDs with underscores and numbers are handled correctly"""
        output_file = self.temp_path / "special_chars.json"

        test_args = [
            "create_clear_index_request.py",
            "--generation-id",
            "507f191e810c19729de860ea_f6_u2_a3",
            "--requested-at",
            "2026-02-05T14:00:00Z",
            "--requested-by",
            "test-user",
            "--reason",
            "Test special characters",
            "--output",
            str(output_file),
        ]

        with patch.object(sys, "argv", test_args):
            create_clear_index_request.main()

        with open(output_file, "r") as f:
            request_json = json.load(f)

        self.assertEqual(request_json["generationId"], "507f191e810c19729de860ea_f6_u2_a3")


if __name__ == "__main__":
    unittest.main()
