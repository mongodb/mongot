import argparse
import contextlib
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict

TEST_PATH = "src/test/integration/java/com/xgen/mongot/index/TestQueryIntegrationSubset.java"
TEST_TARGET = "src/test/integration/java/com/xgen/mongot/index:TestQueryIntegrationSubset"
TEST_SPEC_PATH = "src/test/integration/resources/index/bisection.json"


# ANSI colors for terminal output
class Colors:
    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    BLUE = "\033[0;34m"
    NOCOLOR = "\033[0m"


class GitBisectError(Exception):
    """Custom exception for git bisect errors."""

    pass


def get_test_files() -> Dict[str, bytes]:
    """Get content of test files we want to copy across commits."""
    files = {}
    test_files = [TEST_PATH, TEST_SPEC_PATH]

    for filepath in test_files:
        if os.path.exists(filepath):
            with open(filepath, "rb") as f:
                files[filepath] = f.read()
            print(f"Captured {filepath} for bisect testing")
        else:
            print(f"Warning: Could not find {filepath}")

    return files


def apply_test_files(files: Dict[str, bytes]) -> None:
    """
    Write test files to the current checkout.
    Creates directories as needed and overwrites existing files.
    """
    for filepath, content in files.items():
        print(f"applying changes from HEAD to {filepath}")
        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        # Write file
        with open(filepath, "wb") as f:
            f.write(content)


@contextlib.contextmanager
def cleanup_state(uncommitted_files: Dict[str, bytes]):
    """
    Context manager to ensure git state is restored on completion or error.
    Handles bisect reset, docker cleanup, and restoring uncommitted changes.
    """
    # Store initial branch for later restoration
    try:
        current_branch = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
    except subprocess.CalledProcessError:
        current_branch = "HEAD"

    try:
        yield
    finally:
        # Cleanup sequence: bisect -> docker -> branch -> files
        with contextlib.suppress(subprocess.CalledProcessError):
            subprocess.run(["git", "bisect", "reset"], check=False)
        with contextlib.suppress(subprocess.CalledProcessError):
            subprocess.run(["make", "docker.down"], check=False, stdout=subprocess.DEVNULL)
        with contextlib.suppress(subprocess.CalledProcessError):
            subprocess.run(["git", "checkout", current_branch], check=True)
            subprocess.run(["git", "checkout", "--", "."], check=True)
            if uncommitted_files:
                apply_test_files(uncommitted_files)


class GitBisect:
    """Handles individual test runs at specific commits with Docker setup/teardown."""

    def __init__(self, bazelisk_path: str, test_files: Dict[str, bytes]):
        self.bazelisk_path = Path(bazelisk_path)
        self.test_files = test_files

    def run_test(self) -> int:
        """
        Run test with Docker setup/teardown.
        Returns 0 if test passes, 1 if it fails.
        """
        try:
            # Clean working directory and apply test files
            subprocess.run(["git", "checkout", "--", "."], check=True)
            if self.test_files:
                apply_test_files(self.test_files)

            # Setup and run test in Docker
            self._color_print("Setting up Docker environment...", Colors.BLUE)
            subprocess.run(["make", "docker.up"], check=True)

            time.sleep(5)  # Wait for Docker services to be ready

            test_command = f"{self.bazelisk_path} test --test_output=errors {TEST_TARGET}"
            self._color_print(f"Running test: {test_command}", Colors.BLUE)
            subprocess.run(test_command.split(), check=True)
            return 0

        except subprocess.CalledProcessError:
            return 1

        finally:
            # Cleanup after test regardless of outcome
            self._color_print("Tearing down Docker environment...", Colors.BLUE)
            subprocess.run(["make", "docker.down"], check=False)
            subprocess.run(["git", "checkout", "--", "."], check=True)

    def _color_print(self, message: str, color: str = "") -> None:
        print(f"{color}{message}{Colors.NOCOLOR}")


class BisectRunner:
    """Orchestrates the git bisect process and manages state."""

    def __init__(self, bazelisk_path: str, good_commit: str, bad_commit: str):
        self.bazelisk_path = Path(bazelisk_path)
        # Resolve HEAD references immediately to handle detached HEAD states
        self.bad_commit = (
            self._get_revision(bad_commit) if bad_commit.startswith("HEAD") else bad_commit
        )
        self.good_commit = (
            self._get_revision(good_commit) if good_commit.startswith("HEAD") else good_commit
        )
        self.current_branch = self._get_current_branch()
        self.test_files = get_test_files()
        self._validate_basic_inputs()
        if self.test_files:
            print("Found uncommitted test files that will be used during bisect:")
            for f in self.test_files:
                print(f"  {f}")

    def _color_print(self, message: str, color: str = "") -> None:
        print(f"{color}{message}{Colors.NOCOLOR}")

    def _get_current_branch(self) -> str:
        """Get name of current git branch."""
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True, check=True
        )
        return result.stdout.strip()

    def _get_revision(self, revision: str) -> str:
        """Resolve git revision specifier to full commit hash."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", revision], capture_output=True, text=True, check=True
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError:
            # Fall back to first commit if revision can't be resolved
            result = subprocess.run(
                ["git", "rev-list", "--max-parents=0", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip()

    def _validate_basic_inputs(self) -> None:
        """
        Validate all inputs before starting bisect:
        - Git repository exists
        - Bazelisk is available
        - Commits are valid
        - Bazel target exists
        """
        if subprocess.run(["git", "rev-parse", "--git-dir"], capture_output=True).returncode != 0:
            raise GitBisectError("Not in a git repository")

        if not self.bazelisk_path.exists():
            raise GitBisectError(f"Bazelisk script not found: {self.bazelisk_path}")
        if not os.access(self.bazelisk_path, os.X_OK):
            raise GitBisectError(f"Bazelisk script not executable: {self.bazelisk_path}")

        for commit, commit_type in [(self.good_commit, "good"), (self.bad_commit, "bad")]:
            if (
                subprocess.run(
                    ["git", "rev-parse", "--verify", commit], capture_output=True
                ).returncode
                != 0
            ):
                raise GitBisectError(f"Invalid {commit_type} commit: {commit}")

    def _run_at_commit(self, commit: str) -> bool:
        """Run test at a specific commit."""
        subprocess.run(["git", "checkout", "-f", commit], check=True)
        bisect = GitBisect(self.bazelisk_path, self.test_files)
        return bisect.run_test() == 0

    def run(self) -> None:
        """
        Run the git bisect process:
        1. Validate good/bad commits behave as expected
        2. Start bisect
        3. Test each commit and mark good/bad
        4. Report first bad commit
        """
        with cleanup_state(self.test_files):
            self._color_print("Starting git bisect process", Colors.BLUE)
            print(f"Good commit: {self.good_commit}")
            print(f"Bad commit: {self.bad_commit}")

            # Verify good/bad commits actually pass/fail
            self._color_print("\nValidating commits...", Colors.BLUE)

            if not self._run_at_commit(self.good_commit):
                raise GitBisectError(f"'Good' commit {self.good_commit[:8]} fails the test!")
            self._color_print(
                f"✓ Verified 'good' commit {self.good_commit[:8]} passes test", Colors.GREEN
            )

            if self._run_at_commit(self.bad_commit):
                raise GitBisectError(f"'Bad' commit {self.bad_commit[:8]} passes the test!")
            self._color_print(
                f"✓ Verified 'bad' commit {self.bad_commit[:8]} fails test", Colors.GREEN
            )

            print("-" * 40)
            self._color_print("Running bisect...", Colors.BLUE)

            # Start bisect process
            subprocess.run(["git", "bisect", "start"], check=True)
            subprocess.run(["git", "bisect", "good", self.good_commit], check=True)
            subprocess.run(["git", "bisect", "bad", self.bad_commit], check=True)

            # Run bisect loop
            while True:
                current = subprocess.run(
                    ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
                ).stdout.strip()

                # Test current commit
                test_passes = self._run_at_commit(current)
                cmd = ["git", "bisect", "good" if test_passes else "bad"]
                result = subprocess.run(cmd, capture_output=True, text=True)

                # Check if bisect is complete
                if "is the first bad commit" in result.stdout:
                    self._color_print(f"\nFirst bad commit found: {current}", Colors.GREEN)
                    break
                elif result.returncode != 0:
                    raise GitBisectError("Bisect failed")


def main():
    parser = argparse.ArgumentParser(
        description="Automated git bisect with Docker and Bazel support"
    )
    parser.add_argument("-z", "--bazelisk", required=True, help="Path to bazelisk script")
    parser.add_argument(
        "-g", "--good", default="HEAD~20", help="Known good commit (default: HEAD~20)"
    )
    parser.add_argument("-b", "--bad", default="HEAD", help="Known bad commit (default: HEAD)")

    args = parser.parse_args()

    try:
        runner = BisectRunner(
            bazelisk_path=args.bazelisk, good_commit=args.good, bad_commit=args.bad
        )
        runner.run()
    except (GitBisectError, KeyboardInterrupt) as e:
        print(f"\n{Colors.RED}Error: {str(e)}{Colors.NOCOLOR}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
