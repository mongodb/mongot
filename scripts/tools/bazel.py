import subprocess
from enum import Enum
from typing import List, Optional, Tuple, Union

from scripts.tools.mongot import MONGOT_ROOT
from scripts.tools.process import Process


class Label:
    def __init__(self, value: str):
        self._value = value

    def __str__(self):
        return self._value

    def __repr__(self):
        return self._value

    @staticmethod
    def all() -> "Label":
        return Label("//...")

    @staticmethod
    def join(*labels: "Label") -> "Label":
        return Label(" ".join(str(label) for label in labels))


class Query:
    def __init__(self, value: str):
        self._value = value

    def __str__(self):
        return self._value

    def __repr__(self):
        return self._value

    @staticmethod
    def all_tests() -> "Query":
        return Query.tests(Label.all())

    @staticmethod
    def tests(label: Label) -> "Query":
        return Query(f"tests({str(label)})")

    @staticmethod
    def tags(tag: str) -> "Query":
        return Query.attr("tags", tag, Query(str(Label.all())))

    @staticmethod
    def attr(attribute: str, pattern: str, query: "Query") -> "Query":
        return Query(f"attr({attribute}, {pattern}, {str(query)})")

    @staticmethod
    def intersect(*queries: "Query") -> "Query":
        return Query(f"( {' intersect '.join(str(query) for query in queries)} )")


class Bazel:
    WORKSPACE = MONGOT_ROOT
    BAZELISK = WORKSPACE.joinpath("scripts/tools/bazelisk/run.sh")

    class Command(Enum):
        BUILD = "build"
        QUERY = "query"
        RUN = "run"
        TEST = "test"

    class FailedCommandError(Exception):
        def __init__(
            self,
            command: "Bazel.Command",
            args: Tuple[str, ...],
            exit_code: int,
            stderr: Optional[str],
        ):
            self.command = command
            self.args = args
            self.exit_code = exit_code
            self.stderr = stderr

        def __str__(self):
            message = f"""Bazel command '{self.command.value} {" ".join(self.args)}' failed (exit code {self.exit_code})"""
            if self.stderr is not None:
                message += f""":
{self.stderr}
                """

            return message

    @staticmethod
    def build(labels: Union[Label, List[Label]], bazel_flags: List[str] = None):
        if bazel_flags is None:
            bazel_flags = []

        if isinstance(labels, Label):
            labels = [labels]

        Bazel._execute(True, Bazel.Command.BUILD, bazel_flags, *(str(label) for label in labels))

    @staticmethod
    def query(query: Query) -> List[Label]:
        result = Bazel._execute(False, Bazel.Command.QUERY, [], str(query))
        return [Label(label) for label in result.stdout.splitlines()]

    @staticmethod
    def run(label: Label, bazel_flags: List[str] = None, flags: List[str] = None):
        if bazel_flags is None:
            bazel_flags = []

        args = [] if flags is None or len(flags) == 0 else ["--", *flags]
        Bazel._execute(True, Bazel.Command.RUN, bazel_flags, str(label), *args)

    @staticmethod
    def test(label: Label, bazel_flags: List[str] = None):
        if bazel_flags is None:
            bazel_flags = []

        Bazel._execute(True, Bazel.Command.TEST, bazel_flags, str(label))

    @staticmethod
    def _execute(
        stream: bool, command: "Bazel.Command", bazel_flags: List[str], *args: str
    ) -> subprocess.CompletedProcess:
        result = (
            Process()
            .command(Bazel.BAZELISK)
            .cwd(Bazel.WORKSPACE)
            .stream(stream)
            .arg(command.value)
            .args(*bazel_flags)
            .args(*args)
            .execute()
        )

        if result.returncode != 0:
            raise Bazel.FailedCommandError(
                command, args, result.returncode, result.stderr if not stream else None
            )

        return result
