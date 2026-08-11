import subprocess
from enum import Enum

from scripts.tools.process import Process


class Docker:
    DOCKER = "docker"

    class Command(Enum):
        EXEC = "exec"
        INSPECT = "inspect"
        KILL = "kill"
        TAG = "tag"
        LOAD = "load"

    class FailedCommandError(Exception):
        def __init__(self, target: str, exit_code: int, stderr: str):
            self.target = target
            self.exit_code = exit_code
            self.stderr = stderr

        def __str__(self):
            return f"""Make target '{self.target}' failed (exit code {self.exit_code}):
            
{self.stderr}"""

    @staticmethod
    def load_image(image_tar: str) -> None:
        Docker._execute(Docker.Command.LOAD, "-i", image_tar)

    @staticmethod
    def container_is_running(container: str) -> bool:
        try:
            result = Docker._execute(Docker.Command.INSPECT, "-f", "{{.State.Running}}", container)
            return str(result.stdout).strip() == "true"
        except Docker.FailedCommandError:
            return False

    @staticmethod
    def kill(container: str):
        Docker._execute(Docker.Command.KILL, container)

    @staticmethod
    def tag(old_tag: str, new_tag: str):
        Docker._execute(Docker.Command.TAG, old_tag, new_tag)

    @staticmethod
    def metrics(container: str, port: int = 9946) -> str:
        """Fetch the Prometheus metrics page from inside the container."""
        result = Docker._execute(
            Docker.Command.EXEC,
            container,
            "curl",
            "--silent",
            "--fail",
            f"http://localhost:{port}/metrics",
        )
        if not result.stdout:
            raise Exception("failed to scrape prometheus metrics")
        return result.stdout

    @staticmethod
    def _execute(command: "Docker.Command", *args: str) -> "subprocess.CompletedProcess":
        result = Process().command(Docker.DOCKER).stream(False).args(command.value, *args).execute()

        if result.returncode != 0:
            raise Docker.FailedCommandError(command.value, result.returncode, result.stderr)

        return result
