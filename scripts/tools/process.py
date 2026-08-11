import os
import subprocess
import sys
from typing import Dict


class Process:
    def __init__(self):
        self._command = None
        self._cwd = None
        self._stream = True
        self._args = []
        self._env = {}

    def command(self, command: str) -> "Process":
        self._command = command
        return self

    def cwd(self, cwd: str) -> "Process":
        self._cwd = cwd
        return self

    def stream(self, stream: bool) -> "Process":
        self._stream = stream
        return self

    def env(self, key: str, value: str) -> "Process":
        self._env[key] = value
        return self

    def envs(self, envs: Dict[str, str]) -> "Process":
        self._env.update(envs)
        return self

    def arg(self, arg: str) -> "Process":
        self._args += [arg]
        return self

    def args(self, *args: str) -> "Process":
        self._args += args
        return self

    def execute(self) -> subprocess.CompletedProcess:
        # Want to ensure we inherit environment variables such as $HOME or $PATH from
        # the current process.
        env = os.environ.copy()
        env.update(self._env)

        if self._stream:
            kwargs = {"stdout": sys.stdout, "stderr": sys.stderr}
        else:
            kwargs = {"capture_output": True}

        return subprocess.run(
            [self._command, *self._args], cwd=self._cwd, env=env, text=True, **kwargs
        )
