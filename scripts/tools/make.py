from typing import Dict

from scripts.tools.mongot import MONGOT_ROOT
from scripts.tools.process import Process


class Make:
    MAKE = "make"

    class FailedCommandError(Exception):
        def __init__(self, target: str, exit_code: int):
            self.target = target
            self.exit_code = exit_code

        def __str__(self):
            return f"""Make target '{self.target}' failed (exit code {self.exit_code})"""

    @staticmethod
    def run_target(target: str, env: Dict[str, str] = None, args: Dict[str, str] = None):
        if env is None:
            env = {}

        args = [f"{key}={value}" for (key, value) in args.items()] if args is not None else []
        result = (
            Process()
            .command(Make.MAKE)
            .cwd(MONGOT_ROOT)
            .stream(True)
            .arg(target)
            .args(*args)
            .envs(env)
            .execute()
        )

        if result.returncode != 0:
            raise Make.FailedCommandError(target, result.returncode)

        return result
