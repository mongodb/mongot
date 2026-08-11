import argparse
import platform
import signal
import subprocess
import sys
import tempfile
import traceback
from functools import partial
from pathlib import Path
from subprocess import CompletedProcess
from textwrap import dedent
from time import sleep
from typing import List


# ANSI colors for terminal output
class Color:
    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    BLUE = "\033[0;34m"
    OFF = "\033[0m"


scripts_data = Path(__file__).parent.absolute()


def print_info(msg: str):
    print(f"{Color.GREEN}Info:{Color.OFF} {msg}", file=sys.stderr)


def subprocess_log_run(args: List[str], **kwargs) -> CompletedProcess[bytes]:
    print_info(f"Running command: {args}")
    return subprocess.run(args, check=True, **kwargs)


def extract_tarball_for_file(tarball_path: str, dest_dir: Path, filename: str):
    cmd = ["tar", "-xzvf", tarball_path]
    print_info(f"Running command: {cmd}")
    tar = subprocess.run(cmd, cwd=dest_dir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    # Decode and print output before checking return code
    tar_output = tar.stdout.decode()
    print(tar_output, file=sys.stderr)
    tar.check_returncode()

    filepath_to_find = next(l for l in tar_output.splitlines() if l.endswith(f"/{filename}"))
    return dest_dir / (
        filepath_to_find[2:] if filepath_to_find.startswith("x ") else filepath_to_find
    )


def subprocess_expect_output(args: List[str], output_needle: str, **kwargs):
    print_info(f"Running command: {args}")
    proc: CompletedProcess[bytes] = subprocess.run(
        args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kwargs
    )

    # Decode and print output before checking return code
    proc_output = proc.stdout.decode()
    print(proc_output, file=sys.stderr)

    if output_needle not in proc_output:
        raise ValueError(f"Command did not output '{output_needle}' as expected")

    proc.check_returncode()


class MongodbCommunityDownloader:
    SYSTEM_MAPPING = {
        "Darwin": "macos",
        "Linux": "linux",
    }

    DEFAULT_MONGODB_VERSION = "8.0.11"

    LINUX_URL_FORMAT = (
        "https://fastdl.mongodb.org/linux/mongodb-linux-{machine}-{distro}-{version}.tgz"
    )
    MACOS_URL_FORMAT = "https://fastdl.mongodb.org/osx/mongodb-macos-{machine}-{version}.tgz"

    def __init__(self, dest_dir: Path, *, mongodb_version: str = DEFAULT_MONGODB_VERSION):
        self.dest_dir = dest_dir
        self.mongodb_version = mongodb_version

    def download(self) -> Path:
        url = self.url
        filename = url[url.rindex("/") + 1 :]
        print_info(f"Downloading mongod from '{url}' into '{self.dest_dir}' as '{filename}'")

        subprocess_log_run(["curl", "-SLo", filename, url], cwd=self.dest_dir)

        # Parse output from tar command to detect the path to mongod
        return extract_tarball_for_file(filename, self.dest_dir, "mongod")

    @property
    def url(self) -> str:
        platform_system = platform.system()
        if platform_system not in self.SYSTEM_MAPPING:
            raise ValueError(f"unsupported system '{platform_system}'")

        system = self.SYSTEM_MAPPING[platform_system]
        machine = platform.machine()

        if system == "macos":
            return self.MACOS_URL_FORMAT.format(machine=machine, version=self.mongodb_version)

        # The only non-macos system we support is Linux for now
        osrel = platform.freedesktop_os_release()
        osrel_id, osrel_version_id = (osrel["ID"], osrel["VERSION_ID"])
        url_for_distro = partial(
            self.LINUX_URL_FORMAT.format, machine=machine, version=self.mongodb_version
        )

        if osrel_id == "rhel" and osrel_version_id.startswith("8."):
            return url_for_distro(distro="rhel8")
        elif osrel_id == "rhel" and osrel_version_id == "9.3":
            return url_for_distro(distro="rhel93")
        elif osrel_id == "amzn" and osrel_version_id == "2023":
            return url_for_distro(distro="amazon2023")
        elif osrel_id == "sles" and osrel_version_id.startswith("15"):
            return url_for_distro(distro="suse15")
        elif osrel_id == "ubuntu" and osrel_version_id in {"20.04", "22.04", "24.04"}:
            return url_for_distro(distro=f"ubuntu{osrel_version_id.replace('.', '')}")
        elif osrel_id == "debian" and osrel_version_id == "12":
            return url_for_distro(distro="debian12")

        raise ValueError(f"unsupported linux release: ({osrel_id}, {osrel_version_id})")


class MongoshDownloader:
    MACHINE_MAPPING = {
        "x86_64": "x64",
        "arm64": "arm64",
        "aarch64": "arm64",
    }

    SYSTEM_TO_URL_FORMAT = {
        "Linux": "https://downloads.mongodb.com/compass/mongosh-{version}-linux-{machine}.tgz",
        "Linux_rhel8.8": "https://downloads.mongodb.com/compass/mongosh-{version}-linux-{machine}-openssl11.tgz",
        "Linux_rhel9.3": "https://downloads.mongodb.com/compass/mongosh-{version}-linux-{machine}-openssl3.tgz",
        "Linux_amzn": "https://downloads.mongodb.com/compass/mongosh-{version}-linux-{machine}-openssl3.tgz",
        "Darwin": "https://downloads.mongodb.com/compass/mongosh-{version}-darwin-{machine}.zip",
    }

    MONGOSH_VERSION = "2.5.5"

    def __init__(self, dest_dir: Path):
        self.dest_dir = dest_dir

    def download(self) -> Path:
        url = self.url
        filename = url[url.rindex("/") + 1 :]
        print_info(f"Downloading mongosh from '{url}' into '{self.dest_dir}' as '{filename}'")

        subprocess_log_run(["curl", "-SLo", filename, url], cwd=self.dest_dir)

        if filename.endswith("tgz"):
            return extract_tarball_for_file(filename, self.dest_dir, "mongosh")
        elif filename.endswith("zip"):
            unzip = subprocess_log_run(["unzip", filename], cwd=self.dest_dir, capture_output=True)
            unzip_output = unzip.stdout.decode()
            print(unzip_output)
            return self.dest_dir / next(
                line.replace("inflating: ", "")
                for line in (l.strip() for l in unzip_output.splitlines())
                if line.endswith("mongosh")
            )

        raise ValueError(f"unexpected file ending for mongosh archive: {filename}")

    @property
    def url(self) -> str:
        platform_machine = platform.machine()
        if platform_machine not in self.MACHINE_MAPPING:
            raise ValueError(f"unsupported system '{platform_machine}'")
        machine = self.MACHINE_MAPPING[platform_machine]

        system = platform.system()
        system_os = system_os_version = None
        if system == "Linux":
            osrel = platform.freedesktop_os_release()
            system_os = f"Linux_{osrel['ID']}"
            system_os_version = system_os + osrel["VERSION_ID"]

        # Some OSes require a different version of mongosh (OpenSSL version)
        url_format = (
            self.SYSTEM_TO_URL_FORMAT.get(system_os_version)
            or self.SYSTEM_TO_URL_FORMAT.get(system_os)
            or self.SYSTEM_TO_URL_FORMAT.get(system)
        )
        return url_format.format(machine=machine, version=self.MONGOSH_VERSION)


class MonitoredServerProcess:
    proc: subprocess.Popen

    def __init__(self, args: List[str], outfiles_prefix: str, **kwargs):
        self.args = args
        self.outfiles_prefix = outfiles_prefix
        self.kwargs = kwargs

    @property
    def out_filename(self):
        return f"{self.outfiles_prefix}-STDOUT"

    @property
    def err_filename(self):
        return f"{self.outfiles_prefix}-STDERR"

    def __enter__(self):
        print_info(f"Starting monitored process: {self.args}")
        self.proc = subprocess.Popen(
            self.args,
            encoding="utf8",
            stdout=open(self.out_filename, "w"),
            stderr=open(self.err_filename, "w"),
            **self.kwargs,
        )
        return self

    def __exit__(self, *exc):
        print_info(f"Sending SIGINT to monitored process: {self.args}")
        self.proc.send_signal(signal.SIGINT)
        try:
            self.proc.wait(timeout=30)
        except TimeoutError:
            print_info("Process did not complete in 30 seconds, sending SIGKILL")
            self.proc.kill()

        print_info("Monitored process STDOUT:")
        with open(self.out_filename) as f:
            print(f.read(), file=sys.stderr)
        print_info("End of monitored process STDOUT")

        print_info("Monitored process STDERR:")
        with open(self.err_filename) as f:
            print(f.read(), file=sys.stderr)
        print_info("End of monitored process STDERR")


class CompatibilityTestRunner:
    MONGOSH_CREATE_INDEX_COMMAND = dedent("""
        db.runCommand({
            "createSearchIndexes": "movies",
            "indexes": [{"name": "default", "definition": {"mappings": {"dynamic": true}}}]
        });
    """).strip()

    def __init__(self, *, mongot_tarball: str, workdir: Path):
        self.mongot_tarball = mongot_tarball
        self.workdir = workdir

    def run(self):
        mongod_path = MongodbCommunityDownloader(dest_dir=self.workdir).download()
        print_info(f"Unpacked mongod to '{mongod_path}'")

        mongosh_path = MongoshDownloader(dest_dir=self.workdir).download()
        print_info(f"Unpacked mongosh to '{mongosh_path}'")

        print_info(f"Will unpack {self.mongot_tarball} into {self.workdir}")
        mongot_path = extract_tarball_for_file(self.mongot_tarball, self.workdir, "mongot")

        datadir = self.workdir / "data"
        subprocess_log_run(["mkdir", datadir])
        subprocess_log_run(["mkdir", datadir / "mongod"])
        subprocess_log_run(["mkdir", datadir / "mongot"])
        with open(scripts_data / "mongod.conf") as f:
            self.write_file(datadir / "mongod.conf", f.read())
        with open(scripts_data / "mongot.conf") as f:
            self.write_file(datadir / "mongot.conf", f.read())

        self.write_file(datadir / "keyfile", "SuperSecretKeyFile")
        subprocess_log_run(["chmod", "600", datadir / "keyfile"])

        subprocess_log_run(["ls", "-l", self.workdir])
        subprocess_log_run(["ls", "-l", datadir])

        with MonitoredServerProcess(
            [mongod_path, "--config", datadir / "mongod.conf"],
            outfiles_prefix=str(self.workdir / "mongod"),
            cwd=self.workdir,
        ):
            print_info("Waiting 5 seconds to allow mongod to start up...")
            sleep(5)

            # Initiate MongoDB replica set and add admin user to be used.
            print_info("Waiting 5 seconds to allow the mongod replica set to initialize...")
            subprocess_log_run([str(mongosh_path), scripts_data / "mongodSetupStage1.js"])

            with MonitoredServerProcess(
                [mongot_path, "--config", datadir / "mongot.conf"],
                outfiles_prefix=str(self.workdir / "mongot"),
                cwd=self.workdir,
            ):
                # Insert test data
                subprocess_log_run(
                    [
                        str(mongosh_path),
                        "-u",
                        "admin",
                        "-p",
                        "password",
                        scripts_data / "mongodSetupStage2.js",
                    ]
                )

                # As much as I hate sleeping in tests, we need to allow time for mongot to start up
                print_info("Waiting 5 seconds to allow mongot to start up...")
                sleep(5)

                # Create Search index
                subprocess_expect_output(
                    [
                        str(mongosh_path),
                        "-u",
                        "admin",
                        "-p",
                        "password",
                        "--eval",
                        self.MONGOSH_CREATE_INDEX_COMMAND,
                    ],
                    "default",
                )

                # List search indexes
                subprocess_expect_output(
                    [
                        str(mongosh_path),
                        "-u",
                        "admin",
                        "-p",
                        "password",
                        "--eval",
                        'db.runCommand({"listSearchIndexes": "movies"})',
                    ],
                    "default",
                )

                # Sleep again to let mongot build the index
                print_info("Waiting 5 seconds to allow mongot to build the index...")
                sleep(5)

                # Query search index
                subprocess_expect_output(
                    [
                        str(mongosh_path),
                        "-u",
                        "admin",
                        "-p",
                        "password",
                        "--eval",
                        'db.movies.aggregate([{ $search: {"index": "default", "text": {"query": "fallen", "path": "title"}}}, { $limit: 5}])',
                    ],
                    "Jurassic World",
                )

                print_info("Success!!")

    @staticmethod
    def write_file(path: Path, contents: str):
        print_info(f"Writing '{path}' with UTF-8 contents:")
        print(contents, file=sys.stderr)

        with open(path, "w") as f:
            f.write(contents)


def main():
    parser = argparse.ArgumentParser(
        description="Run Community tarball compatibility test with setup of dependencies"
    )
    parser.add_argument("mongot_tarball", nargs=1, help="Path to mongot tarball")
    args = parser.parse_args()

    try:
        with tempfile.TemporaryDirectory() as workdir:
            CompatibilityTestRunner(
                mongot_tarball=args.mongot_tarball[0],
                workdir=Path(workdir),
            ).run()
    except Exception as e:
        print(f"\n{Color.RED}Error:{Color.OFF} {str(e)}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
