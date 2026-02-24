workspace(name = "com_xgen_mongot")

register_toolchains(
    "//bazel/java:mongot_java21_toolchain",
)

load("//bazel/rust/prost:config.bzl", "register_prost_toolchains")

register_prost_toolchains()

# First load any dependencies that themselves contain additional bazel rules.
# For example, we first have to load rules_oci before loading any .bzl file
# that itself tries to load from @rules_oci.
load("//bazel:repos.bzl", "mongot_repos")

mongot_repos()

# Early initialization required for Bazel 8.0.0 (must happen right after repos)
load("//bazel:early_init.bzl", "mongot_early_init")

mongot_early_init()

# Then load any dependencies that those rules may have
load("//bazel:bazel_deps.bzl", "mongot_bazel_deps")

mongot_bazel_deps()

# Some of those dependencies require initialization after being loaded.
load("//bazel:init_bazel_deps.bzl", "mongot_init_bazel_deps")

mongot_init_bazel_deps()

# Then load the actual dependencies that mongot itself requires.
load("//bazel:deps.bzl", "mongot_deps")

mongot_deps()

# Some of those dependencies require initialization after being loaded.
load("//bazel:init_deps.bzl", "mongot_init_deps")

mongot_init_deps()

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Poetry rules for managing Python dependencies
http_archive(
    name = "rules_poetry",
    sha256 = "48001b928488e78f03a47bcc712c56432a471fc6cdd90fe57c884efbfcd13696",
    strip_prefix = "rules_poetry-917630033c736c188605cf0f558c34afc1eca540",
    urls = [
        # Implements retry by relisting each url multiple times to be used as a failover.
        "https://github.com/mongodb-forks/rules_poetry/archive/917630033c736c188605cf0f558c34afc1eca540.tar.gz",
        "https://github.com/mongodb-forks/rules_poetry/archive/917630033c736c188605cf0f558c34afc1eca540.tar.gz",
        "https://github.com/mongodb-forks/rules_poetry/archive/917630033c736c188605cf0f558c34afc1eca540.tar.gz",
        "https://github.com/mongodb-forks/rules_poetry/archive/917630033c736c188605cf0f558c34afc1eca540.tar.gz",
        "https://github.com/mongodb-forks/rules_poetry/archive/917630033c736c188605cf0f558c34afc1eca540.tar.gz",
    ],
)

load("@rules_poetry//rules_poetry:poetry.bzl", "poetry")

http_archive(
    name = "bazel_rules_mongo",
    repo_mapping = {"@poetry": "@poetry_bazel_rules_mongo"},
    sha256 = "4bdde5bcd17fc50bd04697e6dddebde53db052214ef2988658b2ee0a4284d911",
    strip_prefix = "bazel_rules_mongo",
    urls = [
        # Implements retry by relisting each url multiple times to be used as a failover.
        "https://mdb-build-public.s3.amazonaws.com/bazel_rules_mongo/0.1.11/bazel_rules_mongo.tar.gz",
        "https://mdb-build-public.s3.amazonaws.com/bazel_rules_mongo/0.1.11/bazel_rules_mongo.tar.gz",
        "https://mdb-build-public.s3.amazonaws.com/bazel_rules_mongo/0.1.11/bazel_rules_mongo.tar.gz",
    ],
)

load("@bazel_rules_mongo//codeowners:codeowners_validator.bzl", "codeowners_validator")

codeowners_validator()

load("@bazel_rules_mongo//codeowners:codeowners_binary.bzl", "codeowners_binary")

codeowners_binary()

poetry(
    name = "poetry_bazel_rules_mongo",
    lockfile = "@bazel_rules_mongo//:poetry.lock",
    pyproject = "@bazel_rules_mongo//:pyproject.toml",
    python_interpreter_target_mac = "@python_3_11_host//:python",
)
