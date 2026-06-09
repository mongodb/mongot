"""Java module extensions.

`java_repos` creates the Adoptium JDK repositories + english_dictionary http_file.
The JDK archive/toolchain definitions live in the sibling jdk_config.bzl.
adoptium_jdk() registers, per platform:
  - adoptium_jdk_<os>_<arch>      : http_archive with our jdk_{linux,macos}.BUILD
                                    template (consumed by
                                    //bazel/java:jdk21_<os>_<arch> java_runtime
                                    targets).
  - adoptium_jdk_<os>_<arch>_repo : remote_java_repository that auto-generates
                                    a toolchain definition for
                                    --java_runtime_version=adoptium_21.
adoptium_jdk_community() registers the @adoptium_jdk_community_* http_archives
(community release JDK) that deploy/BUILD bundles into community artifacts.

`netty_tcnative_ext` wraps netty_tcnative_deps() (the AL2023 http_file download).

`grpc_java` wraps grpc_java_repositories() so its transitive repos (notably
@io_grpc_grpc_proto) become available to the @maven install. `bzlmod = True`
tells the macro to skip repos already provided by bazel_dep modules (protobuf,
rules_ruby, ...) and avoid declaring stale duplicates. Refresh the caller's
`use_repo` list with `bazel mod tidy` if the macro starts declaring more repos.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_file")
load("@io_grpc_grpc_java//:repositories.bzl", "grpc_java_repositories")
load("//bazel/java:jdk_config.bzl", "adoptium_jdk", "adoptium_jdk_community")
load("//bazel/java:netty_tcnative.bzl", "netty_tcnative_deps")

def _impl(_mctx):
    adoptium_jdk()
    adoptium_jdk_community()
    http_file(
        name = "english_dictionary",
        sha256 = "5bef207ef2a954ce7df13cee21e8a181ac6a6f9ccb663a0671fc42dc86fc68c0",
        urls = ["https://raw.githubusercontent.com/dwyl/english-words/54b470a763d3df98ec33cb049382711972975317/words.txt"],
    )

java_repos = module_extension(implementation = _impl)

def _netty_tcnative_impl(_mctx):
    netty_tcnative_deps()

netty_tcnative_ext = module_extension(implementation = _netty_tcnative_impl)

def _grpc_java_impl(_mctx):
    grpc_java_repositories(bzlmod = True)

grpc_java = module_extension(implementation = _grpc_java_impl)
