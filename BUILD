load("@rules_pkg//pkg:mappings.bzl", "pkg_attributes", "pkg_files")
load("//bazel/java:netty_tcnative.bzl", "NETTY_TCNATIVE_X86_64_AL2023_TARGET", "NETTY_TCNATIVE_X86_64_AL2_TARGET")
load("//bazel/java:package.bzl", "java_binary_stamped_manifest")
load("//deploy:def.bzl", "extracted_libraries")

java_binary_stamped_manifest(
    name = "mongot_community",
    classpath_resources = ["//conf/mongot/production:conf"],
    main_class = "com.xgen.mongot.community.MongotCommunity",
    manifest_lines = [
        "Implementation-Title: Atlas Search (mongot)",
        "Implementation-Version: $$BUILD_EMBED_LABEL",
        "Implementation-Vendor: MongoDB Inc.",
    ],
    visibility = ["//visibility:public"],
    runtime_deps = ["//src/main/java/com/xgen/mongot/community"],
)

# Jars that must be bundled individually in a deployable tarball
filegroup(
    name = "fips-jars",
    srcs = [
        "@maven//:org_bouncycastle_bc_fips",
        "@maven//:org_bouncycastle_bctls_fips",
    ],
    visibility = ["//visibility:public"],
)

extracted_libraries({
    "aws-crt": {
        "outputs": {
            "libaws-crt-jni.so": {
                "linux_aarch64": "linux/armv8/glibc/libaws-crt-jni.so",
                "linux_x86_64": "linux/x86_64/glibc/libaws-crt-jni.so",
            },
        },
        "src": "@maven//:software_amazon_awssdk_crt_aws_crt",
    },
    "epoll": {
        "outputs": {
            "libnetty_transport_native_epoll_aarch_64.so": {
                "linux_aarch64": "META-INF/native/libnetty_transport_native_epoll_aarch_64.so",
            },
            "libnetty_transport_native_epoll_x86_64.so": {
                "linux_x86_64": "META-INF/native/libnetty_transport_native_epoll_x86_64.so",
            },
        },
        "src": "//bazel/java:netty-transport-native-epoll",
    },
    "jna": {
        "outputs": {
            "libjnidispatch.so": {
                "linux_aarch64": "com/sun/jna/linux-aarch64/libjnidispatch.so",
                "linux_x86_64": "com/sun/jna/linux-x86-64/libjnidispatch.so",
            },
        },
        "properties": ["-Djna.nosys=false"],
        "src": "@maven//:net_java_dev_jna_jna",
    },
    "snappy": {
        "outputs": {
            "libsnappyjava.so": {
                "linux_aarch64": "org/xerial/snappy/native/Linux/aarch64/libsnappyjava.so",
                "linux_x86_64": "org/xerial/snappy/native/Linux/x86_64/libsnappyjava.so",
            },
        },
        "properties": ["-Dorg.xerial.snappy.use.systemlib=true"],
        "src": "@maven//:org_xerial_snappy_snappy_java",
    },
    "tcnative-aarch64": {
        "outputs": {
            "libnetty_tcnative_linux_aarch_64.so": {
                "linux_aarch64": "META-INF/native/libnetty_tcnative_linux_aarch_64.so",
            },
        },
        "src": "//bazel/java:netty-tcnative",
    },
    "tcnative-x86_64-al2": {
        "outputs": {
            "libnetty_tcnative_x86_64_al2.so": {
                "linux_x86_64": "META-INF/native/libnetty_tcnative_linux_x86_64.so",
            },
        },
        "src": NETTY_TCNATIVE_X86_64_AL2_TARGET,
    },
    "tcnative-x86_64-al2023": {
        "outputs": {
            "libnetty_tcnative_x86_64_al2023.so": {
                "linux_x86_64": "META-INF/native/libnetty_tcnative_linux_x86_64.so",
            },
        },
        "src": NETTY_TCNATIVE_X86_64_AL2023_TARGET,
    },
    "zstd": {
        "outputs": {
            "libzstd-jni-1.5.5-11.so": {
                "linux_aarch64": "linux/aarch64/libzstd-jni-1.5.5-11.so",
                "linux_x86_64": "linux/amd64/libzstd-jni-1.5.5-11.so",
            },
        },
        "src": "@maven//:com_github_luben_zstd_jni",
    },
})

# Don't include extracted dependencies by default
filegroup(
    name = "nothing",
    srcs = [],
)

# Allow test scripts to access test results
filegroup(
    name = "test-results",
    srcs = glob(["bazel-testlogs/**/test.xml"]),
    visibility = ["//scripts/tests:__pkg__"],
)

# Make the comment in .github/CODEOWNERS point to the right rule.
alias(
    name = "codeowners",
    actual = "//bazel/python:codeowners",
)

# Gazelle magic for dependency resolution
# gazelle:java_maven_install_file bazel/java/maven_pin_info.json
# TODO(mccullocht): figure out why there are two sources for this (tomcat?)
# gazelle:resolve java javax.annotation @maven//:com_google_code_findbugs_jsr305
# gazelle:resolve java javax.annotation.concurrent @maven//:com_google_code_findbugs_jsr305
# gazelle:resolve java com.google.errorprone.annotations @maven//:com_google_errorprone_error_prone_annotations
# gazelle:resolve java com.google.errorprone @maven//:com_google_errorprone_error_prone_core
# gazelle:resolve java com.google.errorprone.bugpatterns @maven//:com_google_errorprone_error_prone_core
# TODO(mccullocht): we only every use hamcrest_core so eliminate the other bits
# gazelle:resolve java org.hamcrest.core @maven//:org_hamcrest_hamcrest_core
# gazelle:resolve java com.pholser.junit.quickcheck.generator @maven//:com_pholser_junit_quickcheck_generators

# TODO(mccullocht): figure out why I have to tell gazelle where grpc is.
# gazelle:resolve java io.grpc.health.v1 @io_grpc_grpc_proto//:health_java_proto
# gazelle:resolve java io.grpc.netty @io_grpc_grpc_java//netty
# gazelle:resolve java io.grpc.protobuf @io_grpc_grpc_java//protobuf
# gazelle:resolve java io.grpc.protobuf.services @io_grpc_grpc_java//services:services_maven
# gazelle:resolve java io.grpc.services @io_grpc_grpc_java//services:services_maven
# gazelle:resolve java io.grpc.stub @io_grpc_grpc_java//stub
# gazelle:resolve java io.grpc @io_grpc_grpc_java//api

# Prefer @com_google_protobuf to @maven import of the same library.
# gazelle:resolve java com.google.protobuf @com_google_protobuf//:protobuf_java

# gazelle:resolve java com.xgen.mongot.proto.bson //src/proto/bson:types_java_proto
# gazelle:resolve java com.xgen.mongot.searchenvoy.grpc //src/proto/searchenvoy:searchenvoy_java_proto
# gazelle:resolve java com.xgen.atlas.index.vectorlite //src/main/java/com/xgen/atlas/index/vectorlite

# Wrappers for contrib_jvm rules.
# * library wrapper handles nullaway
# * test wrapper handles extra mockito dependencies test name errors.
# gazelle:map_kind java_library java_library //bazel/java:package.bzl
# gazelle:map_kind java_test_suite java_test_suite //bazel/java:test.bzl

# Disable java_proto_library generation. We need another arrangement for bson java rules.
# gazelle:java_generate_proto false

# TODO: remove ignores and exclusions to fully adopt gazelle.
# gazelle:ignore
# gazelle:exclude src/main/java/com/xgen/perf
# gazelle:exclude src/proto/searchenvoy
# gazelle:exclude src/test/bench
# gazelle:exclude src/test/e2e
# gazelle:exclude src/test/fuzz
# gazelle:exclude src/test/integration
# gazelle:exclude src/test/mock
# gazelle:exclude src/test/perf
# gazelle:exclude src/test/proto
# gazelle:exclude src/test/setup
# gazelle:exclude src/test/unit/java/com/xgen/proto
# gazelle:exclude src/test/unit/java/com/xgen/testing
# gazelle:exclude src/test/util

exports_files([
    "BUILD",
    "WORKSPACE",
    "scripts/runfiles_helper.sh",
])

#package the docker config separately because it will not exist in evergreen builds
pkg_files(
    name = "dockerConfigYaml",
    srcs = glob([".mongot.yml"]),
    attributes = pkg_attributes(mode = "0755"),
    visibility = ["//visibility:public"],
)
