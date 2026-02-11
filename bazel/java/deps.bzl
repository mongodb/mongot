load("@apple_rules_lint//lint:setup.bzl", "lint_setup")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("@bazel_tools//tools/jdk:remote_java_repository.bzl", "remote_java_repository")
load("@contrib_rules_jvm//:gazelle_setup.bzl", "contrib_rules_jvm_gazelle_setup")
load("@contrib_rules_jvm//:setup.bzl", "contrib_rules_jvm_setup")
load("@io_grpc_grpc_java//:repositories.bzl", "IO_GRPC_GRPC_JAVA_ARTIFACTS")
load("@rules_jvm_external//:defs.bzl", "maven_install")
load("//bazel/java:dep_utils.bzl", "append_version", "as_neverlink", "as_test_only")
load("//bazel/java:search_query_deps.bzl", "SEARCH_QUERY_DEPS")
load("//bazel/java:systems_deps.bzl", "SYSTEMS_DEPS")

GUAVA_VERSION = "33.5.0-jre"
GUAVA_ARTIFACTS = append_version(
    GUAVA_VERSION,
    [
        "com.google.guava:guava",
    ],
)

JACKSON_VERSION = "2.19.2"
JACKSON_ARTIFACTS = (
    append_version(
        JACKSON_VERSION,
        [
            "com.fasterxml.jackson.core:jackson-databind",
            "com.fasterxml.jackson.core:jackson-annotations",
            "com.fasterxml.jackson.core:jackson-core",
            "com.fasterxml.jackson.dataformat:jackson-dataformat-yaml",
            "com.fasterxml.jackson.datatype:jackson-datatype-jdk8",
            "com.fasterxml.jackson.datatype:jackson-datatype-jsr310",
            "com.fasterxml.jackson.module:jackson-module-parameter-names",
        ],
    )
)

METRICS_VERSION = "1.15.3"
METRICS_ARTIFACTS = append_version(
    METRICS_VERSION,
    [
        "io.micrometer:micrometer-core",
        "io.micrometer:micrometer-registry-prometheus",
        "io.micrometer:micrometer-registry-otlp",
    ],
)

COMPILE_ONLY_ARTIFACTS = [
    # Bazel v7.6.1 bundles error_prone 2.27.1,
    # Using annotations from other versions may cause compatibility issues.
    "com.google.errorprone:error_prone_annotations:2.27.1",
    "com.google.errorprone:error_prone_core:2.27.1",
    "com.google.errorprone:error_prone_check_api:2.27.1",  # For developing EP checks
    "org.jetbrains:annotations:26.0.2",
]

JMH_VERSION = "1.37"
QUICKCHECK_VERSION = "1.0"
DOCKER_JAVA_VERSION = "3.5.3"
TEST_ONLY_ARTIFACTS = (
    # These maven artifacts will marked testonly and therefore fail the build if they are indirect
    # dependencies of any production Mongot code. However, the transitive dependencies of these
    # targets are NOT automatically marked testonly. This cannot be done without also pinning
    # the version of the transitive test dependencies.
    append_version(
        JMH_VERSION,
        [
            "org.openjdk.jmh:jmh-core",
            "org.openjdk.jmh:jmh-generator-annprocess",
        ],
    ) +
    append_version(GUAVA_VERSION, ["com.google.guava:guava-testlib"]) +
    append_version(
        QUICKCHECK_VERSION,
        [
            "com.pholser:junit-quickcheck-core",
            "com.pholser:junit-quickcheck-generators",
        ],
    ) +
    append_version(
        DOCKER_JAVA_VERSION,
        [
            "com.github.docker-java:docker-java-core",
            "com.github.docker-java:docker-java-api",
            "com.github.docker-java:docker-java-transport",
            "com.github.docker-java:docker-java-transport-httpclient5",
        ],
    ) +
    [
        "com.googlecode.junit-toolbox:junit-toolbox:2.4",
        "org.hamcrest:hamcrest-library:3.0",
        "org.hamcrest:hamcrest-core:3.0",
        "junit:junit:4.13.2",
        "org.mockito:mockito-subclass:5.18.0",
        "com.google.truth:truth:1.4.5",
        "com.googlecode.json-simple:json-simple:1.1.1",
        "com.puppycrawl.tools:checkstyle:12.3.0",
        # SpotBugs 4.9.8 supports Java 21 (class file major version 65)
        "com.github.spotbugs:spotbugs:4.9.8",
    ]
)

OPEN_TELEMETRY_VERSION = "1.53.0"
OPEN_TELEMETRY_SEMCONV_VERSION = "1.28.0"
OPEN_TELEMETRY_ARTIFACTS = (
    append_version(
        OPEN_TELEMETRY_VERSION,
        [
            "io.opentelemetry:opentelemetry-api",
            "io.opentelemetry:opentelemetry-context",
            "io.opentelemetry:opentelemetry-sdk",
            "io.opentelemetry:opentelemetry-sdk-trace",
            "io.opentelemetry:opentelemetry-sdk-common",
            "io.opentelemetry:opentelemetry-exporter-logging-otlp",
            "io.opentelemetry:opentelemetry-exporter-otlp-common",
            "io.opentelemetry:opentelemetry-exporter-common",
            "io.opentelemetry:opentelemetry-exporter-otlp",
        ],
    ) + append_version(
        OPEN_TELEMETRY_SEMCONV_VERSION + "-alpha",
        [
            "io.opentelemetry:opentelemetry-semconv",
        ],
    ) + [
        "io.opentelemetry:opentelemetry-sdk-extension-resources:1.19.0",
    ]
)

MISC_ARTIFACTS = [
    "com.uber.nullaway:nullaway:0.12.15",
    "ch.qos.logback:logback-classic:1.5.25",
    "net.logstash.logback:logstash-logback-encoder:8.1",
    "org.slf4j:slf4j-api:2.0.17",
    "org.slf4j:jul-to-slf4j:2.0.17",
    "com.github.ben-manes.caffeine:caffeine:3.2.2",
    "commons-codec:commons-codec:1.19.0",
    "commons-io:commons-io:2.20.0",
    "info.picocli:picocli:4.7.7",
    "net.jodah:failsafe:2.4.4",
    "org.apache.commons:commons-collections4:4.5.0",
    "org.apache.commons:commons-rng-client-api:1.6",
    "org.apache.commons:commons-rng-simple:1.6",
    "org.apache.commons:commons-rng-sampling:1.6",
    "org.apache.commons:commons-lang3:3.18.0",
    "org.apache.commons:commons-math3:3.6.1",
    "org.apache.commons:commons-text:1.14.0",
    "org.apache.commons:commons-compress:1.28.0",
    "org.apache.httpcomponents:httpclient:4.5.14",
    "org.apache.httpcomponents:httpcore:4.4.16",
    "org.apache.logging.log4j:log4j-core:2.25.3",
    "org.reactivestreams:reactive-streams:1.0.4",
    "org.yaml:snakeyaml:2.4",
    "com.google.flogger:flogger:0.9",
    "com.google.flogger:flogger-slf4j-backend:0.9",
    "com.github.oshi:oshi-core:6.8.2",
    "com.github.luben:zstd-jni:1.5.5-11",
    "org.xerial.snappy:snappy-java:1.1.10.8",
    # Okio is a grpc dependency with CVE-2023-3635 vulnerability,
    # manually pinning to fix version until GRPC integrates it
    # TODO(CLOUDP-196746): Remove okio dependency when GRPC upgrades okio > 3.4.0
    "com.squareup.okio:okio:3.10.2",
    "com.google.auto.service:auto-service:1.1.1",
]

PINNED_TRANSITIVE_ARTIFACTS = [
    "com.google.protobuf:protobuf-java:4.29.0",
    "io.netty:netty-codec-http:4.1.130.Final",
    "io.netty:netty-handler:4.1.130.Final",
    "io.netty:netty-codec-http2:4.1.130.Final",
    # GRPC requires this but doesn't bundle it for some reason
    "org.checkerframework:checker-qual:3.48.4",
    # Force gRPC version to 1.75.0 to address CVE-2025-55163
    "io.grpc:grpc-netty-shaded:1.75.0",
    # Force gson version to 2.13.2 to address CVE-2025-53864
    "com.google.code.gson:gson:2.13.2",
]

def java_deps():
    lint_setup({
        "java-checkstyle": "//conf/checkstyle:mongot",
        "java-spotbugs": "//conf/spotbugs:mongot",
    })
    contrib_rules_jvm_setup()
    contrib_rules_jvm_gazelle_setup()

    maven_install(
        artifacts = _mongot_java_artifacts(),
        repositories = ["https://repo1.maven.org/maven2"],
        fetch_sources = True,
        generate_compat_repositories = True,
        maven_install_json = "//bazel/java:maven_pin_info.json",
        duplicate_version_warning = "error",
        # Use the versions we've pinned if a conflict is detected.
        version_conflict_policy = "pinned",
        excluded_artifacts = _mongot_java_excluded_artifacts(),
    )

    _adoptium_jdk()
    _test_deps()

def _adoptium_jdk():
    _adoptium_jdk_linux_x86_64()
    _adoptium_jdk_linux_x86_64_repo()

    _adoptium_jdk_linux_aarch64()
    _adoptium_jdk_linux_aarch64_repo()

    _adoptium_jdk_macos_x86_64()
    _adoptium_jdk_macos_x86_64_repo()

    _adoptium_jdk_macos_aarch64()
    _adoptium_jdk_macos_aarch64_repo()

def _adoptium_jdk_linux_x86_64():
    http_archive(
        name = "adoptium_jdk_linux_x86_64",
        build_file = "@//bazel/java:jdk_linux.BUILD",
        sha256 = "fffa52c22d797b715a962e6c8d11ec7d79b90dd819b5bc51d62137ea4b22a340",
        strip_prefix = "jdk-21.0.3+9",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.3%2B9/OpenJDK21U-jdk_x64_linux_hotspot_21.0.3_9.tar.gz"],
    )

def _adoptium_jdk_linux_x86_64_repo():
    remote_java_repository(
        name = "adoptium_jdk_linux_x86_64_repo",
        strip_prefix = "jdk-21.0.3+9",
        sha256 = "fffa52c22d797b715a962e6c8d11ec7d79b90dd819b5bc51d62137ea4b22a340",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.3%2B9/OpenJDK21U-jdk_x64_linux_hotspot_21.0.3_9.tar.gz"],
        target_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:x86_64",
        ],
        prefix = "adoptium",
        version = "21",
    )

def _adoptium_jdk_linux_aarch64():
    http_archive(
        name = "adoptium_jdk_linux_aarch64",
        build_file = "@//bazel/java:jdk_linux.BUILD",
        sha256 = "7d3ab0e8eba95bd682cfda8041c6cb6fa21e09d0d9131316fd7c96c78969de31",
        strip_prefix = "jdk-21.0.3+9",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.3%2B9/OpenJDK21U-jdk_aarch64_linux_hotspot_21.0.3_9.tar.gz"],
    )

def _adoptium_jdk_linux_aarch64_repo():
    remote_java_repository(
        name = "adoptium_jdk_linux_aarch64_repo",
        strip_prefix = "jdk-21.0.3+9",
        sha256 = "7d3ab0e8eba95bd682cfda8041c6cb6fa21e09d0d9131316fd7c96c78969de31",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.3%2B9/OpenJDK21U-jdk_aarch64_linux_hotspot_21.0.3_9.tar.gz"],
        target_compatible_with = [
            "@platforms//os:linux",
            "@platforms//cpu:aarch64",
        ],
        prefix = "adoptium",
        version = "21",
    )

def _adoptium_jdk_macos_x86_64():
    http_archive(
        name = "adoptium_jdk_macos_x86_64",
        build_file = "@//bazel/java:jdk_macos.BUILD",
        sha256 = "f777103aab94330d14a29bd99f3a26d60abbab8e2c375cec9602746096721a7c",
        strip_prefix = "jdk-21.0.3+9",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.3%2B9/OpenJDK21U-jdk_x64_mac_hotspot_21.0.3_9.tar.gz"],
    )

def _adoptium_jdk_macos_x86_64_repo():
    remote_java_repository(
        name = "adoptium_jdk_macos_x86_64_repo",
        sha256 = "f777103aab94330d14a29bd99f3a26d60abbab8e2c375cec9602746096721a7c",
        strip_prefix = "jdk-21.0.3+9/Contents/Home",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.3%2B9/OpenJDK21U-jdk_x64_mac_hotspot_21.0.3_9.tar.gz"],
        target_compatible_with = [
            "@platforms//os:macos",
            "@platforms//cpu:x86_64",
        ],
        prefix = "adoptium",
        version = "21",
    )

def _adoptium_jdk_macos_aarch64():
    http_archive(
        name = "adoptium_jdk_macos_aarch64",
        build_file = "@//bazel/java:jdk_macos.BUILD",
        sha256 = "b6be6a9568be83695ec6b7cb977f4902f7be47d74494c290bc2a5c3c951e254f",
        strip_prefix = "jdk-21.0.3+9",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.3%2B9/OpenJDK21U-jdk_aarch64_mac_hotspot_21.0.3_9.tar.gz"],
    )

def _adoptium_jdk_macos_aarch64_repo():
    remote_java_repository(
        name = "adoptium_jdk_macos_aarch64_repo",
        sha256 = "b6be6a9568be83695ec6b7cb977f4902f7be47d74494c290bc2a5c3c951e254f",
        strip_prefix = "jdk-21.0.3+9/Contents/Home",
        urls = ["https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.3%2B9/OpenJDK21U-jdk_aarch64_mac_hotspot_21.0.3_9.tar.gz"],
        target_compatible_with = [
            "@platforms//os:macos",
            "@platforms//cpu:aarch64",
        ],
        prefix = "adoptium",
        version = "21",
    )

def _test_deps():
    http_file(
        name = "english_dictionary",
        sha256 = "5bef207ef2a954ce7df13cee21e8a181ac6a6f9ccb663a0671fc42dc86fc68c0",
        urls = ["https://raw.githubusercontent.com/dwyl/english-words/54b470a763d3df98ec33cb049382711972975317/words.txt"],
    )

def _mongot_java_artifacts():
    # Gather all of the artifacts we depend on besides gRPC.
    artifacts_without_grpc = GUAVA_ARTIFACTS + \
                             JACKSON_ARTIFACTS + \
                             METRICS_ARTIFACTS + \
                             OPEN_TELEMETRY_ARTIFACTS + \
                             MISC_ARTIFACTS + \
                             PINNED_TRANSITIVE_ARTIFACTS + \
                             as_test_only(TEST_ONLY_ARTIFACTS) + \
                             as_neverlink(COMPILE_ONLY_ARTIFACTS) + \
                             SYSTEMS_DEPS + \
                             SEARCH_QUERY_DEPS

    # Strip the versions of the artifacts we depend on.
    versionless_artifacts = [
        _artifact_without_version(artifact)
        for artifact in artifacts_without_grpc
    ]

    # Find the artifacts that gRPC requires that we have not already required ourselves.
    additional_grpc_artifacts = [
        artifact
        for artifact in IO_GRPC_GRPC_JAVA_ARTIFACTS
        if _artifact_without_version(artifact) not in versionless_artifacts
    ]

    # Use our explicitly defined artifacts as well as any additional ones gRPC requires.
    return artifacts_without_grpc + additional_grpc_artifacts

def _mongot_java_excluded_artifacts():
    return [
        # bcpkix-jdk18on is a transitive dependency from docker-java that we do not want to include
        # because -jdk18on is not approved for use in gov.
        "org.bouncycastle:bcpkix-jdk18on",
        # Default SFL4J binding used by checkstyle, but we can't allow it on the classpath for
        # mongot as we require logback for structured logging.
        "org.slf4j:slf4j-simple",
        # Exclude vulnerable okhttp 2.x (CVE-2021-0341) - the project uses okhttp3 instead.
        # See CLOUDP-378237 for details.
        "com.squareup.okhttp:okhttp",
    ]

def _artifact_without_version(artifact):
    """
    Returns the tuple (group_id, artifact) of the given maven artifact or coordinate string.
    """
    if type(artifact) == "string":
        return artifact.split(":")[:2]
    else:
        return [artifact["group"], artifact["artifact"]]
