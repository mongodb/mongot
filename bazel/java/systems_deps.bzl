load("//bazel/java:dep_utils.bzl", "append_version")

_AWS_SDK_VERSION = "2.22.9"
_AWS_SDK_ARTIFACTS = (
    append_version(
        _AWS_SDK_VERSION,
        [
            "software.amazon.awssdk:s3",
            "software.amazon.awssdk:sdk-core",
            "software.amazon.awssdk:auth",
            "software.amazon.awssdk:regions",
            "software.amazon.awssdk:s3-transfer-manager",
            "software.amazon.awssdk:sts",
            "software.amazon.awssdk:secretsmanager",
        ],
    )
)
_AWS_CRT_VERSION = "0.29.5"
_AWS_CRT_ARTIFACTS = (
    append_version(
        _AWS_CRT_VERSION,
        [
            "software.amazon.awssdk.crt:aws-crt",
        ],
    )
)

_BOUNCY_CASTLE_ARTIFACTS = [
    "org.bouncycastle:bc-fips:2.0.1",
    "org.bouncycastle:bcpkix-fips:2.0.10",
    "org.bouncycastle:bctls-fips:2.0.22",
]

_AZURE = [
    "com.azure:azure-identity:1.17.0",
    "com.azure:azure-storage-blob:12.31.1",
    "com.azure:azure-storage-blob-batch:12.27.1",
]

_GCP = [
    "com.google.cloud:google-cloud-storage:2.59.0",
    "com.google.cloud:google-cloud-storage-control:2.59.0",
    "com.google.auth:google-auth-library-oauth2-http:1.40.0",
    "com.google.auth:google-auth-library-credentials:1.40.0",
    "com.google.cloud:google-cloud-nio:0.128.7",
]

_NETTY_VERSION = "4.1.130.Final"

# Note that matching tcnative version for Netty 4.1.130.Final is 2.0.74.Final.
# However, the linux-aarch_64-fedora artifact has been removed for that version,
# hence using 2.0.71.Final.
_NETTY_TCNATIVE_VERSION = "2.0.71.Final"
_NETTY_ARTIFACTS = (
    append_version(
        _NETTY_VERSION,
        [
            "io.netty:netty-buffer",
            "io.netty:netty-codec",
            "io.netty:netty-common",
            "io.netty:netty-transport",
            "io.netty:netty-transport-native-epoll:jar:linux-x86_64",
            "io.netty:netty-transport-native-epoll:jar:linux-aarch_64",
            "io.netty:netty-transport-native-kqueue:jar:osx-x86_64",
            "io.netty:netty-transport-native-kqueue:jar:osx-aarch_64",
        ],
    ) + append_version(_NETTY_TCNATIVE_VERSION, [
        "io.netty:netty-tcnative-boringssl-static",
        "io.netty:netty-tcnative-classes",
        "io.netty:netty-tcnative:jar:linux-aarch_64-fedora",
        "io.netty:netty-tcnative:jar:linux-x86_64-fedora",
    ])
)

MONGO_DRIVER_VERSION = "4.11.5"
_MONGO_DRIVER_ARTIFACTS = append_version(
    MONGO_DRIVER_VERSION,
    [
        "org.mongodb:mongodb-driver-sync",
    ],
)

SYSTEMS_DEPS = _AWS_CRT_ARTIFACTS + \
               _AWS_SDK_ARTIFACTS + \
               _BOUNCY_CASTLE_ARTIFACTS + \
               _AZURE + _NETTY_ARTIFACTS + \
               _GCP + \
               _MONGO_DRIVER_ARTIFACTS
