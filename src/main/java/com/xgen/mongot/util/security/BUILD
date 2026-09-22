load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "security",
    srcs = [
        "AesGcmCipher.java",
        "PemCertificateBundle.java",
        "Security.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:org_bouncycastle_bc_fips",
        "@maven//:org_bouncycastle_bctls_fips",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
