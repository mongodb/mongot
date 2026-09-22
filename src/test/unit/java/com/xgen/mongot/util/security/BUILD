load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "security",
    srcs = [
        "AesGcmCipherTest.java",
        "PemCertificateBundleTest.java",
        "SecurityTest.java",
    ],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/util/security",
        "@maven//:com_google_truth_truth",
        "@maven//:junit_junit",
    ],
)
