load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "vector",
    srcs = [
        "Lucene99NativeHnswVectorsFormat.java",
        "Lucene99NativeMemorySegmentVectorScorer.java",
        "Similarity.java",
    ],
    # mongot_vecsim has no CC toolchain on macOS x86_64; Similarity degrades gracefully when absent.
    data = select({
        "//bazel/platforms:is_macos_x86_64": [],
        "//conditions:default": ["//src/main/c:mongot_vecsim"],
    }),
    javacopts = ["--enable-preview"],
    visibility = ["//:__subpackages__"],
    exports = ["@maven//:org_apache_lucene_lucene_core"],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "@maven//:com_google_auto_service_auto_service_annotations",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
