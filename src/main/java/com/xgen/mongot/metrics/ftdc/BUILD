load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "ftdc",
    srcs = [
        "DocumentType.java",
        "Ftdc.java",
        "FtdcCollector.java",
        "FtdcCompressor.java",
        "FtdcConfig.java",
        "FtdcDecoder.java",
        "FtdcFileManager.java",
        "FtdcFileType.java",
        "FtdcMetadata.java",
        "FtdcScheduledReporter.java",
        "LongPacker.java",
        "MetricChunk.java",
        "ZlibUtil.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:org_mongodb_bson",
        "@maven//:org_slf4j_slf4j_api",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/metrics/micrometer",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/mongot/util/concurrent",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_flogger_flogger",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:org_apache_commons_commons_lang3",
        "@maven//:org_mongodb_bson",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
