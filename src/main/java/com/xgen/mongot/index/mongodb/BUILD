load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "mongodb",
    srcs = [
        "MaterializedViewMetricValuesSupplier.java",
        "MaterializedViewWriter.java",
    ],
    visibility = ["//:__subpackages__"],
    exports = [
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/status",
        "@maven//:org_mongodb_mongodb_driver_core",
        "@maven//:org_mongodb_mongodb_driver_sync",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/embedding/exceptions",
        "//src/main/java/com/xgen/mongot/embedding/mongodb/leasing",
        "//src/main/java/com/xgen/mongot/embedding/utils",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/status",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/util/concurrent",
        "//src/main/java/com/xgen/mongot/util/mongodb",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
        "@maven//:org_mongodb_mongodb_driver_sync",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
