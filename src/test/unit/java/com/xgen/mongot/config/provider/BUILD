load("//bazel/java:test.bzl", "java_test_suite")

java_test_suite(
    name = "provider",
    srcs = ["CommonUtilsTest.java"],
    tags = ["unit"],
    deps = [
        "//src/main/java/com/xgen/mongot/catalog",
        "//src/main/java/com/xgen/mongot/config/manager",
        "//src/main/java/com/xgen/mongot/config/provider",
        "//src/main/java/com/xgen/mongot/cursor",
        "//src/main/java/com/xgen/mongot/embedding/config",
        "//src/main/java/com/xgen/mongot/embedding/mongodb/leasing",
        "//src/main/java/com/xgen/mongot/embedding/providers",
        "//src/main/java/com/xgen/mongot/featureflag",
        "//src/main/java/com/xgen/mongot/metrics",
        "//src/main/java/com/xgen/mongot/monitor",
        "//src/main/java/com/xgen/mongot/replication/mongodb",
        "//src/main/java/com/xgen/mongot/replication/mongodb/common",
        "//src/main/java/com/xgen/mongot/replication/mongodb/initialsync/config",
        "//src/main/java/com/xgen/mongot/util/mongodb",
        "@maven//:com_google_guava_guava",
        "@maven//:io_micrometer_micrometer_core",
        "@maven//:junit_junit",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mongodb_mongodb_driver_core",
    ],
)
