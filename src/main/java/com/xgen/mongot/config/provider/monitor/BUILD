load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "monitor",
    srcs = ["PeriodicConfigMonitor.java"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/config/updater",
        "//src/main/java/com/xgen/mongot/logging",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/concurrent",
        "//src/main/java/com/xgen/mongot/util/retry",
        "@maven//:io_micrometer_micrometer_core",
    ],
)
