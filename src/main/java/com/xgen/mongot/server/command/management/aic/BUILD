load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "aic",
    srcs = [
        "AicCreateSearchIndexesCommand.java",
        "AicDropSearchIndexCommand.java",
        "AicListSearchIndexesCommand.java",
        "AicManageSearchIndexCommandFactory.java",
        "AicUpdateSearchIndexCommand.java",
    ],
    visibility = ["//:__subpackages__"],
    exports = [
        "//src/main/java/com/xgen/mongot/catalogservice",
        "//src/main/java/com/xgen/mongot/server/command",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/catalogservice",
        "//src/main/java/com/xgen/mongot/config/manager",
        "//src/main/java/com/xgen/mongot/config/provider/community/embedding",
        "//src/main/java/com/xgen/mongot/config/util",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/server/command",
        "//src/main/java/com/xgen/mongot/server/command/management",
        "//src/main/java/com/xgen/mongot/server/command/management/definition",
        "//src/main/java/com/xgen/mongot/server/command/management/definition/common",
        "//src/main/java/com/xgen/mongot/server/command/management/util",
        "//src/main/java/com/xgen/mongot/server/message",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/mongodb",
        "@maven//:org_mongodb_bson",
        "@maven//:org_mongodb_mongodb_driver_core",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
