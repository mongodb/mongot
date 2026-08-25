load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "backup",
    srcs = [
        "ConfigJournalV1.java",
        "DynamicFeatureFlagJournal.java",
        "DynamicFeatureFlagJournalWriter.java",
        "JournalEditor.java",
        "JournalEntry.java",
        "JournalEntryIdentity.java",
        "JournalWriter.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/config/util",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/version",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/config/util",
        "//src/main/java/com/xgen/mongot/featureflag/dynamic",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/version",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/bson",
        "//src/main/java/com/xgen/mongot/util/bson/parser",
        "@maven//:com_google_guava_guava",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_mongodb_bson",
    ],
)
