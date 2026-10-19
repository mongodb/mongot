load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "synonym",
    srcs = [
        "LuceneSynonymMapBuilder.java",
        "LuceneSynonymRegistry.java",
        "SynonymAnalyzer.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/status",
        "//src/main/java/com/xgen/mongot/index/synonym",
        "@maven//:org_apache_lucene_lucene_core",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/analyzer",
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/status",
        "//src/main/java/com/xgen/mongot/index/synonym",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/regex",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_lucene_lucene_analysis_common",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_mongodb_bson",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
