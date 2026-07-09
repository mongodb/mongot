load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "optimizer",
    srcs = [
        "ClauseRewriteEngine.java",
        "ClauseRewriteRule.java",
        "OperatorRewriteEngine.java",
        "OperatorRewriteRule.java",
        "RewriteAudit.java",
        "RewriteContext.java",
        "RewriteRule.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/featureflag/dynamic",
        "//src/main/java/com/xgen/mongot/index/query/operators",
        "//src/main/java/com/xgen/mongot/index/query/operators/mql",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/featureflag/dynamic",
        "//src/main/java/com/xgen/mongot/index",
        "//src/main/java/com/xgen/mongot/index/lucene/query/context",
        "//src/main/java/com/xgen/mongot/index/query/operators",
        "//src/main/java/com/xgen/mongot/index/query/operators/mql",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_flogger_flogger",
        "@maven//:org_apache_lucene_lucene_core",
    ],
)
