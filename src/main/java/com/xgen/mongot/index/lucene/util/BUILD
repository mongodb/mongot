load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "util",
    srcs = [
        "AnalyzedText.java",
        "FieldTypeBuilder.java",
        "InvalidAnalyzerOutputException.java",
        "LuceneCodecUtils.java",
        "LuceneDocumentIdEncoder.java",
        "LuceneDoubleConversionUtils.java",
        "TopLevelDocValues.java",
        "VectorSearchUtil.java",
    ],
    javacopts = ["-Xep:LongDoubleConversion:ERROR"],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/util",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/definition",
        "//src/main/java/com/xgen/mongot/index/lucene/field",
        "//src/main/java/com/xgen/mongot/index/path/string",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/functionalinterfaces",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_mongodb_bson",
    ],
)
