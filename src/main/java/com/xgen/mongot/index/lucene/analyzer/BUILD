load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "analyzer",
    srcs = [
        "AnalysisStep.java",
        "BoundedPatternReplaceFilter.java",
        "BoundedPatternTokenizer.java",
        "CustomAnalyzer.java",
        "CustomAnalyzerSpecification.java",
        "LuceneAnalyzerFactory.java",
        "SnowballTokenFilterFactory.java",
    ],
    visibility = ["//visibility:public"],
    exports = [
        "//src/main/java/com/xgen/mongot/index/analyzer/custom",
        "@maven//:org_apache_lucene_lucene_analysis_common",
        "@maven//:org_apache_lucene_lucene_core",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/analyzer/custom",
        "//src/main/java/com/xgen/mongot/util/regex",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:org_apache_lucene_lucene_analysis_common",
        "@maven//:org_apache_lucene_lucene_analysis_icu",
        "@maven//:org_apache_lucene_lucene_analysis_phonetic",
        "@maven//:org_apache_lucene_lucene_analysis_stempel",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_jetbrains_annotations",
    ],
)
