load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "comparator",
    srcs = [
        "BlockJoinReducedNumericDocValues.java",
        "BlockJoinReducedSortedDocValues.java",
        "BsonConverter.java",
        "DoubleFieldConverter.java",
        "EmbeddedScopedLongComparator.java",
        "FieldComparatorBsonWrapper.java",
        "MongotNumericComparator.java",
        "MongotUpdateableDocIdSetIterator.java",
        "MqlLongComparator.java",
    ],
    # Disable checks for these specific files as they're cloned from library
    javacopts = ["-XepExcludedPaths:.*/com.xgen.mongot.index.lucene.query.sort.comparator.(MongotNumericComparator|MongotUpdateableDocIdSetIterator).java"],
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_mongodb_bson",
    ],
    deps = [
        "//src/main/java/com/xgen/mongot/index/lucene/util",
        "//src/main/java/com/xgen/mongot/index/query/sort",
        "@maven//:com_google_errorprone_error_prone_annotations",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_lucene_lucene_core",
        "@maven//:org_apache_lucene_lucene_join",
        "@maven//:org_jetbrains_annotations",
        "@maven//:org_mongodb_bson",
    ],
)
