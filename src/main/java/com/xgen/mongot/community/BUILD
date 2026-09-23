load("@rules_java//java:defs.bzl", "java_binary")
load("//bazel/java:package.bzl", "java_library")

java_library(
    name = "community",
    srcs = [
        "MongotCommunity.java",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/main/java/com/xgen/mongot/config/provider/community",
        "//src/main/java/com/xgen/mongot/featureflag",
        "//src/main/java/com/xgen/mongot/featureflag/dynamic",
        "//src/main/java/com/xgen/mongot/index/lucene/vector",
        "//src/main/java/com/xgen/mongot/logging",
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/mongot/util/security",
        "@maven//:com_google_guava_guava",
        "@maven//:info_picocli_picocli",
        "@maven//:org_slf4j_slf4j_api",
    ],
)

java_binary(
    name = "MongotCommunity",
    main_class = "com.xgen.mongot.community.MongotCommunity",
    visibility = ["//visibility:public"],
    runtime_deps = [":community"],
)
