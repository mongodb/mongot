load("@rules_java//java:java_test.bzl", "java_test")

java_test(
    name = "GenBsonProtoTest",
    srcs = ["GenBsonProtoTest.java"],
    deps = [
        "//src/main/java/com/xgen/mongot/util",
        "//src/main/java/com/xgen/proto",
        "//src/proto/bson:types_java_proto",
        "//src/test/proto:proto3_java_proto",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:org_mongodb_bson",
    ],
)
