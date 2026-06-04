load("@bazel_skylib//lib:selects.bzl", "selects")
load(":def.bzl", "package_deploy_tar", "pkg_var", "set_version")

pkg_var(
    name = "release_pkg_vars",
    platform = select({
        "//bazel/platforms:is_linux_aarch64": "linux_aarch64",
        "//bazel/platforms:is_linux_x86_64": "linux_x86_64",
        "//bazel/platforms:is_macos_aarch64": "macos_aarch64",
        "//bazel/platforms:is_macos_x86_64": "macos_x86_64",
    }),
    version = "//bazel/config:version",
    visibility = [":__subpackages__"],
)

alias(
    name = "release_target_jdk",
    actual = selects.with_or({
        "//bazel/platforms:is_linux_aarch64": "@adoptium_jdk_linux_aarch64//:jdk",
        "//bazel/platforms:is_linux_x86_64": "@adoptium_jdk_linux_x86_64//:jdk",
        "//bazel/platforms:is_macos_aarch64": "@adoptium_jdk_macos_aarch64//:jdk",
        "//bazel/platforms:is_macos_x86_64": "@adoptium_jdk_macos_x86_64//:jdk",
    }),
    visibility = ["//visibility:public"],
)

alias(
    name = "docker_target_jdk",
    actual = select({
        "@platforms//cpu:aarch64": "@adoptium_jdk_linux_aarch64//:jdk",
        "@platforms//cpu:x86_64": "@adoptium_jdk_linux_x86_64//:jdk",
    }),
    visibility = ["//visibility:public"],
)

alias(
    name = "release_community_target_jdk",
    actual = selects.with_or({
        "//bazel/platforms:is_linux_aarch64": "@adoptium_jdk_community_linux_aarch64//:jdk",
        "//bazel/platforms:is_linux_x86_64": "@adoptium_jdk_community_linux_x86_64//:jdk",
        "//bazel/platforms:is_macos_aarch64": "@adoptium_jdk_community_macos_aarch64//:jdk",
        "//bazel/platforms:is_macos_x86_64": "@adoptium_jdk_community_macos_x86_64//:jdk",
    }),
    visibility = ["//visibility:public"],
)

alias(
    name = "docker_community_target_jdk",
    actual = select({
        "@platforms//cpu:aarch64": "@adoptium_jdk_community_linux_aarch64//:jdk",
        "@platforms//cpu:x86_64": "@adoptium_jdk_community_linux_x86_64//:jdk",
    }),
    visibility = ["//visibility:public"],
)

set_version(
    name = "community_release_version",
    version = "//bazel/config:version",
)

genrule(
    name = "generate_community_version_file",
    outs = ["VERSION.txt"],
    cmd = "echo MongoDb Search Community Version $(VERSION) > $@",
    toolchains = [":community_release_version"],
    visibility = ["//visibility:public"],
)

package_deploy_tar(
    name = "mongot-community",
    bin = [
        "//:mongot_community_deploy.jar",
    ],
    docker_jdk = "//deploy:docker_community_target_jdk",
    lib = [
        "//:fips-jars",
    ] + select({
        # mongot_vecsim has no CC toolchain on macOS x86_64: exclude it there and
        # rely on the JVM-side Similarity loader, which degrades gracefully when
        # the library is absent.
        "//bazel/platforms:is_macos_x86_64": [],
        "//conditions:default": ["//src/main/c:mongot_vecsim"],
    }),
    license = [
        ":community-resources/LICENSE/LICENSE",
        ":community-resources/LICENSE/THIRD_PARTY_NOTICES.txt",
    ],
    target_jdk = "//deploy:release_community_target_jdk",
    toplevel = [
        ":community-resources/config.default.yml",
        ":community-resources/mongot",
        ":community-resources/README.md",
        ":community-resources/mongot.example.logrotate",
        ":generate_community_version_file",
    ],
    with_tags = False,
)
