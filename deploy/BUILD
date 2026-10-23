load("@bazel_skylib//lib:selects.bzl", "selects")
load("@rules_pkg//pkg:mappings.bzl", "pkg_attributes", "pkg_files")
load(":def.bzl", "package_deploy_tar", "pkg_var", "set_version")

# Sourced by every launcher, so it ships next to each of them in every bundle.
exports_files(
    ["bc-fips-env.sh"],
    visibility = ["//deploy:__subpackages__"],
)

# The two anchoring roots distrusted by CLOUDP-466523. Matches the scope
# of the SRE host remediation (chef nds-common::ca-trust blocklists exactly these two).
DISTRUSTED_ROOT_CNS = [
    "AAA Certificate Services",
    "emSign ECC Root CA - G3",
]

# Strip the distrusted roots from the bundled-JDK trust store. cacerts is
# platform-independent data, byte-identical across the linux/macos tarballs of a given
# Temurin release (sha256-verified for 21.0.3+9 and 21.0.11+10), so each JDK family is
# sanitized once from its linux_x86_64 repo and packaged into every platform's bundle.
# The JDK pkg_files exclude the stock cacerts; package_deploy_tar ships this copy in its
# place. keytool runs on the exec platform so cross-building linux artifacts works.
[
    genrule(
        name = "sanitized-cacerts-" + family,
        srcs = [repo + "//:cacerts"],
        outs = [
            "sanitized-cacerts-" + family + "/cacerts",
            "sanitized-cacerts-" + family + "/report.tsv",
        ],
        cmd = "$(location :strip_distrusted_roots.sh) $(JAVABASE)/bin/keytool $(SRCS) $(OUTS) " +
              " ".join(["'{}'".format(cn) for cn in DISTRUSTED_ROOT_CNS]),
        toolchains = ["@bazel_tools//tools/jdk:current_host_java_runtime"],
        tools = [
            ":strip_distrusted_roots.sh",
            "@bazel_tools//tools/jdk:current_host_java_runtime",
        ],
    )
    for family, repo in [
        ("atlas", "@adoptium_jdk_linux_x86_64"),
        ("community", "@adoptium_jdk_community_linux_x86_64"),
    ]
]

# Place the sanitized store at the bundled JDK's cacerts path. Release bundles follow the
# target platform's JDK layout; docker bundles are always the linux layout.
[
    pkg_files(
        name = "sanitized-cacerts-{}-{}-pkg".format(family, kind),
        srcs = [":sanitized-cacerts-" + family + "/cacerts"],
        attributes = pkg_attributes(mode = "0644"),
        prefix = select({
            "//bazel/platforms:is_macos_aarch64": "jdk/Contents/Home/lib/security",
            "//bazel/platforms:is_macos_x86_64": "jdk/Contents/Home/lib/security",
            "//conditions:default": "jdk/lib/security",
        }) if kind == "release" else "jdk/lib/security",
        renames = {":sanitized-cacerts-" + family + "/cacerts": "cacerts"},
        visibility = ["//visibility:public"],
    )
    for family in [
        "atlas",
        "community",
    ]
    for kind in [
        "release",
        "docker",
    ]
]

sh_test(
    name = "distrusted_roots_stripped_test",
    size = "small",
    srcs = ["distrusted_roots_stripped_test.sh"],
    args = ["$(location :sanitized-cacerts-atlas/report.tsv)"],
    data = [":sanitized-cacerts-atlas/report.tsv"],
    tags = ["unit"],
)

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
    docker_jdk_cacerts = ":sanitized-cacerts-community-docker-pkg",
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
    target_jdk_cacerts = ":sanitized-cacerts-community-release-pkg",
    toplevel = [
        ":community-resources/config.default.yml",
        ":community-resources/mongot",
        ":community-resources/README.md",
        ":community-resources/mongot.example.logrotate",
        ":bc-fips-env.sh",
        ":generate_community_version_file",
    ],
    with_tags = False,
)
