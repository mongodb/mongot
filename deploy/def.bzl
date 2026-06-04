load("@rules_pkg//pkg:pkg.bzl", "pkg_tar")
load("@rules_pkg//pkg:providers.bzl", "PackageVariablesInfo")
load("//bazel/config:def.bzl", "VersionInfo")

def package_deploy_tar(name, bin, lib, toplevel = [], with_tags = True, license = [], target_jdk = "//deploy:release_target_jdk", docker_jdk = "//deploy:docker_target_jdk"):
    """
    Makes two deployable bundles:
      - One named <name>, whose directory structure is stamped using the value of the
        version config flag. For example, for name=mongot, version=1.0.0 and platform=linux_aarch64,
        this would create a target mongot with the following directory layout.
          - If with_tags is true, the version and platform information will be appended to the
            directory name, else just the name will be used.
          - Note the `toplevel` labels are located directly in the <name> directory, while the `bin`
            and `lib` labels are located in corresponding subdirectories.
          ├── mongot_1.0.0_linux_aarch64
          │   ├── toplevel label 1
          │   ├── toplevel label 2 ...
          │   ├── bin
          │   │   ├── jdk
          │   │   ├── profile.sh
          │   │   ├── report_oom_to_mms.sh
          │   │   ├── mongot_deploy.jar
          │   │   ├── mongot.sh
          │   │   └── mongot
          │   └── lib         (if lib_jar provided)
          │       └── lib_jar (if lib_jar provided)

      - The other named <name>-local. This is the same as the bundle above, except that
        the version is "local". This is useful for targets that want to be able to rely on this
        bundle having a consistent directory layout, such as docker images.
    """

    pkg_tar(
        name = "{}-bin".format(name),
        srcs = bin + [target_jdk],
        package_dir = "bin",
    )

    pkg_tar(
        name = "{}-lib".format(name),
        srcs = lib,
        package_dir = "lib",
    )

    release_deps = [
        ":{}-bin".format(name),
        ":{}-lib".format(name),
    ]

    if license:
        pkg_tar(
            name = "{}-LICENSE".format(name),
            srcs = license,
            package_dir = "LICENSE",
        )
        release_deps.append(":{}-LICENSE".format(name))

    # This is the artifact that is properly formatted and packaged for releases.
    pkg_tar(
        name = name,
        # Compress, since it will be uploaded to s3 from evergreen.
        extension = "tgz",
        package_dir = name + "_{version}_{platform}" if with_tags else name,
        package_variables = "//deploy:release_pkg_vars",
        srcs = toplevel,
        deps = release_deps,
        visibility = ["//visibility:public"],
    )

    pkg_tar(
        name = "{}-bin-docker".format(name),
        srcs = bin + [docker_jdk],
        package_dir = "bin",
    )

    # This is an artifact purpose built for local docker images.
    # Don't compress this one, compression is time consuming (~20-30s) and does not help for local docker images.
    pkg_tar(
        name = "{}-local-docker".format(name),
        package_dir = name,
        srcs = toplevel,
        deps = [
            ":{}-bin-docker".format(name),
            ":{}-lib".format(name),
        ],
        visibility = ["//visibility:public"],
    )

"""
Rule which allows to use config flag values in the pkg_tar package_variables
"""

def _pkg_var_impl(ctx):
    return PackageVariablesInfo(values = {
        "platform": ctx.attr.platform,
        "version": ctx.attr.version[VersionInfo].type,
    })

pkg_var = rule(
    implementation = _pkg_var_impl,
    attrs = {
        "platform": attr.string(),
        "version": attr.label(),
    },
)

"""
Rule which allows genrules to set the VERSION variable value from a label.
"""

def _set_version_impl(ctx):
    return platform_common.TemplateVariableInfo({
        "VERSION": ctx.attr.version[VersionInfo].type,
    })

set_version = rule(
    implementation = _set_version_impl,
    attrs = {
        "version": attr.label(),
    },
)

def extract_library(out, paths, lib):
    native.genrule(
        name = "extract_" + out,
        srcs = [lib],
        outs = [out],
        cmd = "unzip -p $(location " + lib + ") " + select(paths) + " > $@",
    )

"""
config format:
{
    "<library name>": {
        "src": "<library src label>",
        "outputs": {
            "<filename>": {
                "<platform>": "<path>",
            },
        },
        "properties": ["<system property>"],
    },
}
"""

def extracted_libraries(config):
    libs_by_platform = {}
    properties = []
    for name, cfg in config.items():
        for output, platform_to_path in cfg["outputs"].items():
            paths = {"//conditions:default": ":nothing"}
            for platform, path in platform_to_path.items():
                paths["//bazel/platforms:is_{}".format(platform)] = path
                libs_by_platform.setdefault(platform, []).append(":extract_{}".format(output))
            extract_library(output, paths, cfg["src"])
            properties += cfg.get("properties", [])

    for platform, srcs in libs_by_platform.items():
        native.filegroup(
            name = "{}_extracted".format(platform),
            srcs = srcs,
        )

    platform_check = {"//bazel/platforms:is_{}".format(platform): ":{}_extracted".format(platform) for platform in libs_by_platform.keys()}
    platform_check["//conditions:default"] = ":nothing"
    native.alias(
        name = "extracted-deps",
        actual = select(platform_check),
        visibility = ["//visibility:public"],
    )

    native.genrule(
        name = "extracted-deps-properties",
        outs = ["extracted-deps.properties"],
        cmd = "echo \"{}\" > $@".format("\n".join(properties)),
        visibility = ["//visibility:public"],
    )
