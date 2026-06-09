load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load(":def.bzl", "version")

string_flag(
    name = "base_docker_image_os",
    build_setting_default = "amazon_linux_2023",
    values = [
        "amazon_linux_2023",
        "amazon_linux_2",
    ],
    visibility = ["//visibility:public"],
)

config_setting(
    name = "amazon_linux_2023_docker_image",
    flag_values = {
        ":base_docker_image_os": "amazon_linux_2023",
    },
    visibility = ["//visibility:public"],
)

config_setting(
    name = "amazon_linux_2_docker_image",
    flag_values = {
        ":base_docker_image_os": "amazon_linux_2",
    },
    visibility = ["//visibility:public"],
)

string_flag(
    name = "server_binaries_source",
    build_setting_default = "latest_patch_release",
    values = [
        "last_green",
        "latest_patch_release",
        "evergreen_upload",
    ],
    visibility = ["//visibility:public"],
)

config_setting(
    name = "evergreen_upload_binaries_source",
    flag_values = {
        ":server_binaries_source": "evergreen_upload",
    },
)

config_setting(
    name = "al2_aarch64_last_green",
    constraint_values = [
        "@platforms//cpu:aarch64",
    ],
    flag_values = {
        ":base_docker_image_os": "amazon_linux_2",
        ":server_binaries_source": "last_green",
    },
)

config_setting(
    name = "al2023_aarch64_last_green",
    constraint_values = [
        "@platforms//cpu:aarch64",
    ],
    flag_values = {
        ":base_docker_image_os": "amazon_linux_2023",
        ":server_binaries_source": "last_green",
    },
)

config_setting(
    name = "aarch64_latest_patch_release",
    constraint_values = [
        "@platforms//cpu:aarch64",
    ],
    flag_values = {
        ":server_binaries_source": "latest_patch_release",
    },
)

config_setting(
    name = "al2_x86_64_last_green",
    constraint_values = [
        "@platforms//cpu:x86_64",
    ],
    flag_values = {
        ":base_docker_image_os": "amazon_linux_2",
        ":server_binaries_source": "last_green",
    },
)

config_setting(
    name = "al2023_x86_64_last_green",
    constraint_values = [
        "@platforms//cpu:x86_64",
    ],
    flag_values = {
        ":base_docker_image_os": "amazon_linux_2023",
        ":server_binaries_source": "last_green",
    },
)

config_setting(
    name = "x86_64_latest_patch_release",
    constraint_values = [
        "@platforms//cpu:x86_64",
    ],
    flag_values = {
        ":server_binaries_source": "latest_patch_release",
    },
)

# Version config (using build_setting to be able to specify arbitrary versions)
version(
    name = "version",
    build_setting_default = "local",
    visibility = ["//visibility:public"],
)
