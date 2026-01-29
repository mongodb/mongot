load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def tools_repos():
    _prometheus_linux_x86_64()
    _prometheus_linux_aarch64()
    _prometheus_macos_x86_64()
    _prometheus_macos_aarch64()

def _prometheus_linux_x86_64():
    http_archive(
        name = "prometheus_linux_x86_64",
        url = "https://github.com/prometheus/prometheus/releases/download/v2.54.0/prometheus-2.54.0.linux-amd64.tar.gz",
        sha256 = "465e1393a0cca9705598f6ffaf96ffa78d0347808ab21386b0c6aaec2cf7aa13",
        strip_prefix = "prometheus-2.54.0.linux-amd64",
        build_file_content = """
filegroup(
    name = "bin",
    srcs = ["promtool"],
    visibility = ["//visibility:public"],
)
""",
    )

def _prometheus_linux_aarch64():
    http_archive(
        name = "prometheus_linux_aarch64",
        url = "https://github.com/prometheus/prometheus/releases/download/v2.54.0/prometheus-2.54.0.linux-arm64.tar.gz",
        sha256 = "ed50b67cb833a225ec2a53b487c6e20372b20e56dce226423fa8611c8aa50392",
        strip_prefix = "prometheus-2.54.0.linux-arm64",
        build_file_content = """
filegroup(
    name = "bin",
    srcs = ["promtool"],
    visibility = ["//visibility:public"],
)
""",
    )

def _prometheus_macos_x86_64():
    http_archive(
        name = "prometheus_macos_x86_64",
        url = "https://github.com/prometheus/prometheus/releases/download/v2.54.0/prometheus-2.54.0.darwin-amd64.tar.gz",
        sha256 = "ca4caee10bfd114adcffe8c23b80e53973be4a7c2666cd5a182a601f0eac2295",
        strip_prefix = "prometheus-2.54.0.darwin-amd64",
        build_file_content = """
filegroup(
    name = "bin",
    srcs = ["promtool"],
    visibility = ["//visibility:public"],
)
""",
    )

def _prometheus_macos_aarch64():
    http_archive(
        name = "prometheus_macos_aarch64",
        url = "https://github.com/prometheus/prometheus/releases/download/v2.54.0/prometheus-2.54.0.darwin-arm64.tar.gz",
        sha256 = "875db6df65636d047b6aea3cfac56f4a0e2deb325232fb87bda2383d0330f033",
        strip_prefix = "prometheus-2.54.0.darwin-arm64",
        build_file_content = """
filegroup(
    name = "bin",
    srcs = ["promtool"],
    visibility = ["//visibility:public"],
)
""",
    )
