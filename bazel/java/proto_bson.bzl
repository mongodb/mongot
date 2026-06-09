"""Build rule to build protos with Bson extensions.

This is cribbed from java_grpc_library().
"""

# TODO: this does not function like java_proto_library(), to do some we would need to implement an
# aspect so that we generate bson proto compilation of all the libraries in between.

load("@com_google_protobuf//bazel/common:proto_info.bzl", "ProtoInfo")
load("@rules_java//java/common:java_common.bzl", "java_common")
load("@rules_java//java/common:java_info.bzl", "JavaInfo")

_JavaProtoBsonToolchainInfo = provider(
    fields = [
        "java_toolchain",
        "plugin",
        "protoc",
        "runtime",
    ],
)

def _java_proto_bson_toolchain_impl(ctx):
    return [
        _JavaProtoBsonToolchainInfo(
            java_toolchain = ctx.attr._java_toolchain,
            plugin = ctx.attr.plugin,
            protoc = ctx.executable._protoc,
            runtime = ctx.attr.runtime,
        ),
        platform_common.ToolchainInfo(),  # Magic for b/78647825
    ]

java_proto_bson_toolchain = rule(
    attrs = {
        "plugin": attr.label(
            cfg = "exec",
            executable = True,
        ),
        # This attribute has a "magic" name recognized by the native DexArchiveAspect (b/78647825).
        "runtime": attr.label_list(
            cfg = "target",
            providers = [JavaInfo],
        ),
        "_java_toolchain": attr.label(
            default = Label("@bazel_tools//tools/jdk:current_java_toolchain"),
        ),
        "_protoc": attr.label(
            cfg = "exec",
            default = Label("@com_google_protobuf//:protoc"),
            executable = True,
        ),
    },
    provides = [
        _JavaProtoBsonToolchainInfo,
        platform_common.ToolchainInfo,
    ],
    implementation = _java_proto_bson_toolchain_impl,
)

# "repository" here is for Bazel builds that span multiple WORKSPACES.
def _path_ignoring_repository(f):
    # Bazel creates a _virtual_imports directory in case the .proto source files
    # need to be accessed at a path that's different from their source path:
    # https://github.com/bazelbuild/bazel/blob/0.27.1/src/main/java/com/google/devtools/build/lib/rules/proto/ProtoCommon.java#L289
    #
    # In that case, the import path of the .proto file is the path relative to
    # the virtual imports directory of the rule in question.
    virtual_imports = "/_virtual_imports/"
    if virtual_imports in f.path:
        return f.path.split(virtual_imports)[1].split("/", 1)[1]
    elif len(f.owner.workspace_root) == 0:
        # |f| is in the main repository
        return f.short_path
    else:
        # If |f| is a generated file, it will have "bazel-out/*/genfiles" prefix
        # before "external/workspace", so we need to add the starting index of "external/workspace"
        return f.path[f.path.find(f.owner.workspace_root) + len(f.owner.workspace_root) + 1:]

def _java_bson_proto_library_impl(ctx):
    toolchain = ctx.attr._toolchain[_JavaProtoBsonToolchainInfo]
    srcs = []
    descriptor_depsets = []
    for src in ctx.attr.srcs:
        srcs.extend(src[ProtoInfo].direct_sources)
        descriptor_depsets.append(src[ProtoInfo].transitive_descriptor_sets)

    descriptor_depset = depset(transitive = descriptor_depsets)

    srcjar = ctx.actions.declare_file("%s-proto-gensrc.jar" % ctx.label.name)

    # Specify --java_out followed by --bson_java_out to invoke the plugin on the same files.
    args = ctx.actions.args()
    args.add("--plugin=protoc-gen-bson_java={0}".format(toolchain.plugin.files_to_run.executable.path))
    args.add("--java_out={0}".format(srcjar.path))
    args.add("--bson_java_out={0}".format(srcjar.path))
    args.add_joined("--descriptor_set_in", descriptor_depset.to_list(), join_with = ctx.configuration.host_path_separator)
    args.add_all(srcs, map_each = _path_ignoring_repository)

    ctx.actions.run(
        inputs = depset(srcs, transitive = [descriptor_depset]),
        outputs = [srcjar],
        executable = toolchain.protoc,
        arguments = [args],
        use_default_shell_env = True,
        tools = [toolchain.plugin.files_to_run],
    )

    java_info = java_common.compile(
        ctx,
        java_toolchain = toolchain.java_toolchain[java_common.JavaToolchainInfo],
        source_jars = [srcjar],
        output = ctx.outputs.jar,
        output_source_jar = ctx.outputs.srcjar,
        deps = [dep[JavaInfo] for dep in toolchain.runtime] + [dep[JavaInfo] for dep in ctx.attr.deps],
    )

    return [java_info]

_java_bson_proto_library = rule(
    attrs = {
        "deps": attr.label_list(
            mandatory = False,
            allow_empty = True,
            # TODO: return a marker provider from this rule and _require_ it for all deps.
            providers = [JavaInfo],
        ),
        "srcs": attr.label_list(
            mandatory = True,
            allow_empty = False,
            providers = [ProtoInfo],
        ),
        "_toolchain": attr.label(
            default = Label("//src/main/java/com/xgen/proto/plugin:java_proto_bson_library_toolchain"),
        ),
    },
    toolchains = ["@bazel_tools//tools/jdk:toolchain_type"],
    fragments = ["java"],
    outputs = {
        "jar": "lib%{name}.jar",
        "srcjar": "lib%{name}-src.jar",
    },
    provides = [JavaInfo],
    implementation = _java_bson_proto_library_impl,
)

def java_bson_proto_library(
        name,
        srcs,
        deps = [],
        **kwargs):
    """Generates Java protobuf code with Bson extensions for protos in a `proto_library`.

    This rule is intended to replace uses of `java_proto_library`. It is not quite a drop-in
    replacement as both the source `proto_library` and `java_bson_proto_library` dependencies need
    to be specified.

    TODO: implement an Aspect so that this works more like `java_proto_library`.

    Args:
      name: A unique name for this rule.
      srcs: (List of `labels`) a list of proto_library labels.
      deps: (List of `labels`) a list of java_bson_proto_library labels.
      **kwargs: Other common attributes
    """
    _java_bson_proto_library(name = name, srcs = srcs, deps = deps, **kwargs)
