load("@contrib_rules_jvm//java:defs.bzl", _java_library = "java_library")
load("@rules_jvm_external//:defs.bzl", "artifact")

def java_library(name, plugins = [], javacopts = [], testonly = False, **kwargs):
    visibility = kwargs.pop("visibility", ["//visibility:public"])

    plugins = plugins + ["//bazel/java:autoservice_annotation_processor"]
    plugins = plugins + ["//src/main/java/com/xgen/errorprone:unsafe_collectors_plugin"]

    if not testonly:
        plugins = plugins + ["//bazel/java:nullaway_annotation_processor"]

    _java_library(
        name = name,
        plugins = plugins,
        visibility = visibility,
        javacopts = javacopts,
        testonly = testonly,
        **kwargs
    )

def mongot_java_package(name = "lib", deps = [], data = [], plugins = [], exports = [], testonly = False):
    deps = deps + [
        "org.slf4j:slf4j-api",
        "com.google.errorprone:error_prone_annotations",
        "org.jetbrains:annotations",
        "com.google.code.findbugs:jsr305",
        "com.google.guava:guava",
        "io.micrometer:micrometer-core",
        "org.apache.commons:commons-collections4",
        "org.apache.commons:commons-lang3",
        "org.apache.commons:commons-math3",
        "org.apache.lucene:lucene-analysis-common",
        "org.apache.lucene:lucene-core",
        "org.apache.lucene:lucene-expressions",
        "org.apache.lucene:lucene-join",
        "org.apache.lucene:lucene-queries",
        "org.apache.lucene:lucene-queryparser",
        "org.apache.lucene:lucene-sandbox",
        "org.mongodb:bson",
    ]
    deps = {d: d for d in deps}.keys()
    deps = _transform_deps(deps)
    deps = depset(deps).to_list()

    native.filegroup(
        name = "srcs",
        srcs = native.glob(["*.java"]),
        visibility = ["//visibility:public"],
    )
    plugins = plugins + ["//src/main/java/com/xgen/errorprone:unsafe_collectors_plugin"]
    plugins = plugins + ["//bazel/java:autoservice_annotation_processor"]

    if not testonly:
        plugins = plugins + ["//bazel/java:nullaway_annotation_processor"]

    _java_library(
        name = name,
        srcs = [":srcs"],
        deps = deps,
        exports = exports,
        data = data,
        plugins = plugins,
        visibility = ["//visibility:public"],
        testonly = testonly,
    )

def _transform_deps(deps):
    transformed = []
    for dep in deps:
        if _looks_like_absolute_label(dep):
            # Honor absolute labels above all else.
            transformed.append(dep)
        elif _looks_like_internal_package(dep):
            transformed.append(dep)
        else:
            # Otherwise assume that it's an external dependency.
            transformed.append(artifact(dep))

    return transformed

def _looks_like_absolute_label(dep):
    return dep.startswith("@") or dep.startswith("//") or dep.startswith(":")

def _looks_like_internal_package(dep):
    return dep.startswith("com.xgen")

def java_binary_stamped_manifest(name, manifest_lines = [], visibility = [], **kwargs):
    """
        Same as java_binary rule but also has a manifest_lines to allow stamping over values
    """

    native.java_binary(
        name = name + "__non_stamped",
        visibility = visibility,
        **kwargs
    )

    native.genrule(
        name = name,
        stamp = 1,
        srcs = [name + "__non_stamped_deploy.jar"],
        cmd = "\n".join(
            [
                # Ensure pipelines fail if any command in the pipe fails (e.g. unzip -p).
                "set -o pipefail",
                # Getting stamped values into vars.sh.
                "sed 's| |=\\\"|' < bazel-out/stable-status.txt | sed 's|$$|\\\"|' | sed 's|^|export |' > vars.sh",
                "sed 's| |=\\\"|' < bazel-out/volatile-status.txt | sed 's|$$|\\\"|' | sed 's|^|export |' >> vars.sh",
                "source vars.sh",
                "export INPUT=\"$$PWD/$<\"",  # points to "{name}__non_stamped_deploy.jar" location
                "export OUTPUT=\"$$PWD/$@\"",  # points to "{name}_deploy.jar" location
                # Copy the jar and make it writable so we can modify it in-place.
                # This avoids extracting and re-zipping the entire jar (61K+ files)
                # just to update the manifest, which previously took ~70s.
                "cp $$INPUT $$OUTPUT",
                "chmod u+w $$OUTPUT",
                # Extract just the manifest (via pipe, no disk extraction), strip empty
                # lines, and append the stamped manifest lines.
                "mkdir -p tmp/META-INF",
                "unzip -q -p $$OUTPUT META-INF/MANIFEST.MF | grep -v '^\\s*$$' > tmp/META-INF/MANIFEST.MF",
            ] + [
                "echo \"" + s + "\" >> tmp/META-INF/MANIFEST.MF"
                for s in manifest_lines
            ] + [
                # Replace the manifest entry in the jar without touching other entries.
                "zip -d -q $$OUTPUT META-INF/MANIFEST.MF",
                "(cd tmp && zip -q $$OUTPUT META-INF/MANIFEST.MF)",
                "rm -rf tmp vars.sh",
            ],
        ),
        outs = [name + "_deploy.jar"],
        visibility = visibility,
    )
