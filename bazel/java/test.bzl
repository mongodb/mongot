load("@contrib_rules_jvm//java:defs.bzl", _java_test_suite = "java_test_suite")
load("@rules_java//java:java_library.bzl", "java_library")
load("@rules_java//java:java_test.bzl", "java_test")
load("@rules_jvm_external//:defs.bzl", "artifact")
load(":package.bzl", "mongot_java_package")

# Maven coordinates resolved as test-only deps for every java test suite.
# The strings are converted to @maven//... labels by mongot_java_package._transform_deps.
# Versions are pinned via the testonly maven.artifact tags in bazel/java/maven.MODULE.bazel,
# so they may be omitted here.
TEST_ONLY_ARTIFACTS = [
    "org.openjdk.jmh:jmh-core",
    "org.openjdk.jmh:jmh-generator-annprocess",
    "com.google.guava:guava-testlib",
    "com.pholser:junit-quickcheck-core",
    "com.pholser:junit-quickcheck-generators",
    "com.github.docker-java:docker-java-core",
    "com.github.docker-java:docker-java-api",
    "com.github.docker-java:docker-java-transport",
    "com.github.docker-java:docker-java-transport-httpclient5",
    "io.projectreactor:reactor-test",
    "com.googlecode.junit-toolbox:junit-toolbox",
    "org.hamcrest:hamcrest-library",
    "org.hamcrest:hamcrest-core",
    "junit:junit",
    "org.mockito:mockito-subclass",
    "com.google.truth:truth",
    "com.googlecode.json-simple:json-simple",
    "com.puppycrawl.tools:checkstyle",
    "com.github.spotbugs:spotbugs",
]

MOCKITO_CORE_LIB = "@maven//:org_mockito_mockito_core"
MOCKITO_SUBCLASS_LIB = "@maven//:org_mockito_mockito_subclass"

def java_test_suite(name, srcs, deps, tags, jvm_flags = [], **kwargs):
    """A thin wrapper around contrib_rules_jvm java_test_suite

    The enforces certain style constraints within the mongot code base.
    * Error if a test source begins with 'Test'. Older tests begin this way but java_test_suite
      requires suffixes to indicate if a class should generate a test rule.
    * Introduce an additional dependency for tests that depend on mockito that cannot be derived
      directly by gazelle.
    * Require tags to be set. CI uses tag filters and tests without tags may not be run at all.
    """
    for src in srcs:
        if src.startswith("Test"):
            fail("Test suite sources may not start with 'test': {src}")

    # Add the mockito subclass lib if mockito is used. This dependency cannot be inferred at compile
    # time by gazelle but is needed for ByteBuddy on linux builds.
    if MOCKITO_CORE_LIB in deps and MOCKITO_SUBCLASS_LIB not in deps:
        deps.append("@maven//:org_mockito_mockito_subclass")

    # This is required to use FFM preview features in Java 21.
    # It can be removed when we upgrade to Java 22+.
    jvm_flags = jvm_flags + ["--enable-preview"]

    _java_test_suite(
        name = name,
        srcs = srcs,
        deps = deps,
        tags = tags,
        jvm_flags = jvm_flags,
        **kwargs
    )

def mongot_java_test_resources():
    native.exports_files(native.glob(["*"]))
    native.filegroup(
        name = "srcs",
        srcs = native.glob(["*"]),
        visibility = ["//visibility:public"],
    )

    # Add dummy java library so resource files work with Intellij's 'Select Opened File'
    java_library(
        name = "dummy_resources_for_intellij_indexing",
        srcs = [],  # no Java sources needed
        resources = [":srcs"],
        visibility = ["//visibility:private"],
    )

def mongot_java_unit_test(name, test_class, size = "small", timeout = "short", tags = [], data = [], **kwargs):
    java_test(
        name = name,
        tags = tags + ["unit"],
        size = size,
        timeout = timeout,
        test_class = test_class,
        data = data,
        **kwargs
    )

def mongot_java_unit_test_suite(name, deps = [], resources = False, exclude_files = [], sizes = {}, timeouts = {}, test_module = "mongot"):
    _mongot_java_test_suite(
        name,
        "unit",
        mongot_java_unit_test,
        package_suffix_strip = "",
        test_module = test_module,
        deps = deps,
        resources = resources,
        exclude_files = exclude_files,
        sizes = sizes,
        timeouts = timeouts,
    )

def perf_java_unit_test_suite(name, deps = [], resources = False, data = [], exclude_files = [], sizes =
                                                                                                     {}, timeouts = {}):
    _mongot_java_test_suite(
        name,
        "perf.unit",
        mongot_java_unit_test,
        package_suffix_strip = "",
        test_module = "perf",
        deps = deps,
        resources = resources,
        data = data,
        exclude_files = exclude_files,
        sizes = sizes,
        timeouts = timeouts,
    )

def perf_java_integration_test_suite(
        name,
        deps = [],
        resources = False,
        data = [],
        exclude_files = [],
        sizes = {},
        timeouts = {}):
    _mongot_java_test_suite(
        name,
        "perf.integration",
        mongot_java_integration_test,
        package_suffix_strip = "",
        test_module = "perf",
        deps = deps,
        resources = resources,
        data = data,
        exclude_files = exclude_files,
        sizes = sizes,
        timeouts = timeouts,
    )

def mongot_java_integration_test(size = "small", timeout = "short", tags = [], **kwargs):
    final_tags = tags if "integration_community_sharded" in tags else tags + ["integration"]
    java_test(
        tags = final_tags,
        size = size,
        timeout = timeout,
        **kwargs
    )

def mongot_java_integration_test_suite(
        name,
        deps = [],
        resources = False,
        data = [],
        exclude_files = [],
        sizes = {},
        timeouts = {},
        default_tags = [],
        tags = {},
        **kwargs):
    _mongot_java_test_suite(
        name,
        "integration",
        mongot_java_integration_test,
        package_suffix_strip = "",
        deps = deps,
        resources = resources,
        data = data,
        exclude_files = exclude_files,
        sizes = sizes,
        timeouts = timeouts,
        default_tags = default_tags,
        tags = tags,
        **kwargs
    )

def mongot_java_e2e_test(size = "small", timeout = "short", tags = [], **kwargs):
    java_test(
        tags = tags + ["e2e", "exclusive"],
        size = size,
        timeout = timeout,
        **kwargs
    )

def smear_hash(input_string):
    h = hash(input_string)  # Get the built-in hash value
    h = h ^ ((h >> 20) ^ (h >> 12))  # Smearing higher bits
    h = h ^ ((h >> 7) ^ (h >> 4))  # Smearing lower bits
    return h

def mongot_java_e2e_test_suite(
        name,
        deps = [],
        resources = False,
        data = [],
        exclude_files = [],
        sizes = {},
        timeouts = {},
        default_tags = [],
        tags = {},
        jvm_flags = {},
        classpath_resources = []):
    for test_file in native.glob(["*.java"]):
        if test_file in exclude_files:
            continue
        if smear_hash(test_file) % 2 == 0:
            tags = tags | {test_file: (["e2e1"] + tags.get(test_file, []))}
        else:
            tags = tags | {test_file: (["e2e2"] + tags.get(test_file, []))}
    _mongot_java_test_suite(
        name,
        "e2e",
        mongot_java_e2e_test,
        package_suffix_strip = "/e2e",
        deps = deps,
        resources = resources,
        data = data + ["//docker:default-mms-config.json"],
        exclude_files = exclude_files,
        sizes = sizes,
        timeouts = timeouts,
        default_tags = default_tags + ["exclusive"],
        tags = tags,
        jvm_flags = jvm_flags,
        classpath_resources = classpath_resources,
    )

def mongot_java_fuzz_test(size = "small", timeout = "short", tags = [], **kwargs):
    java_test(
        tags = tags + ["fuzz"],
        size = size,
        timeout = timeout,
        **kwargs
    )

def mongot_java_fuzz_test_suite(name, deps = [], resources = False, exclude_files = [], sizes = {}, timeouts = {}):
    _mongot_java_test_suite(
        name,
        "fuzz",
        mongot_java_fuzz_test,
        package_suffix_strip = "/fuzz",
        deps = deps,
        resources = resources,
        exclude_files = exclude_files,
        sizes = sizes,
        timeouts = timeouts,
    )

def mongot_java_bench_suite(name, deps = [], exclude_files = [], resources = False, profile_gc = True):
    # Create a package with the dependencies that can be used by the individual benchmarks.
    mongot_java_package(
        name,
        deps = deps + [artifact("org.openjdk.jmh:jmh-core")],
        plugins = ["//bazel/java:jmh_annotation_processor"],
        testonly = True,
        javacopts = ["--enable-preview"],
    )

    # Create a java_binary for each file in the package not explicitly excluded.
    for bench_file in native.glob(["*.java"]):
        if bench_file in exclude_files:
            continue

        # Give the test the name of the file without the java extension.
        bench_name = bench_file[:-len(".java")]

        test_data = _get_test_data_path("mongot") if resources else []
        java_test(
            name = bench_name,
            main_class = "org.openjdk.jmh.Main",
            args = ["-prof gc", bench_name] if profile_gc else [],
            runtime_deps = [":lib"],
            classpath_resources = ["//conf/mongot/test:conf"],
            visibility = ["//visibility:public"],
            data = test_data,
            jvm_flags = ["--enable-preview"],
        )

def _mongot_java_test_suite(
        name,
        test_type,
        test_macro,
        package_suffix_strip,
        test_module = "mongot",
        deps = [],
        resources = False,
        data = [],
        exclude_files = [],
        sizes = {},
        timeouts = {},
        default_tags = [],
        tags = {},
        jvm_flags = {},
        classpath_resources = [],
        **kwargs):
    deps = deps + TEST_ONLY_ARTIFACTS + ["org.mockito:mockito-core"]

    # Create a package with the dependencies that can be used by the individual tests.
    mongot_java_package(name = name, deps = deps, testonly = True)

    if not "//conf/mongot/test:conf" in classpath_resources:
        classpath_resources = ["//conf/mongot/test:conf"] + classpath_resources

    # Create a java_test for each file in the package not explicitly excluded.
    for test_file in native.glob(["*.java"]):
        if test_file in exclude_files:
            continue

        # Give the test the name of the file without the java extension.
        test_name = test_file[:-len(".java")]
        test_class = _get_test_class(test_file, test_type)
        test_data = _get_test_data_path(test_module, package_suffix_strip) if resources else []
        test_data += data

        if test_file in sizes:
            kwargs["size"] = sizes[test_file]
        if test_file in timeouts:
            kwargs["timeout"] = timeouts[test_file]

        # Always include --enable-preview for FFM support; it can be removed when we upgrade to Java 22+.
        file_jvm_flags = jvm_flags.get(test_file, []) + ["--enable-preview"]
        kwargs["jvm_flags"] = file_jvm_flags

        test_tags = default_tags + tags.get(test_file, [])

        # Invoke the proper macro to create the actual test.
        test_macro(
            name = test_name,
            test_class = test_class,
            visibility = ["//visibility:public"],
            data = test_data,
            runtime_deps = [":lib"],
            classpath_resources = classpath_resources,
            tags = test_tags,
            testonly = True,
            **kwargs
        )

def _get_test_class(test_file, test_type):
    # Gets something like "src/test/unit/java/com/xgen/mongot/cursor/TestMongotCursorManagerImpl.java"
    test_class_path = native.package_name() + "/" + test_file

    # Convert to "src.test.unit.java.com.xgen.mongot.cursor.TestMongotCursorManagerImpl.java"
    test_class = test_class_path.replace("/", ".")

    # Strip off "src.test.unit.java."
    package_prefix = "src.test." + test_type + ".java."
    test_class = test_class[len(package_prefix):]

    # Strip off ".java"
    test_class = test_class[:-len(".java")]
    return test_class

def _get_test_data_path(test_module, package_suffix_strip = ""):
    # Gets something like "src/test/unit/java/com/xgen/mongot/cursor"
    test_path = native.package_name()

    # Convert to something like "src/test/unit/resources/cursor"
    test_data_path = test_path.replace("/java/com/xgen/" + test_module +
                                       package_suffix_strip, "/resources")

    # Assumes that the path has a BUILD file with mongot_test_resources()
    return ["//" + test_data_path + ":srcs"]
