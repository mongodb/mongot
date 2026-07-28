load("@bazel_gazelle//:def.bzl", "gazelle", "gazelle_binary", "gazelle_test")
load("@rules_java//java:defs.bzl", "java_package_configuration", "java_plugin", "java_runtime")
load("@rules_java//toolchains:default_java_toolchain.bzl", "default_java_toolchain")
load("@rules_jvm_external//:defs.bzl", "artifact")
load("//bazel/java:package.bzl", "java_library")

java_plugin(
    name = "nullaway_annotation_processor",
    visibility = ["//visibility:public"],
    deps = [
        artifact("com.uber.nullaway:nullaway"),
        artifact("com.google.guava:guava"),
        "//src/main/java/com/xgen/testing/errorprone",
    ],
)

java_plugin(
    name = "jmh_annotation_processor",
    testonly = True,
    processor_class = "org.openjdk.jmh.generators.BenchmarkProcessor",
    visibility = ["//visibility:public"],
    deps = [artifact("org.openjdk.jmh:jmh-generator-annprocess")],
)

java_plugin(
    name = "autoservice_annotation_processor",
    processor_class = "com.google.auto.service.processor.AutoServiceProcessor",
    visibility = ["//visibility:public"],
    deps = [artifact("com.google.auto.service:auto-service")],
)

java_runtime(
    name = "jdk21_macos_x86_64",
    srcs = ["@adoptium_jdk_macos_x86_64//:srcs"],
    java = "@adoptium_jdk_macos_x86_64//:java",
    visibility = ["//visibility:public"],
)

java_runtime(
    name = "jdk21_macos_aarch64",
    srcs = ["@adoptium_jdk_macos_aarch64//:srcs"],
    java = "@adoptium_jdk_macos_aarch64//:java",
    visibility = ["//visibility:public"],
)

java_runtime(
    name = "jdk21_linux_x86_64",
    srcs = ["@adoptium_jdk_linux_x86_64//:srcs"],
    java = "@adoptium_jdk_linux_x86_64//:java",
    visibility = ["//visibility:public"],
)

java_runtime(
    name = "jdk21_linux_aarch64",
    srcs = ["@adoptium_jdk_linux_aarch64//:srcs"],
    java = "@adoptium_jdk_linux_aarch64//:java",
    visibility = ["//visibility:public"],
)

# netty-tcnative target for runtime dependency resolution.
# Exports both the platform-specific native jar and the netty-tcnative-classes jar,
# which provides the io.netty.internal.tcnative.* Java bridge needed by the OpenSSL provider.
java_library(
    name = "netty-tcnative",
    visibility = ["//visibility:public"],
    exports = [
        "@maven//:io_netty_netty_tcnative_classes",
    ] + select({
        "//bazel/platforms:is_linux_aarch64": ["@maven//:io_netty_netty_tcnative_linux_aarch_64_fedora"],
        # We assume all local testing that will end up using tcnative on Apple Silicon will be
        # done through docker, so we say to use the Linux version for the docker image.
        "//bazel/platforms:is_macos_aarch64": ["@maven//:io_netty_netty_tcnative_linux_aarch_64_fedora"],
        "//conditions:default": ["@maven//:io_netty_netty_tcnative_linux_x86_64_fedora"],
    }),
)

alias(
    name = "netty-transport-native-epoll",
    actual = select({
        "//bazel/platforms:is_linux_aarch64": "@maven//:io_netty_netty_transport_native_epoll_linux_aarch_64",
        # We assume all local testing that will end up using epoll on Apple Silicon will be
        # done through docker, so we say to use the Linux version for the docker image.
        "//bazel/platforms:is_macos_aarch64": "@maven//:io_netty_netty_transport_native_epoll_linux_aarch_64",
        "//conditions:default": "@maven//:io_netty_netty_transport_native_epoll_linux_x86_64",
    }),
    visibility = ["//visibility:public"],
)

alias(
    name = "netty-transport-native-kqueue",
    actual = select({
        "//conditions:default": "@maven//:io_netty_netty_transport_native_kqueue_osx_aarch_64",
    }),
    visibility = ["//visibility:public"],
)

default_java_toolchain(
    name = "mongot_java21_toolchain_config",
    package_configuration = [
        ":nullaway_package_config",
        ":extra_error_prone_check_package_config",
        ":test_package_config",
        ":testing_utilities_package_config",
        ":mongot_javac_warnings_config",
    ],
    source_version = "21",
    target_version = "21",
    visibility = ["//visibility:public"],
)

java_package_configuration(
    name = "mongot_javac_warnings_config",
    javacopts = [
        # Enable all javac linting by default
        "-Xlint:all",
        # disable warnings that relate to bazel's "optimized" compile-time classpath
        "-Xlint:-classfile",
        # disable warnings that there are no annotation processors for an annotation
        "-Xlint:-processing",
        # disable warnings about not providing serialVersionUID (usually Exception classes)
        "-Xlint:-serial",
        # disable warnings about try-with-resources, usually about spans and timers.
        "-Xlint:-try",
        # disable warnings about fallthrough, since there is a more robust Errorprone check
        "-Xlint:-fallthrough",
    ],
    packages = [":warnings_packages"],
)

package_group(
    name = "warnings_packages",
    packages = [
        "//src/...",
    ],
)

_NULLAWAY_WHITELISTED_METHODS = [
    "java.util.concurrent.atomic.AtomicReference.get",  # NullAway doesn't support annotations on type parameters yet
    "java.util.Map.get",  # Temporarily disabled because it requires a large number of changes
]

java_package_configuration(
    name = "nullaway_package_config",
    javacopts = [
        "-Xep:NullAway:ERROR",
        "-XepOpt:NullAway:CheckOptionalEmptiness=false",  # Temporary
        "-XepOpt:NullAway:IgnoreLibraryModelsFor=" + ",".join(_NULLAWAY_WHITELISTED_METHODS),
        "-XepOpt:NullAway:AnnotatedPackages=com.xgen,io.micrometer.core",
        "-XepOpt:NullAway:UnannotatedSubPackages=com.xgen.mongot.index.lucene.query.sort.mixed,com.google.common," +  # Temporary
        "com.xgen.atlas.index.vectorlite.ffm",
        "-XepOpt:NullAway:ExcludedFieldAnnotations=picocli.CommandLine.Option,picocli.CommandLine.ArgGroup",
    ],
    packages = [":nullaway_packages"],
)

package_group(
    name = "nullaway_packages",
    packages = [
        "//src/main/...",
    ],
)

_EXTRA_CHECKS = [
    "AlreadyChecked",
    "AnnotateFormatMethod",
    "AssertEqualsArgumentOrderChecker",
    "AssertFalse",
    "BigDecimalLiteralDouble",
    "ClassCanBeStatic",
    "ConstantField",
    "ConstantPatternCompile",
    "CompareToZero",
    "DateFormatConstant",
    "EqualsBrokenForNull",
    "EqualsHashCode",
    "FallThrough",
    "FieldCanBeFinal",
    "Finally",
    "FloggerArgumentToString",
    "FloggerStringConcatenation",
    "GetClassOnEnum",
    "InconsistentHashCode",
    "InitializeInline",
    "ImmutableEnumChecker",
    "IterablePathParameter",
    "JavaDurationGetSecondsGetNano",
    "JavaDurationWithNanos",
    "JavaDurationWithSeconds",
    "JavaInstantGetSecondsGetNano",
    "JavaLocalDateTimeGetNano",
    "JavaLocalTimeGetNano",
    "JavaPeriodGetDays",
    "JavaUtilDate",
    "LockNotBeforeTry",
    "LockOnBoxedPrimitive",
    "LogicalAssignment",
    "MissingCasesInEnumSwitch",
    "MissingDefault",
    "ModifyCollectionInEnhancedForLoop",
    "ModifySourceCollectionInStream",
    "MultimapKeys",
    "MutableConstantField",
    "MutablePublicArray",
    "NullOptional",
    "NumericEquality",
    "RemoveUnusedImports",
    "StaticQualifiedUsingExpression",
    "StringEquality",
    "StringFormatWithLiteral",
    "StreamResourceLeak",
    "ThreadSafe",
    "TryWithResourcesVariable",
    "TruthContainsExactlyElementsInUsage",
    "TruthAssertExpected",
    "UnnecessaryAsync",
    "UnnecessaryBoxedVariable",
    "UnnecessaryLambda",
    "UnnecessaryParentheses",
    "UnnecessaryStringBuilder",
    "UnnecessaryDefaultInEnumSwitch",
    "UnsafeCollectors",  # Custom check for unsafe Collectors.toMap() usage
    "UnusedVariable",
    "UnusedMethod",
    "Var",  # requires @Var annotation on non constants
]

# These checks were included in _EXTRA_CHECKS but were not actually supported in
# errorprone 2.4 or were introduced/made default since 2.4.
# Temporarily disabling them until they can be fixed.
_TEMPORARILY_DISABLED_CHECKS = [
    "JavaUtilDate",  # CLOUDP-122885
]

java_package_configuration(
    name = "extra_error_prone_check_package_config",
    javacopts = ["-Xep:{}:ERROR".format(check) for check in _EXTRA_CHECKS] +
                ["-Xep:{}:OFF".format(check) for check in _TEMPORARILY_DISABLED_CHECKS] + [
        "-XDshould-stop.ifError=FLOW",  # Required for Var
    ],
    packages = [":extra_error_prone_check_packages"],
)

# TODO: come up with a better way to exclude generated proto code from additional error prone
# checks. Moving protos to a handful of high level packages might be the best option.
package_group(
    name = "extra_error_prone_check_packages",
    packages = [
        "-//src/main/java/com/xgen/proto/...",
        "-//src/main/java/com/xgen/testing/...",
        "-//src/test/bench/...",
        "-//src/test/mock/proto/...",
        "-//src/test/proto/...",
        "-//src/test/setup/proto/...",
        "//src/main/...",
        "//src/test/...",
    ],
)

# These checks are ignored in test code
_TESTS_DISABLED_CHECKS = [
    "PreferredInterfaceType",  # Many classes have an Iterable or Collection of test cases
    "UnsafeCollectors",  # Allow unsafe collectors in test code
]

# These checks are ignored in testing utilities
_TESTING_UTILITIES_DISABLED_CHECKS = [
    "UnsafeCollectors",  # Allow unsafe collectors in testing utilities
]

java_package_configuration(
    name = "test_package_config",
    javacopts = ["-Xep:{}:OFF".format(check) for check in _TESTS_DISABLED_CHECKS],
    packages = [":test_package"],
)

java_package_configuration(
    name = "testing_utilities_package_config",
    javacopts = ["-Xep:{}:OFF".format(check) for check in _TESTING_UTILITIES_DISABLED_CHECKS],
    packages = [":testing_utilities_package"],
)

package_group(
    name = "test_package",
    packages = [
        "//src/test/...",
    ],
)

package_group(
    name = "testing_utilities_package",
    packages = [
        "//src/main/java/com/xgen/testing/...",
    ],
)

toolchain(
    name = "mongot_java21_toolchain",
    toolchain = ":mongot_java21_toolchain_config",
    toolchain_type = "@bazel_tools//tools/jdk:toolchain_type",
)

gazelle(
    name = "gazelle",
    gazelle = ":gazelle_bin",
)

gazelle_binary(
    name = "gazelle_bin",
    languages = [
        "@contrib_rules_jvm//java/gazelle",
    ],
)

# If this tests fails run `bazel run //bazel/java:gazelle` to apply any necessary changes.
gazelle_test(
    name = "gazelle_test",
    gazelle = ":gazelle_bin",
    tags = ["lint"],
    workspace = "//:BUILD",
)
