import os
import re
import sys

named_rules = {
    "mongot_java_package(",
    "mongot_java_unit_test_suite(",
    "mongot_java_e2e_test(",
    "mongot_java_e2e_test_suite(",
    "perf_java_unit_test_suite(",
    "perf_java_integration_test_suite(",
    "mongot_java_integration_test(",
    "mongot_java_integration_test_suite(",
    "mongot_java_bench_suite(",
    "mongot_java_fuzz_test_suite(",
}


def normalize(line):
    """
    Transforms internal dependency strings from using dotted path format to //src/main/java/...
    If a lib name is found in the original path, it will be preserved, else ":lib" will be appended

    :param line: any line of text from a BUILD file
    :return:
    """
    # Look for dependency in format "com.xgen.foo.bar" or "com.xgen.foo:bar"
    match = re.match(r'"(?P<package>com\.xgen\.([a-z]+\.)*[a-z]+)(?P<lib>:[a-z]+)?",', line.strip())
    if match:
        # Preserve indentation
        indent = line[: len(line) - len(line.lstrip())]
        matches = match.groupdict()
        lib = matches.get("lib") or ":lib"
        package = match.group("package").replace(".", "/")
        return '%s"//src/main/java/%s%s",\n' % (indent, package, lib)
    return line


def main():
    """
    Recursively iterates over all files in the specified directory tree, and ensures the following
    hold for every BUILD file:
      1) All recognized rules have an explicit name as their first parameter
      2) All java rules use standard bazel syntax for dependencies, i.e. //src/main/java/...:lib

    This is required for intellij to automatically update dependencies names when
     renaming our java packages.
      usage:
      python3 scripts/python/normalize_build.py . && make tools.buildifier.fix
    """
    directory = sys.argv[1]
    for path, dirs, files in os.walk(os.path.abspath(directory)):
        for filename in {"BUILD", "BUILD.bazel"}.intersection(files):
            filepath = os.path.join(path, filename)

            with open(filepath, "r") as f:
                lines = f.readlines()

            output = []
            expected_name = None
            for line in lines:
                # If first line of rule, insert name if not exists
                if expected_name:
                    if "name" not in line:
                        output.append('    name = "lib",\n')

                # See if beginning of new rule
                expected_name = line.strip() in named_rules
                output.append(normalize(line))

            with open(filepath, "w") as f:
                f.write("".join(output))


if __name__ == "__main__":
    main()
    os.system("make tools.buildifier.fix")
