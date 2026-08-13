# Bazel dependency rules

After adding/removing/bumping any 3rd party dependencies in a `**/*MODULE.bazel` file, always re-pin
versions by running `make deps.update`. If you edit a `**/*.bzl` file, run `bazel mod tidy`

