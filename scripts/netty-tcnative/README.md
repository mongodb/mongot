# Building netty-tcnative with Dynamic OpenSSL Linking

> **DEPRECATED (CLOUDP-413451):** As of netty-tcnative **2.0.80.Final**, mongot uses the standard
> Maven Central `*-fedora` artifacts (which dynamically link the system OpenSSL 3.x) for both Atlas
> and community, on x86_64 and aarch64. The custom-built S3 JAR described below is **no longer used**,
> and this directory (and `netty_tcnative_deps()`) is removed in a follow-up cleanup PR. Do not
> follow these instructions for new updates.

This directory contains scripts to build `netty-tcnative` from source with dynamic OpenSSL linking.
The built JAR is uploaded to S3 and downloaded by Bazel at build time.

## Background

The Maven Central version of `netty-tcnative` does not dynamically link to the system's OpenSSL
library on AL2023 systems. For AL2023 x86_64 builds, we need to package our own jar that
properly links to the system OpenSSL.

## Building and Publishing

### Run the Build Script

```bash
./scripts/netty-tcnative/build-and-publish.sh 2.0.71.Final --aws-profile <your-profile>
```

Replace `2.0.71.Final` with the desired netty-tcnative version. This should match the
`NETTY_TCNATIVE_VERSION` in `bazel/java/netty_tcnative.bzl`.

The script will:

1. Build the JAR in a Docker container
2. Upload the JAR to S3 (`s3://search-build-and-release-3p-tools/netty-tcnative/`)
3. Update `bazel/java/netty_tcnative.bzl` with the new version and SHA256

### Options

- `--aws-profile <profile>`: AWS profile to use for S3 upload
- `--dry-run`: Preview what would be uploaded/updated without making changes
- `--skip-build`: Skip Docker build and use existing JAR in output directory

### Commit Changes

After building, commit the updated file:

```bash
git add bazel/java/netty_tcnative.bzl
git commit -m "Update netty-tcnative AL2023-compatible JAR to version X.Y.Z"
```

## When to Rebuild

Rebuild when:

- Upgrading the netty-tcnative version
- Changing OpenSSL compatibility requirements
- Adding support for new platforms

## S3 Bucket Configuration

JARs are stored in `s3://search-build-and-release-3p-tools/netty-tcnative/`. The bucket is
configured with:

- **Versioning**: Enabled. Re-publishing the same key creates a new version rather than overwriting,
  preserving the history of published JARs.
- **Object Lock (Governance mode, 3-year default retention)**: Enabled at the bucket level. Every
  uploaded JAR is automatically protected against deletion for 3 years from its upload date. The
  retention period can be extended before expiry if the JAR is still in use.

## How It Works

### Build Process

1. `build-and-publish.sh` builds a Docker image from `Dockerfile`
2. The Docker container runs `build.sh` which:
	- Clones netty-tcnative from GitHub at the specified version
	- Builds the `openssl-dynamic` module with Maven
	- Produces a JAR with the native `.so` file
3. The JAR is uploaded to S3
4. `bazel/java/netty_tcnative.bzl` is updated with the new version and SHA256

### Bazel Integration

The Bazel build bundles **both** AL2 and AL2023 tcnative libraries for x86_64, enabling a single
deployment artifact that works on both OS versions. Runtime selection happens in Java code.

- **`bazel/java/netty_tcnative.bzl`**: Defines `NETTY_TCNATIVE_VERSION`,
  `NETTY_TCNATIVE_AL2023_X86_64_SHA256`, `NETTY_TCNATIVE_MAVEN_ARTIFACTS`, and target constants:
	- `NETTY_TCNATIVE_X86_64_AL2_TARGET`: Points to Maven Central JAR (OpenSSL 1.x)
	- `NETTY_TCNATIVE_X86_64_AL2023_TARGET`: Points to S3 JAR (OpenSSL 3.x)
	- `NETTY_TCNATIVE_AARCH64_TARGET`: Points to Maven Central JAR
	- Also defines the `http_file` rule that downloads the AL2023 JAR from S3.
	- This file is swapped with `bazel/java/netty_tcnative-Community.bzl` by copybara for open
	  source
	  builds (Community version points both AL2 and AL2023 targets to the same Maven JAR).
- **`bazel/java/BUILD`**: Defines the `netty-tcnative` alias for runtime dependency resolution
- **`bazel/atlas/java/BUILD`**: Defines:
	- `netty-tcnative-openssl-dynamic-linux-x86_64` (`java_import` for AL2023 S3 JAR)
	- `netty-tcnative-al2-linux-x86_64` (alias to Maven JAR for AL2)
- **`BUILD` (root)**: Uses `extracted_libraries` to extract both libraries with unique names:
	- `libnetty_tcnative_x86_64_al2.so` (from Maven JAR)
	- `libnetty_tcnative_x86_64_al2023.so` (from S3 JAR)

### Library Bundling

| Platform | Product   | Libraries Bundled                                                        |
|----------|-----------|--------------------------------------------------------------------------|
| x86_64   | Atlas     | `libnetty_tcnative_x86_64_al2.so` + `libnetty_tcnative_x86_64_al2023.so` |
| x86_64   | Community | Two identical files (both from Maven JAR)                                |
| aarch64  | Any       | `libnetty_tcnative_linux_aarch_64.so` (single library)                   |

### Runtime Selection

At runtime, `TcNativeLoader.java` (in `com.xgen.atlas.util`) detects the OS version by reading
`/etc/os-release` and preloads the appropriate library using `System.load()`:

- AL2023 (`VERSION_ID="2023"`): Loads `libnetty_tcnative_x86_64_al2023.so`
- AL2 or other: Loads `libnetty_tcnative_x86_64_al2.so`
- Fallback: If renamed libraries aren't found, falls back to default Netty loading behavior

This is called early in `Mongot.run()` after the FIPS security provider is installed, before any
SSL context is created.

## Testing tcnative on AL2 and AL2023

To verify the single-artifact tcnative works correctly on both OS versions, use the built-in
Makefile target. This builds mongot once (bundling both libraries) and runs it on the specified OS.

### Prerequisites

Start the other containers (mongod, etc.) first with `make docker.up`, then stop any existing mongot
container and build and start the new one, replacing BASE_OS with the desired OS:

```bash
docker rm -f docker-mongot1-1

BASE_OS="amazon_linux_2" BUILD_OPTS="--platforms=//bazel/platforms:linux_x86_64 --//bazel/config:base_docker_image_os=$BASE_OS" bazel run //src/test/setup/java/com/xgen/mongot/setup/docker:Docker -- build-images;
DOCKER_DEFAULT_PLATFORM=linux/amd64 docker compose -f docker/docker-compose-single8.0.yml up -d mongot1;
```

### Verifying the right netty-tcnative JAR is Used

Once the cluster is running, you can verify that mongot is using the dynamically-linked OpenSSL:
On AL2023, you should see a log message like this:
{"t":"2026-02-24T06:19:54.144+0000","s":"INFO","svc":"MONGOT","ctx":"main",
"n":"com.xgen.mongot.util.mongodb.MongoClientBuilder","msg":"Dynamically linked to OpenSSL version",
"attr":{"openSslVersion":"OpenSSL 3.2.2 4 Jun 2024",
"applicationName":"Optional[mongot database metadata resolver]"}}

On AL2, you should see a log message like this:
{"t":"2026-03-09T23:26:43.084+0000","s":"INFO","svc":"MONGOT","ctx":"main",
"n":"com.xgen.mongot.util.mongodb.MongoClientBuilder","msg":"Dynamically linked to OpenSSL version",
"attr":{"openSslVersion":"OpenSSL 1.0.1e-fips 11 Feb 2013",
"applicationName":"Optional[mongot database metadata resolver]"}}

On non-x86_64 arch, you should see a line that says:
"Skipping OpenSSL linking on an unsupported platform"

## Files

| File                   | Purpose                                                                      |
|------------------------|------------------------------------------------------------------------------|
| `Dockerfile`           | Build environment with all dependencies                                      |
| `build.sh`             | Builds netty-tcnative inside Docker                                          |
| `build-and-publish.sh` | Orchestrates build, uploads to S3, and updates bazel/java/netty_tcnative.bzl |
| `README.md`            | This file                                                                    |
