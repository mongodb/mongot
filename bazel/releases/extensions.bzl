"""Community variant of bazel/releases/extensions.bzl.

The Community variant of release_deps() is a no-op (see deps-Community.bzl),
but the extension wrapper is preserved so the same use_extension call works.
"""

load("//bazel/releases:deps.bzl", "release_deps")

def _impl(_mctx):
    release_deps()

releases = module_extension(implementation = _impl)
