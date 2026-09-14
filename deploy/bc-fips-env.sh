#!/bin/bash

# Shared BouncyCastle FIPS native library setup, sourced by every mongot launcher.
#
# BouncyCastle FIPS ships its native crypto libraries inside bc-fips.jar and extracts them to disk
# at startup before dlopen()ing them. It always extracts into java.io.tmpdir unless
# org.bouncycastle.native.loader.install_dir points somewhere else. When that directory is mounted
# noexec the dlopen fails with an UnsatisfiedLinkError, which BouncyCastle does not catch because
# its handler only covers Exception, so mongot dies during startup.

# These files cannot be bundled within the tar as well. BouncyCastle demands sole ownership of this directory:
# it silently falls back to its Java implementation if the directory holds any file
# it did not install itself, so the contents must never be pre-staged at build time.
#
# Usage:
#   source "${DIR}/bc-fips-env.sh"
#   bc_fips_configure "${DIR}/lib"
#   "${JAVA_HOME}/bin/java" ... ${BC_FIPS_FLAGS[@]+"${BC_FIPS_FLAGS[@]}"} ...

# The directory BouncyCastle extracts into by default. The JVM hardcodes java.io.tmpdir to /tmp on
# Linux and ignores TMPDIR, so this deliberately does not consult the environment.
BC_FIPS_DEFAULT_TMPDIR="/tmp"

# Reports whether the existing directory $1 sits on a filesystem that permits execution.
#
# This reads the mount table rather than writing and running a probe script, so it leaves nothing
# behind on disk. /proc/self/mountinfo reports each mount's mount point in field 5 and that mount's
# own options in field 6. The mount that governs a path is the one whose mount point is the longest
# prefix of it, and among equal-length mount points the last one listed, since later entries are
# stacked over earlier ones.
#
# noexec gates both execve and the PROT_EXEC mmap that dlopen needs, so it is exactly the property
# BouncyCastle depends on. When the mount table cannot be read the answer is unknown and this
# reports failure: every fallback below is safe on a filesystem that does allow execution, whereas
# a wrong "yes" crashes mongot.
bc_fips_allows_exec() {
  local resolved
  local mount_point options
  local best_point="" best_options=""

  # Resolve symlinks and '..' so the prefix comparison below is meaningful.
  resolved="$(CDPATH='' cd -P -- "$1" 2>/dev/null && pwd)" || return 1
  [[ -n "${resolved}" ]] || return 1

  [[ -r /proc/self/mountinfo ]] || return 1

  while read -r _ _ _ _ mount_point options _; do
    [[ "${resolved}" == "${mount_point}" || "${resolved}" == "${mount_point%/}/"* ]] || continue
    if (( ${#mount_point} >= ${#best_point} )); then
      best_point="${mount_point}"
      best_options="${options}"
    fi
  done < /proc/self/mountinfo

  [[ -n "${best_point}" ]] || return 1
  [[ ",${best_options}," != *",noexec,"* ]]
}

# Selects where BouncyCastle should extract and load its native crypto libraries from, and sets
# BC_FIPS_FLAGS to the JVM flags expressing that choice. $1 is the deployment's lib directory.
#
# Only one configuration is actually dangerous: pointing BouncyCastle at a directory that is
# writable but noexec, which is what triggers the uncaught UnsatisfiedLinkError. Being wrong about
# writability is harmless, because BouncyCastle catches the resulting IOException and falls back to
# its Java implementation by itself. So each branch below refuses a directory unless it can
# positively establish that the directory permits execution.
bc_fips_configure() {
  local install_dir="${1}/bc-fips"

  BC_FIPS_FLAGS=()

  # Nothing to do when BouncyCastle's own default already works.
  if [[ -d "${BC_FIPS_DEFAULT_TMPDIR}" && -w "${BC_FIPS_DEFAULT_TMPDIR}" ]] \
      && bc_fips_allows_exec "${BC_FIPS_DEFAULT_TMPDIR}"; then
    return 0
  fi

  # Otherwise extract under lib/, which is already exec-mounted wherever mongot can run at all
  # because Similarity dlopen()s vectorlite out of the same directory moments later.
  if mkdir -p "${install_dir}" 2>/dev/null \
      && [[ -w "${install_dir}" ]] \
      && bc_fips_allows_exec "${install_dir}"; then
    BC_FIPS_FLAGS=("-Dorg.bouncycastle.native.loader.install_dir=${install_dir}")
    return 0
  fi

  # Read-only installs reach here, such as a mongot container that the MongoDB Kubernetes operator
  # runs with readOnlyRootFilesystem. Leaving the property unset is not an option, because
  # BouncyCastle degrades gracefully when it cannot write at all but dies on the uncaught
  # UnsatisfiedLinkError once it can write to a noexec directory. Ask for the Java implementation
  # outright instead. That is already what every ARM deployment runs, since BouncyCastle only
  # publishes these libraries for Linux x86_64.
  echo "mongot: neither ${BC_FIPS_DEFAULT_TMPDIR} nor ${install_dir} is writable and" \
      "exec-mounted; using the BouncyCastle Java implementation" >&2
  BC_FIPS_FLAGS=("-Dorg.bouncycastle.native.cpu_variant=java")
}
