package com.xgen.mongot.metrics.system;

import com.google.common.flogger.FluentLogger;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Locates the nearest cgroup-v2 memory limit constraining this process.
 *
 * <p>A workload slice often carries no limit of its own while ancestors (the pod, the machine's
 * kubepods slice) have one. Cgroup v2 enforces every ancestor's limit, so the constraint worth
 * reporting is the level with the least headroom ({@code memory.max - memory.current}) among the
 * finitely limited ancestors of this process's cgroup.
 */
final class CgroupV2Memory {
  /** The memory limit and current usage of one cgroup level. */
  record Level(long limitBytes, long usageBytes) {
    /** Bytes left before this level's limit; negative while over it. */
    long headroom() {
      return this.limitBytes - this.usageBytes;
    }
  }

  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();
  private static final Path PROC_SELF_CGROUP = Path.of("/proc/self/cgroup");
  private static final Path CGROUP_V2_ROOT = Path.of("/sys/fs/cgroup");

  private final Path procSelfCgroup;
  private final Path cgroupV2Root;

  /** Reads the kernel's view of this process; tests point at a fake hierarchy instead. */
  CgroupV2Memory(Path procSelfCgroup, Path cgroupV2Root) {
    this.procSelfCgroup = procSelfCgroup;
    this.cgroupV2Root = cgroupV2Root;
  }

  static CgroupV2Memory forThisProcess() {
    return new CgroupV2Memory(PROC_SELF_CGROUP, CGROUP_V2_ROOT);
  }

  /**
   * Among the levels of this process's cgroup-v2 hierarchy whose {@code memory.max} is finite and
   * below {@code hostTotal}, the one with the least headroom; empty when the process is
   * unconstrained (or the hierarchy is absent, e.g. cgroup v1 or a non-Linux host). Levels whose
   * usage is unreadable are skipped.
   */
  Optional<Level> tightestLimitedLevel(long hostTotal) {
    try {
      Optional<String> ownPath = readOwnCgroupPath(this.procSelfCgroup);
      if (ownPath.isEmpty()) {
        return Optional.empty();
      }
      // Walk from the process's own cgroup up to the filesystem root.
      List<Level> limited = new ArrayList<>();
      for (Path rel = Path.of(ownPath.get()); rel != null; rel = rel.getParent()) {
        Path dir = this.cgroupV2Root.resolve(rel.toString().replaceFirst("^/", ""));
        Optional<Long> limit = readLongOrMax(dir.resolve("memory.max"));
        if (limit.isEmpty() || limit.get() >= hostTotal) {
          continue;
        }
        readLong(dir.resolve("memory.current"))
            .ifPresent(usage -> limited.add(new Level(limit.get(), usage)));
      }
      return limited.stream().min(Comparator.comparingLong(Level::headroom));
    } catch (IOException | RuntimeException e) {
      // Unreadable or malformed hierarchy: report no level so the caller falls
      // back, rather than a partial walk or a skipped tick.
      FLOGGER.atWarning().withCause(e).atMostEvery(1, TimeUnit.HOURS).log(
          "cannot walk this process's cgroup hierarchy");
      return Optional.empty();
    }
  }

  /**
   * This process's cgroup path from {@code /proc/self/cgroup}: the {@code 0::/path} v2 line, or
   * empty when only v1 entries exist.
   */
  private static Optional<String> readOwnCgroupPath(Path procSelfCgroup) throws IOException {
    for (String line : Files.readAllLines(procSelfCgroup)) {
      if (line.startsWith("0::")) {
        String path = line.substring(3).trim();
        return Optional.of(path.isEmpty() ? "/" : path);
      }
    }
    return Optional.empty();
  }

  /** {@code memory.max}: empty when absent, unreadable, or {@code "max"} (no limit). */
  private static Optional<Long> readLongOrMax(Path file) {
    try {
      String content = Files.readString(file).trim();
      return "max".equals(content) ? Optional.empty() : Optional.of(Long.parseLong(content));
    } catch (IOException | NumberFormatException e) {
      return Optional.empty();
    }
  }

  private static Optional<Long> readLong(Path file) {
    try {
      return Optional.of(Long.parseLong(Files.readString(file).trim()));
    } catch (IOException | NumberFormatException e) {
      return Optional.empty();
    }
  }
}
