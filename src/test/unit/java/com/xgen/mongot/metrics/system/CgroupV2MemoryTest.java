package com.xgen.mongot.metrics.system;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CgroupV2MemoryTest {
  private static final long HOST_TOTAL = 16_000_000_000L;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private Path procCgroup(String content) throws IOException {
    Path file = this.temp.newFile("proc_self_cgroup").toPath();
    Files.writeString(file, content);
    return file;
  }

  private Path cgroupFs() throws IOException {
    return this.temp.newFolder("cgroupfs").toPath();
  }

  private static void level(Path root, String relPath, String max, String current)
      throws IOException {
    Path dir = root.resolve(relPath.replaceFirst("^/", ""));
    Files.createDirectories(dir);
    Files.writeString(dir.resolve("memory.max"), max);
    if (current != null) {
      Files.writeString(dir.resolve("memory.current"), current);
    }
  }

  @Test
  public void rootCgroup_readsTheRootLevelItself() throws IOException {
    Path fs = cgroupFs();
    level(fs, "/", "4000000000\n", "1000000000\n");
    var walker = new CgroupV2Memory(procCgroup("0::/\n"), fs);

    var found = walker.tightestLimitedLevel(HOST_TOTAL);

    assertEquals(Optional.of(new CgroupV2Memory.Level(4_000_000_000L, 1_000_000_000L)), found);
  }

  @Test
  public void trailingSlashAndWhitespace_areNormalized() throws IOException {
    Path fs = cgroupFs();
    level(fs, "/a/b", "max\n", null);
    level(fs, "/a", " 3000000000 \n", "2000000000");
    var walker = new CgroupV2Memory(procCgroup("0::/a/b/ \n"), fs);

    var found = walker.tightestLimitedLevel(HOST_TOTAL);

    assertEquals(Optional.of(new CgroupV2Memory.Level(3_000_000_000L, 2_000_000_000L)), found);
  }

  @Test
  public void leastHeadroomWins_notNearest() throws IOException {
    Path fs = cgroupFs();
    level(fs, "/k/pod/c", "8000000000", "1000000000"); // 7 GB headroom
    level(fs, "/k/pod", "4000000000", "3900000000"); // 0.1 GB headroom
    level(fs, "/k", "6000000000", "1000000000"); // 5 GB headroom
    var walker = new CgroupV2Memory(procCgroup("0::/k/pod/c"), fs);

    var found = walker.tightestLimitedLevel(HOST_TOTAL);

    assertEquals(Optional.of(new CgroupV2Memory.Level(4_000_000_000L, 3_900_000_000L)), found);
  }

  @Test
  public void malformedMax_isTreatedAsUnlimited() throws IOException {
    Path fs = cgroupFs();
    level(fs, "/a", "lots", "1");
    level(fs, "/", "max", null);
    var walker = new CgroupV2Memory(procCgroup("0::/a"), fs);

    assertTrue(walker.tightestLimitedLevel(HOST_TOTAL).isEmpty());
  }

  @Test
  public void cgroupV1Only_findsNothing() throws IOException {
    Path fs = cgroupFs();
    level(fs, "/", "1000", "1");
    var walker = new CgroupV2Memory(procCgroup("12:memory:/a\n1:name=systemd:/a\n"), fs);

    assertTrue(walker.tightestLimitedLevel(HOST_TOTAL).isEmpty());
  }

  @Test
  public void unreadableProcFile_findsNothing() throws IOException {
    Path fs = cgroupFs();
    level(fs, "/", "1000", "1");
    var walker = new CgroupV2Memory(fs.resolve("absent"), fs);

    assertTrue(walker.tightestLimitedLevel(HOST_TOTAL).isEmpty());
  }

  @Test
  public void limitAtOrAboveHostTotal_isNotBinding() throws IOException {
    Path fs = cgroupFs();
    level(fs, "/a", Long.toString(HOST_TOTAL), "1");
    level(fs, "/", Long.toString(HOST_TOTAL * 2), "1");
    var walker = new CgroupV2Memory(procCgroup("0::/a"), fs);

    assertTrue(walker.tightestLimitedLevel(HOST_TOTAL).isEmpty());
  }
}
