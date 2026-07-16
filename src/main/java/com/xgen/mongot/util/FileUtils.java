package com.xgen.mongot.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import org.jetbrains.annotations.Nullable;

public class FileUtils {

  private static final String DEFAULT_TEMP_FILE_SUFFIX = ".tmp";

  /**
   * Tries to create a directory if it does not exist. As well as parent directories as needed.
   *
   * <p>On failure, the underlying {@link java.nio.file.FileSystemException} (which carries the
   * native {@code strerror(errno)} reason, e.g. {@code "No space left on device"}) is preserved as
   * the cause so that {@link Crash#assignCrashCategory} can classify disk-full crashes as {@link
   * Crash.CrashCategory#DISK_FULL}.
   *
   * <p>Not thread safe!
   */
  public static void mkdirIfNotExist(Path dir) throws IOException {
    if (Files.exists(dir)) {
      return;
    }
    try {
      Files.createDirectories(dir);
    } catch (IOException e) {
      throw new IOException(
          String.format("unable to create directory at %s: %s", dir, e.getMessage()), e);
    }
  }

  /** Atomically updates the file at the supplied path to contain the supplied contents. */
  public static void atomicallyReplace(Path target, String contents) throws IOException {
    byte[] contentBytes = contents.getBytes(StandardCharsets.UTF_8);
    atomicallyReplace(target, contentBytes);
  }

  /** Atomically updates the file at the supplied path to contain the supplied contents. */
  public static void atomicallyReplace(Path target, byte[] contents) throws IOException {
    String targetName = checkNotNull(target.getFileName()).toString();
    String tempName = targetName.concat(DEFAULT_TEMP_FILE_SUFFIX);
    Path tempPath = target.resolveSibling(tempName);
    atomicallyReplace(target, tempPath, contents);
  }

  private static void atomicallyReplace(Path target, Path tempPath, byte[] contents)
      throws IOException {
    // Write the contents to the temporary file that we will atomically rename to the target.
    Files.write(tempPath, contents);

    // Need to ensure that the bytes are flushed to the disk prior to invoking the rename.
    // See this blog post for a more detailed explanation of why this must be fsync-ed:
    // https://blog.dgraph.io/post/alice/
    fsync(tempPath);

    atomicallyRename(tempPath, target);
  }

  /** Atomically rename a file. Assuming target's directory exists. */
  public static void atomicallyRename(Path source, Path target) throws IOException {
    Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);

    // Flush the directories to disk to ensure metadata is updated.
    // See this comment in wiredtiger's POSIX rename implementation for more information:
    // https://github.com/wiredtiger/wiredtiger/blob/9b32813d625d3dbdf0fd83a7eb4ce10fda0d18f3/src/os_posix/os_fs.c#L246-L252
    @Nullable Path sourceParent = source.getParent();
    if (sourceParent != null) {
      fsync(sourceParent);
    }

    @Nullable Path targetParent = target.getParent();
    if (targetParent != null && !targetParent.equals(sourceParent)) {
      fsync(targetParent);
    }
  }

  private static void fsync(Path path) throws IOException {
    // See the section "Syncing Filenames" in
    // http://blog.httrack.com/blog/2013/11/15/everything-you-always-wanted-to-know-about-fsync/ on
    // why we open in R for directories and R/W for files.
    // Note that calling FileChannel.force(true) is currently the only way in java to fsync a
    // directory. This method is relied upon by Lucene itself, as can be seen by reading the
    // following thread: http://mail.openjdk.java.net/pipermail/nio-dev/2015-May/003147.html
    // Ideally this will eventually be explicitly added, and is tracked here:
    // https://bugs.openjdk.java.net/browse/JDK-8080235
    OpenOption openOption =
        path.toFile().isDirectory() ? StandardOpenOption.READ : StandardOpenOption.WRITE;
    try (FileChannel fc = FileChannel.open(path, openOption)) {
      try {
        fc.force(true);
      } catch (IOException e) {
        // For rationale on why we should not continue after a failed fsync, see this comment:
        // https://github.com/wiredtiger/wiredtiger/blob/9b32813d625d3dbdf0fd83a7eb4ce10fda0d18f3/src/os_posix/os_fs.c#L33-L43
        Crash.because("fsync failed").withThrowable(e).now();
      }
    }
  }
}
