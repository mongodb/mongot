package com.xgen.mongot.util;

import static com.xgen.mongot.util.Check.checkArg;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Deleting contents of a directory is by nature non atomic. This class can be used to simulate
 * atomic directory deletion by moving it somewhere else to be deleted recursively. This class
 * relies on the filesystem's atomic move operation, so both the target directory and trashDirectory
 * must reside on the same volume.
 *
 * <p>deleteDirectory is thread safe.
 */
public class AtomicDirectoryRemover {
  private final Path trashDirectory;

  public AtomicDirectoryRemover(Path trashDirectory) throws IOException {
    // ensure we are starting with a clean directory:
    org.apache.commons.io.FileUtils.deleteDirectory(trashDirectory.toFile());
    FileUtils.mkdirIfNotExist(trashDirectory);

    this.trashDirectory = trashDirectory;
  }

  /**
   * Atomically removes a directory (by atomically renaming it, then recursively deleting the
   * directory).
   */
  public synchronized void deleteDirectory(Path dir) throws IOException {
    // Synchronization required here to handle race conditions on the same directory or directories
    // with the same name.
    if (!dir.toFile().exists()) {
      // this is consistent with org.apache.commons.io.FileUtils.deleteDirectory
      return;
    }

    checkArg(!dir.equals(this.trashDirectory), "cannot remove %s", dir);
    checkArg(dir.toFile().isDirectory(), "[%s] not a directory", dir);

    String tempName = dir.toFile().getName() + ".tmp";
    Path temp = this.trashDirectory.resolve(tempName);

    FileUtils.atomicallyRename(dir, temp);
    org.apache.commons.io.FileUtils.deleteDirectory(temp.toFile());
    // At this point, the deletion might not have fsync-ed to disk. It will be fsync-ed if we shut
    // down cooperatively or next time AtomicDirectoryRemover is re-instantiated.
  }

  /**
   * Deletes all files in a directory (and its subdirectories), with each file deletion being
   * atomic. Subdirectories themselves are not removed.
   */
  public synchronized void deleteFilesInDirectory(Path dir) throws IOException {
    // Synchronization required here to handle race conditions on the same directory or directories
    // with the same name.
    if (!dir.toFile().exists()) {
      // this is consistent with org.apache.commons.io.FileUtils.deleteDirectory
      return;
    }

    checkArg(!dir.equals(this.trashDirectory), "cannot remove %s", dir);
    checkArg(dir.toFile().isDirectory(), "[%s] not a directory", dir);
    try (Stream<Path> filePaths = Files.walk(dir)) {
      CheckedStream.from(filePaths.collect(Collectors.toList()))
          .forEachChecked(
              path -> {
                if (!path.toFile().isDirectory()) {
                  String tempName = path.toFile().getName() + ".tmp";
                  Path temp = this.trashDirectory.resolve(tempName);
                  FileUtils.atomicallyRename(path, temp);
                  Files.delete(temp);
                }
              });

      // At this point, the deletions might not have fsync-ed to disk. It will be fsync-ed if we
      // shut down cooperatively or next time AtomicDirectoryRemover is re-instantiated.
    }
  }

  /**
   * Deletes all immediate children (files and subdirectories) in a directory. Each child is
   * atomically renamed to the trash directory before being deleted, so the child disappears from
   * the source directory in a single atomic operation. The parent directory itself is preserved.
   */
  public synchronized void deleteChildrenInDirectory(Path dir) throws IOException {
    if (!dir.toFile().exists()) {
      return;
    }

    checkArg(!dir.equals(this.trashDirectory), "cannot remove %s", dir);
    checkArg(dir.toFile().isDirectory(), "[%s] not a directory", dir);

    try (Stream<Path> children = Files.list(dir)) {
      for (Path child : children.collect(Collectors.toList())) {
        if (child.toFile().isDirectory()) {
          deleteDirectory(child);
        } else {
          String tempName = child.toFile().getName() + ".tmp";
          Path temp = this.trashDirectory.resolve(tempName);
          FileUtils.atomicallyRename(child, temp);
          Files.delete(temp);
        }
      }
    }

    // As with the existing helpers, deletions may not be fsync-ed until cooperative
    // shutdown or the next time AtomicDirectoryRemover is re-instantiated.
  }
}
