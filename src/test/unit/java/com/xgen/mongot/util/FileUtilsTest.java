package com.xgen.mongot.util;

import com.xgen.mongot.util.Crash.CrashCategory;
import com.xgen.testing.TestUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileUtilsTest {
  @Test
  public void testMkdirIfNotExist() throws IOException {
    TemporaryFolder tempDir = TestUtils.getTempFolder();
    File root = tempDir.getRoot();
    Path dir = getChildFilePath(root.toPath(), "dir");
    FileUtils.mkdirIfNotExist(dir);
    Assert.assertTrue(dir.toFile().exists());
    FileUtils.mkdirIfNotExist(dir); // should not throw anything
  }

  @Test
  public void testMkdirIfNotExistPreservesUnderlyingCause() throws IOException {
    TemporaryFolder tempDir = TestUtils.getTempFolder();
    Path root = tempDir.getRoot().toPath();
    // A regular file cannot be a parent of a directory, so Files.createDirectories will fail
    // with a FileSystemException carrying the native strerror(errno) reason.
    Path notADir = root.resolve("not-a-dir");
    Files.writeString(notADir, "x");
    Path childDir = notADir.resolve("child");

    IOException e =
        Assert.assertThrows(IOException.class, () -> FileUtils.mkdirIfNotExist(childDir));

    // The wrapped IOException must preserve the underlying cause so that its message
    // (e.g. "No space left on device" for ENOSPC) reaches Crash.assignCrashCategory via
    // ExceptionUtils.getStackTrace, which is what lets disk-full crashes be classified as
    // CrashCategory.DISK_FULL.
    Assert.assertTrue(e.getCause() instanceof FileSystemException);
    String causeMessage = e.getCause().getMessage();
    Assert.assertNotNull(causeMessage);
    Assert.assertTrue(e.getMessage().contains(childDir.toString()));
    Assert.assertTrue(ExceptionUtils.getStackTrace(e).contains(causeMessage));
  }

  @Test
  public void testMkdirIfNotExistDiskFullIsClassifiedAsDiskFull() {
    // A synthetic ENOSPC failure of the shape mkdirIfNotExist now produces: the wrapped
    // IOException's cause is a FileSystemException whose reason is the native errno string
    // "No space left on device". Verify Crash routes this to CrashCategory.DISK_FULL.
    FileSystemException enospc =
        new FileSystemException("/srv/mongodb/mongot/trash", null, "No space left on device");
    IOException wrapped =
        new IOException(
            String.format(
                "unable to create directory at %s: %s",
                "/srv/mongodb/mongot/trash", enospc.getMessage()),
            enospc);

    String stackTrace = ExceptionUtils.getStackTrace(wrapped);
    CrashCategory category =
        Crash.because("failed to create directory").assignCrashCategory(stackTrace);
    Assert.assertEquals(CrashCategory.DISK_FULL, category);
  }

  @Test
  public void testAtomicallyReplaceFileDoesNotYetExist() throws Exception {
    TemporaryFolder tempDir = TestUtils.getTempFolder();
    File root = tempDir.getRoot();

    String fileName = "file.txt";
    Path filePath = getChildFilePath(root.toPath(), fileName);
    String contents = "hello world";
    FileUtils.atomicallyReplace(filePath, contents);

    Assert.assertEquals(contents, Files.readString(filePath));
    assertOnlyOneFile(root, fileName);
  }

  @Test
  public void testAtomicallyReplaceWithByteContents() throws Exception {
    TemporaryFolder tempDir = TestUtils.getTempFolder();
    File root = tempDir.getRoot();

    String fileName = "file.txt";
    Path filePath = getChildFilePath(root.toPath(), fileName);
    byte[] contents = "hello world".getBytes();
    FileUtils.atomicallyReplace(filePath, contents);

    Assert.assertArrayEquals(contents, Files.readAllBytes(filePath));
    assertOnlyOneFile(root, fileName);
  }

  @Test
  public void testAtomicallyReplaceOverwriteExistingFile() throws Exception {
    TemporaryFolder tempDir = TestUtils.getTempFolder();
    File root = tempDir.getRoot();

    String fileName = "file.txt";
    Path filePath = getChildFilePath(root.toPath(), fileName);
    String originalContents = "hello world";
    Files.writeString(filePath, originalContents);

    String overwrittenContents = "foobar";
    FileUtils.atomicallyReplace(filePath, overwrittenContents);

    Assert.assertEquals(overwrittenContents, Files.readString(filePath));
    assertOnlyOneFile(root, fileName);
  }

  private static Path getChildFilePath(Path dir, String fileName) {
    return dir.resolve(fileName);
  }

  private static void assertOnlyOneFile(File dir, String fileName) {
    File[] files = dir.listFiles();
    Assert.assertEquals(1, files.length);
    Assert.assertEquals(fileName, files[0].toPath().getFileName().toString());
  }
}
