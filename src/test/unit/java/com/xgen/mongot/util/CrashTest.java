package com.xgen.mongot.util;

import static com.xgen.mongot.util.Crash.buildCrashLogs;
import static org.junit.Assert.assertEquals;

import com.xgen.mongot.util.Crash.CrashCategory;
import java.nio.file.Path;
import java.util.Arrays;
import org.bson.BsonDocument;
import org.bson.json.JsonObject;
import org.junit.Test;

public class CrashTest {

  /**
   * Bootstrap reads the crash log path twice: leniently before the config is parsed, and again from
   * the parsed config. A second set of the same path must not crash mongot. A second set of a
   * different path still halts, which would fail this test by taking down the test JVM.
   */
  @Test
  public void setCrashLogPath_samePathTwice_doesNotHalt() {
    Path crashLogPath = Path.of("/test/crash.log");

    Crash.setCrashLogPath(crashLogPath);
    Crash.setCrashLogPath(crashLogPath);
  }

  @Test
  public void createStructuredLogs_containsAllFields() {
    var exception = new RuntimeException();
    String exceptionName = exception.getClass().getTypeName();
    String crashReason = "crash reason";
    String stackTrace = Arrays.toString(exception.getStackTrace());
    String threadDump = "thread\ndump";
    String mongotVersion = "1.2.3";
    CrashCategory category = CrashCategory.EXECUTOR_SHUTDOWN;
    String structuredLogs =
        Crash.createStructuredLogs(
            exceptionName, crashReason, stackTrace, threadDump, mongotVersion, category);
    BsonDocument parsedLogs = new JsonObject(structuredLogs).toBsonDocument();
    assertEquals("mongot-crash", parsedLogs.getString("type").getValue());
    assertEquals(parsedLogs.getString("exception").getValue(), exceptionName);
    assertEquals(crashReason, parsedLogs.getString("reason").getValue());
    assertEquals(parsedLogs.getString("stackTrace").getValue(), stackTrace);
    assertEquals(threadDump, parsedLogs.getString("threadDump").getValue());
    assertEquals(mongotVersion, parsedLogs.getString("mongotVersion").getValue());
    assertEquals(category.toString(), parsedLogs.getString("crashCategory").getValue());
  }

  @Test
  public void buildCrashLogs_formatsCrashLogsCorrectly() {
    var exception = new RuntimeException();
    String exceptionName = exception.getClass().getTypeName();
    String crashReason = "crash reason";
    String stackTrace = Arrays.toString(exception.getStackTrace());
    String threadDump = "thread\ndump";
    String mongotVersion = "1.2.3";
    CrashCategory category = CrashCategory.EXECUTOR_SHUTDOWN;

    String structuredLogs =
        Crash.createStructuredLogs(
            exceptionName, crashReason, stackTrace, threadDump, mongotVersion, category);
    String crashLogs =
        buildCrashLogs(exceptionName, crashReason, stackTrace, threadDump, mongotVersion, category);
    String[] crashLogLines = crashLogs.split("\n");
    assertEquals(3, crashLogLines.length);
    assertEquals(crashLogLines[0], structuredLogs);
    assertEquals(crashReason, crashLogLines[1]);
    assertEquals(crashLogLines[2], stackTrace);
  }

  @Test
  public void assignCrashCategory_handlesAllDefinedCategories() {
    Crash oomCrash = Crash.because("Successfully reported OOM");
    assertEquals(CrashCategory.OOM, oomCrash.assignCrashCategory(""));

    Crash executorShutdownCrash = Crash.because("failed to shut down config-monitor executor");
    assertEquals(CrashCategory.EXECUTOR_SHUTDOWN, executorShutdownCrash.assignCrashCategory(""));

    String stacktrace =
        "java.nio.file.FileSystemException: /srv/mongodb/mongot/diagnostic.data/metrics.2025-09-25"
            + "T00-23-40Z-00000: No space left on device\n"
            + "\tat java.base/sun.nio.fs.UnixException.translateToIOException(UnixException.java:"
            + "100)\n"
            + "\tat java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:"
            + "106)\n"
            + "\tat java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:111"
            + ")\n"
            + "\tat java.base/sun.nio.fs.UnixFileSystemProvider.newByteChannel("
            + "UnixFileSystemProvider.java:261)\n"
            + "\tat java.base/java.nio.file.spi.FileSystemProvider.newOutputStream("
            + "FileSystemProvider.java:482)\n"
            + "\tat java.base/java.nio.file.Files.newOutputStream(Files.java:227)\n"
            + "\tat java.base/java.nio.file.Files.write(Files.java:3492)\n"
            + "\tat com.xgen.mongot.metrics.ftdc.FtdcFileManager.newArchiveFile("
            + "FtdcFileManager.java:193)\n"
            + "\tat com.xgen.mongot.metrics.ftdc.FtdcFileManager.<init>(FtdcFileManager.java:68)\n"
            + "\tat com.xgen.mongot.metrics.ftdc.FtdcFileManager.initialize("
            + "FtdcFileManager.java:74)\n"
            + "\tat com.xgen.mongot.metrics.ftdc.Ftdc.initialize(Ftdc.java:29)...\n";

    Crash diskFullCrash = Crash.because("failed to initialize ftdc");
    assertEquals(CrashCategory.DISK_FULL, diskFullCrash.assignCrashCategory(stacktrace));

    String lockContentionStacktrace =
        "java.io.IOException: Failed to acquire exclusive lock on /srv/mongodb/mongot/.lock."
            + " Another process may be running.\n"
            + "\tat com.xgen.mongot.util.concurrent.FileLockGuard.acquireLock("
            + "FileLockGuard.java:107)\n"
            + "\tat com.xgen.mongot.util.concurrent.FileLockGuard.withPersistent("
            + "FileLockGuard.java:94)\n"
            + "\tat com.xgen.atlas.Mongot.acquireFileLock(Mongot.java:143)...\n";

    Crash lockContentionCrash = Crash.because("failed to acquire exclusive lock");
    assertEquals(
        CrashCategory.LOCK_CONTENTION,
        lockContentionCrash.assignCrashCategory(lockContentionStacktrace));

    // Other failures at the same call site (e.g. a missing data directory) stay in OTHER.
    String missingLockFileStacktrace =
        "java.nio.file.NoSuchFileException: /srv/mongodb/mongot/.lock\n"
            + "\tat java.base/sun.nio.fs.UnixException.translateToIOException("
            + "UnixException.java:92)\n"
            + "\tat com.xgen.atlas.Mongot.acquireFileLock(Mongot.java:143)...\n";

    Crash missingLockFileCrash = Crash.because("failed to acquire exclusive lock");
    assertEquals(
        CrashCategory.OTHER, missingLockFileCrash.assignCrashCategory(missingLockFileStacktrace));

    Crash otherCrash = Crash.because("of some other reason");
    assertEquals(CrashCategory.OTHER, otherCrash.assignCrashCategory(""));

    Crash.setShutdownStartedFlag();
    Crash shutdownCrash = Crash.because("of any reason at all");
    assertEquals(CrashCategory.SHUTDOWN, shutdownCrash.assignCrashCategory(""));
  }
}
