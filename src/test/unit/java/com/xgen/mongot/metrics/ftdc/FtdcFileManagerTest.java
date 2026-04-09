package com.xgen.mongot.metrics.ftdc;

import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.DirectorySize;
import com.xgen.mongot.util.bson.ByteUtils;
import com.xgen.testing.TestUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FtdcFileManagerTest {
  private static final Bytes MAX_STORAGE_SIZE = Bytes.ofKibi(10);
  private static final int MAX_FILE_COUNT = 10;

  private TemporaryFolder dir;
  private Path ftdcDir;

  @Before
  public void setUp() throws IOException {
    this.dir = TestUtils.getTempFolder();
    this.ftdcDir = this.dir.getRoot().toPath();
  }

  @Test
  public void testInitializeCreatesDirectoryIfDoesNotExist() throws Exception {
    manager();
    Path dir = this.dir.getRoot().toPath().resolve("foo");
    Assert.assertFalse(dir.toFile().exists());
    FtdcFileManager.initialize(getConfig(dir), new FtdcMetadata.Builder().build());
    Assert.assertTrue(dir.toFile().exists());
  }

  @Test
  public void testInitializeWritesMetadata() throws Exception {
    manager();

    List<BsonDocument> actual = FtdcDecoder.readDocs(this.ftdcDir, FtdcFileType.ARCHIVE);

    // first document should always be metadata
    BsonDocument result = actual.get(0);
    BsonDocument expected = FtdcTestUtil.defaultMetadata();
    FtdcTestUtil.assertMetadata(expected, result);
  }

  /**
   * This is important, if we don't initialize with a new file, we might be appending to a corrupt
   * file (the last record of archive and interim file may be an incomplete write).
   */
  @Test
  public void testEachInitializationCreatesNewArchiveFile() throws Exception {
    manager().writeAndClearInterim(new BsonDocument());
    manager().writeAndClearInterim(new BsonDocument());
    manager().writeAndClearInterim(new BsonDocument());
    Assert.assertEquals(3, listFiles().size());
  }

  @Test
  public void testFlushesToInterimFile() throws Exception {
    var manager = manager();
    BsonDocument expected = document();
    manager.replaceInterim(expected);
    var actual = FtdcDecoder.readOneDocument(Files.readAllBytes(interimFile()));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testRegularFlushDeletesInterimFile() throws Exception {
    var manager = manager();
    manager.replaceInterim(document());
    manager.writeAndClearInterim(document());
    assertInterimFileDoesNotExist();
  }

  @Test
  public void testInterimWriteOverwrites() throws Exception {
    var manager = manager();
    manager.replaceInterim(document());

    BsonDocument expected = new BsonDocument("second", BsonNull.VALUE);
    manager.replaceInterim(expected);

    var actual = FtdcDecoder.readDocsFromFile(interimFile());
    Assert.assertEquals(List.of(expected), actual);
  }

  @Test
  public void testWriteAppendsToArchiveFile() throws Exception {
    var manager = manager();
    var firstDoc = document();
    var secondDoc = new BsonDocument("second", BsonNull.VALUE);
    manager.writeAndClearInterim(firstDoc);
    manager.writeAndClearInterim(secondDoc);

    List<BsonDocument> actual = FtdcDecoder.readDocs(this.ftdcDir, FtdcFileType.ARCHIVE);

    // first document of file is metadata
    FtdcTestUtil.assertMetadata(FtdcTestUtil.defaultMetadata(), actual.get(0));
    Assert.assertEquals(List.of(firstDoc, secondDoc), actual.subList(1, actual.size()));
  }

  @Test
  public void testRotateCreatesNewFile() throws Exception {
    var manager = manager();
    var firstDoc = document();
    var secondDoc = new BsonDocument("second", BsonNull.VALUE);
    manager.writeAndClearInterim(firstDoc);

    // only one file should exist:
    var onlyOneFile = listFiles();
    Assert.assertEquals(1, onlyOneFile.size());

    manager.rotate();
    manager.writeAndClearInterim(secondDoc);

    // 2 files should exist
    var files = listFiles();
    Assert.assertEquals(2, files.size());

    // first file
    List<BsonDocument> firstFileDocs = FtdcDecoder.readDocsFromFile(files.get(0));
    FtdcTestUtil.assertMetadata(FtdcTestUtil.defaultMetadata(), firstFileDocs.get(0));
    Assert.assertEquals(firstDoc, firstFileDocs.get(1));

    // second file
    List<BsonDocument> secondFileDocs = FtdcDecoder.readDocsFromFile(files.get(1));
    FtdcTestUtil.assertMetadata(FtdcTestUtil.defaultMetadata(), secondFileDocs.get(0));
    Assert.assertEquals(secondDoc, secondFileDocs.get(1));
  }

  @Test
  public void testInitializeRecoversDocumentFromInterimFile() throws Exception {
    var bytes = ByteUtils.toByteArray(document());
    Files.write(interimFile(), bytes);

    manager();

    // should remove the interim file
    assertInterimFileDoesNotExist();
    List<BsonDocument> recovered = FtdcDecoder.readDocs(this.ftdcDir, FtdcFileType.ARCHIVE);

    Assert.assertEquals(2, recovered.size());
    // first document of file is metadata
    FtdcTestUtil.assertMetadata(FtdcTestUtil.defaultMetadata(), recovered.get(0));
    // the document should have moved to an archive file
    Assert.assertEquals(document(), recovered.get(1));
  }

  @Test
  public void testCorruptInterimFileIsNotRecovered() throws Exception {
    // we test it against a few cases of corrupt bson documents:
    byte[] documentBytes = ByteUtils.toByteArray(document());
    List<byte[]> corruptDocuments =
        List.of(
            new byte[0], // first 4 bytes of a bson document are the length of the document.
            new byte[] {1},
            new byte[] {1, 2},
            new byte[] {1, 2, 3},
            Arrays.copyOfRange(documentBytes, 0, documentBytes.length - 1),
            Arrays.copyOfRange(documentBytes, 0, documentBytes.length - 2),
            Arrays.copyOfRange(documentBytes, 0, documentBytes.length - 3));

    for (byte[] corrupt : corruptDocuments) {
      Files.write(interimFile(), corrupt);

      manager();

      // should remove the interim file
      assertInterimFileDoesNotExist();
      var recovered = FtdcDecoder.readDocsWithoutMetadata(this.ftdcDir, FtdcFileType.ALL);
      Assert.assertEquals(0, recovered.size());
    }
  }

  @Test
  public void testTrimsDirectoryWhenTooBig() throws Exception {
    var m = manager();
    // each write is over 1KB, so in total we wrote over 10K
    for (int i = 0; i < 10; i++) {
      m.writeAndClearInterim(
          new BsonDocument("foo", new BsonBinary("a".repeat(1200).getBytes()))); // write to limit
    }
    long maxStorageSizeBytes = MAX_STORAGE_SIZE.toBytes();

    Assert.assertTrue(DirectorySize.of(this.dir.getRoot()) < maxStorageSizeBytes);

    // make sure the next write will exceed the limit:
    Assert.assertTrue(DirectorySize.of(this.dir.getRoot()) > maxStorageSizeBytes - 2000);

    m.writeAndClearInterim(
        new BsonDocument("foo", new BsonBinary("a".repeat(3000).getBytes()))); // writes over limit

    Assert.assertTrue(DirectorySize.of(this.dir.getRoot()) < 10000);
  }

  @Test
  public void testTrimsDirectoryWhenTooManyFiles() throws Exception {
    var m = manager();
    for (int i = 0; i < 10; i++) {
      m.writeAndClearInterim(
          new BsonDocument("foo", new BsonBinary("a".repeat(1000).getBytes()))); // write to limit
    }

    // we should have written just under 10KB to 10 different files.
    long maxStorageSizeBytes = MAX_STORAGE_SIZE.toBytes();
    Assert.assertTrue(DirectorySize.of(this.dir.getRoot()) < maxStorageSizeBytes);
    Assert.assertEquals(10, this.listFiles().size());

    // the next write should cause file rotation and put us over the file limit, inducing trimming.
    m.writeAndClearInterim(new BsonDocument("foo", new BsonBinary("a".getBytes())));
    Assert.assertEquals(10, this.listFiles().size());
  }

  @Test
  public void testTrimsDirectoryOnInitialization() throws Exception {
    // Re-creating the manager on every iteration will create a new file, just like it would in a
    // crash loop.
    for (int i = 0; i < MAX_FILE_COUNT + 1; i++) {
      manager().writeAndClearInterim(document());
    }
    manager();
    // GC only considers files that are not the current file, and a new file is created on
    // initialization.
    Assert.assertEquals(MAX_FILE_COUNT + 1, this.listFiles().size());
  }

  @Test
  public void testAutomaticallyRotatesFileIfTooBig() throws Exception {
    var m = manager();
    m.writeAndClearInterim(new BsonDocument()); // file created
    var files = listFiles();
    Assert.assertEquals(1, files.size());
    var firstFile = files.get(0);

    m.writeAndClearInterim(
        new BsonDocument("foo", new BsonBinary("a".repeat(1000).getBytes()))); // write to limit
    Assert.assertTrue(Files.size(firstFile) > 1000);

    var filesAfter = listFiles();
    Assert.assertEquals(2, filesAfter.size());
    var secondFile = filesAfter.get(1);

    List<BsonDocument> docs = FtdcDecoder.readDocsFromFile(secondFile);

    // metadata is automatically written in a new file
    BsonDocument resultMetadata = docs.get(0);
    BsonDocument expectedMetadata = FtdcTestUtil.defaultMetadata();
    FtdcTestUtil.assertMetadata(expectedMetadata, resultMetadata);
  }

  private void assertInterimFileDoesNotExist() {
    Assert.assertFalse(interimFile().toFile().exists());
  }

  private Path interimFile() {
    return this.dir.getRoot().toPath().resolve("metrics.interim");
  }

  private FtdcFileManager manager() throws Exception {
    return FtdcFileManager.initialize(getConfig(this.ftdcDir), new FtdcMetadata.Builder().build());
  }

  private FtdcConfig getConfig(Path dir) {
    return new FtdcConfig(
        dir, MAX_STORAGE_SIZE, Bytes.ofBytes(1000), 4, 50, MAX_FILE_COUNT, Integer.MAX_VALUE);
  }

  private BsonDocument document() {
    return new BsonDocument("foo", new BsonInt32(1));
  }

  private List<Path> listFiles() {
    return Arrays.stream(Objects.requireNonNull(this.ftdcDir.toFile().list()))
        .sorted()
        .map(f -> this.ftdcDir.toAbsolutePath().resolve(f))
        .collect(Collectors.toList());
  }
}
