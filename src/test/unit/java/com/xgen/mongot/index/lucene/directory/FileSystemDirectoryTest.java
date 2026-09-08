package com.xgen.mongot.index.lucene.directory;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.PreloadHint;
import org.apache.lucene.store.ReadAdvice;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

public class FileSystemDirectoryTest {
  private static final Path ROOT_PATH = Path.of(System.getenv("TEST_TMPDIR"));
  private static final AtomicInteger COUNTER = new AtomicInteger();
  private static final IOContext PRELOAD_CONTEXT =
      IOContext.DEFAULT.withHints(PreloadHint.INSTANCE);

  @Test
  public void prewarmVectorFiles_emptyDirectory_doesNotOpenAnyFiles() throws IOException {
    // Arrange
    MMapDirectory mmapDirectory = mockMMapDirectoryWithFiles();
    FileSystemDirectory directory = createDirectory(mmapDirectory);

    // Act
    directory.prewarmVectorFiles();

    // Assert
    InOrder inOrder = inOrder(mmapDirectory);
    inOrder.verify(mmapDirectory).setPreload(MMapDirectory.PRELOAD_HINT);
    inOrder.verify(mmapDirectory).listAll();
    inOrder.verify(mmapDirectory).setPreload(MMapDirectory.NO_FILES);
    verify(mmapDirectory, never()).openInput(anyString(), eq(PRELOAD_CONTEXT));
  }

  @Test
  public void prewarmVectorFiles_veqSuppressesMatchingVec_skipsMatchingVecFile()
      throws IOException {
    // Arrange
    MMapDirectory mmapDirectory =
        mockMMapDirectoryWithFiles("segmentA.vec", "segmentA.veq", "segmentB.vec", "segmentC.vex");
    IndexInput veqInput = mock(IndexInput.class);
    IndexInput vecInput = mock(IndexInput.class);
    IndexInput vexInput = mock(IndexInput.class);
    when(mmapDirectory.openInput("segmentA.veq", PRELOAD_CONTEXT)).thenReturn(veqInput);
    when(mmapDirectory.openInput("segmentB.vec", PRELOAD_CONTEXT)).thenReturn(vecInput);
    when(mmapDirectory.openInput("segmentC.vex", PRELOAD_CONTEXT)).thenReturn(vexInput);
    FileSystemDirectory directory = createDirectory(mmapDirectory);

    // Act
    directory.prewarmVectorFiles();

    // Assert
    verify(mmapDirectory, never()).openInput("segmentA.vec", PRELOAD_CONTEXT);
    verify(mmapDirectory).openInput("segmentA.veq", PRELOAD_CONTEXT);
    verify(mmapDirectory).openInput("segmentB.vec", PRELOAD_CONTEXT);
    verify(mmapDirectory).openInput("segmentC.vex", PRELOAD_CONTEXT);
    verify(veqInput).close();
    verify(vecInput).close();
    verify(vexInput).close();
  }

  @Test
  public void prewarmVectorFiles_handlesListFailureWithoutThrowing_andResetsPreload()
      throws IOException {
    // Arrange
    MMapDirectory mmapDirectory = mock(MMapDirectory.class);
    when(mmapDirectory.getDirectory()).thenReturn(ROOT_PATH);
    when(mmapDirectory.listAll()).thenThrow(new IOException("listAll failed"));
    FileSystemDirectory directory = createDirectory(mmapDirectory);

    // Act
    directory.prewarmVectorFiles();

    // Assert
    verify(mmapDirectory).setPreload(MMapDirectory.PRELOAD_HINT);
    verify(mmapDirectory).setPreload(MMapDirectory.NO_FILES);
  }

  @Test
  public void prewarmVectorFiles_handlesOpenInputFailureWithoutThrowing_andResetsPreload()
      throws IOException {
    // Arrange
    MMapDirectory mmapDirectory = mockMMapDirectoryWithFiles("segmentA.vec");
    when(mmapDirectory.openInput("segmentA.vec", PRELOAD_CONTEXT))
        .thenThrow(new IOException("openInput failed"));
    FileSystemDirectory directory = createDirectory(mmapDirectory);

    // Act
    directory.prewarmVectorFiles();

    // Assert
    verify(mmapDirectory).setPreload(MMapDirectory.PRELOAD_HINT);
    verify(mmapDirectory).setPreload(MMapDirectory.NO_FILES);
  }

  @Test
  public void prewarmVectorFiles_mixedExtensions_onlyWarmsSupportedVectorExtensions()
      throws IOException {
    // Arrange
    MMapDirectory mmapDirectory =
        mockMMapDirectoryWithFiles("segmentA.vex", "segmentB.tip", "segmentC.doc", "segmentD.vec");
    IndexInput vexInput = mock(IndexInput.class);
    IndexInput vecInput = mock(IndexInput.class);
    when(mmapDirectory.openInput("segmentA.vex", PRELOAD_CONTEXT)).thenReturn(vexInput);
    when(mmapDirectory.openInput("segmentD.vec", PRELOAD_CONTEXT)).thenReturn(vecInput);
    FileSystemDirectory directory = createDirectory(mmapDirectory);

    // Act
    directory.prewarmVectorFiles();

    // Assert
    verify(mmapDirectory).openInput("segmentA.vex", PRELOAD_CONTEXT);
    verify(mmapDirectory).openInput("segmentD.vec", PRELOAD_CONTEXT);
    verify(mmapDirectory, never()).openInput("segmentB.tip", PRELOAD_CONTEXT);
    verify(mmapDirectory, never()).openInput("segmentC.doc", PRELOAD_CONTEXT);
  }

  @Test
  public void prewarmVectorFiles_multipleExtensions_warmsFilesInConfiguredExtensionOrder()
      throws IOException {
    // Arrange
    MMapDirectory mmapDirectory =
        mockMMapDirectoryWithFiles("segmentB.vex", "segmentA.vec", "segmentC.veq", "segmentA.vex");
    when(mmapDirectory.openInput("segmentA.vec", PRELOAD_CONTEXT))
        .thenReturn(mock(IndexInput.class));
    when(mmapDirectory.openInput("segmentC.veq", PRELOAD_CONTEXT))
        .thenReturn(mock(IndexInput.class));
    when(mmapDirectory.openInput("segmentB.vex", PRELOAD_CONTEXT))
        .thenReturn(mock(IndexInput.class));
    when(mmapDirectory.openInput("segmentA.vex", PRELOAD_CONTEXT))
        .thenReturn(mock(IndexInput.class));
    FileSystemDirectory directory = createDirectory(mmapDirectory);

    // Act
    directory.prewarmVectorFiles();

    // Assert
    InOrder inOrder = inOrder(mmapDirectory);
    inOrder.verify(mmapDirectory).openInput("segmentA.vec", PRELOAD_CONTEXT);
    inOrder.verify(mmapDirectory).openInput("segmentC.veq", PRELOAD_CONTEXT);
    inOrder.verify(mmapDirectory).openInput("segmentB.vex", PRELOAD_CONTEXT);
    inOrder.verify(mmapDirectory).openInput("segmentA.vex", PRELOAD_CONTEXT);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void setReadAdvice_resolvesContextThenExplicitHintsWithRandomFloor() throws IOException {
    // Arrange: constructing the directory installs the read-advice resolver on the MMapDirectory.
    MMapDirectory mmapDirectory = mock(MMapDirectory.class);
    createDirectory(mmapDirectory);

    ArgumentCaptor<BiFunction> captor = ArgumentCaptor.forClass(BiFunction.class);
    verify(mmapDirectory).setReadAdvice(captor.capture());
    BiFunction readAdvice = captor.getValue();

    IOContext merge = IOContext.merge(new MergeInfo(1, 1L, false, 1));
    IOContext flush = IOContext.flush(new FlushInfo(1, 1L));

    // Merge/flush stream segments front-to-back (SEQUENTIAL); all other reads default to
    // random-access search traffic (RANDOM), independent of file name.
    assertEquals(Optional.of(ReadAdvice.SEQUENTIAL), readAdvice.apply("_0.doc", merge));
    assertEquals(Optional.of(ReadAdvice.SEQUENTIAL), readAdvice.apply("_0.tim", flush));
    assertEquals(Optional.of(ReadAdvice.RANDOM), readAdvice.apply("_0.doc", IOContext.DEFAULT));

    // Explicit codec DataAccessHints on a non-merge/flush context are honored: SEQUENTIAL for
    // flat-vector bulk reads, RANDOM otherwise.
    assertEquals(
        Optional.of(ReadAdvice.SEQUENTIAL),
        readAdvice.apply("_0.vec", IOContext.DEFAULT.withHints(DataAccessHint.SEQUENTIAL)));
    assertEquals(
        Optional.of(ReadAdvice.RANDOM),
        readAdvice.apply("_0.doc", IOContext.DEFAULT.withHints(DataAccessHint.RANDOM)));

    // Merge context wins over an explicit hint: a merge carrying a RANDOM hint is the same as a
    // merge with no hints at all and still streams SEQUENTIAL.
    assertEquals(merge, merge.withHints(DataAccessHint.RANDOM));
    assertEquals(
        Optional.of(ReadAdvice.SEQUENTIAL),
        readAdvice.apply("_0.doc", merge.withHints(DataAccessHint.RANDOM)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void setReadAdvice_termsIndexAndPostingsHints_restoreNormalReadahead() throws IOException {
    // Arrange: constructing the directory installs the read-advice resolver on the MMapDirectory.
    MMapDirectory mmapDirectory = mock(MMapDirectory.class);
    createDirectory(mmapDirectory);

    ArgumentCaptor<BiFunction> captor = ArgumentCaptor.forClass(BiFunction.class);
    verify(mmapDirectory).setReadAdvice(captor.capture());
    BiFunction readAdvice = captor.getValue();

    // Lucene 10.1 opened the blocktree terms index with RANDOM_PRELOAD (madvise NORMAL) and
    // postings with explicit NORMAL for forward-only readahead. 10.4 replaced those per-file
    // overrides with hints; the resolver maps the hints back to NORMAL instead of the RANDOM
    // floor. Contexts mirror the real call sites in Lucene90BlockTreeTermsReader and
    // Lucene99PostingsReader.
    assertEquals(
        Optional.of(ReadAdvice.NORMAL),
        readAdvice.apply("_0.tip", IOContext.DEFAULT.withHints(FileTypeHint.INDEX)));
    assertEquals(
        Optional.of(ReadAdvice.NORMAL),
        readAdvice.apply(
            "_0.doc", IOContext.DEFAULT.withHints(FileTypeHint.DATA, FileDataHint.POSTINGS)));

    // An explicit DataAccessHint takes precedence, so vector inputs that pass
    // DataAccessHint.RANDOM keep it even if a file-type hint is also present.
    assertEquals(
        Optional.of(ReadAdvice.RANDOM),
        readAdvice.apply(
            "_0.vex", IOContext.DEFAULT.withHints(FileTypeHint.INDEX, DataAccessHint.RANDOM)));

    // Stored fields explicitly request RANDOM in Lucene 10.4.
    assertEquals(
        Optional.of(ReadAdvice.RANDOM),
        readAdvice.apply(
            "_0.fdt", IOContext.DEFAULT.withHints(FileTypeHint.DATA, DataAccessHint.RANDOM)));

    // Merge context still wins over the postings hints and streams SEQUENTIAL.
    IOContext merge = IOContext.merge(new MergeInfo(1, 1L, false, 1));
    assertEquals(
        Optional.of(ReadAdvice.SEQUENTIAL),
        readAdvice.apply("_0.doc", merge.withHints(FileTypeHint.DATA, FileDataHint.POSTINGS)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void setReadAdvice_compoundContainerDocValuesAndNorms_restoreNormalReadahead()
      throws IOException {
    MMapDirectory mmapDirectory = mock(MMapDirectory.class);
    createDirectory(mmapDirectory);

    ArgumentCaptor<BiFunction> captor = ArgumentCaptor.forClass(BiFunction.class);
    verify(mmapDirectory).setReadAdvice(captor.capture());
    BiFunction readAdvice = captor.getValue();

    // Inner compound-file slices inherit the container's advice.
    assertEquals(Optional.of(ReadAdvice.NORMAL), readAdvice.apply("_0.cfs", IOContext.DEFAULT));

    // Doc values and norms carry only a DATA hint in Lucene 10.4.
    assertEquals(
        Optional.of(ReadAdvice.NORMAL),
        readAdvice.apply("_0.dvd", IOContext.DEFAULT.withHints(FileTypeHint.DATA)));
    assertEquals(
        Optional.of(ReadAdvice.NORMAL),
        readAdvice.apply("_0.nvd", IOContext.DEFAULT.withHints(FileTypeHint.DATA)));

    // Unhinted non-container files default to RANDOM.
    assertEquals(Optional.of(ReadAdvice.RANDOM), readAdvice.apply("_0.tim", IOContext.DEFAULT));
    assertEquals(Optional.of(ReadAdvice.RANDOM), readAdvice.apply("_0.blm", IOContext.DEFAULT));

    // FLUSH takes precedence over the container name.
    IOContext flush = IOContext.flush(new FlushInfo(1, 1L));
    assertEquals(Optional.of(ReadAdvice.SEQUENTIAL), readAdvice.apply("_0.cfs", flush));
  }

  private static MMapDirectory mockMMapDirectoryWithFiles(String... files) throws IOException {
    MMapDirectory mmapDirectory = mock(MMapDirectory.class);
    when(mmapDirectory.getDirectory()).thenReturn(ROOT_PATH);
    when(mmapDirectory.listAll()).thenReturn(files);
    return mmapDirectory;
  }

  private static FileSystemDirectory createDirectory(MMapDirectory mmapDirectory)
      throws IOException {
    Path path = Files.createDirectories(ROOT_PATH.resolve("fsd-test-" + COUNTER.incrementAndGet()));
    return new FileSystemDirectory(mmapDirectory, new NIOFSDirectory(path), Optional.empty());
  }
}
