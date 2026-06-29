package com.xgen.mongot.index.lucene.directory;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.util.AtomicDirectoryRemover;
import com.xgen.mongot.util.FileUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexDirectoryHelper {
  private static final Logger LOG = LoggerFactory.getLogger(IndexDirectoryHelper.class);
  private final AtomicDirectoryRemover indexRemover;
  private final MetricsFactory metricsFactory;
  private final Path rootPath;
  private final Counter incompatibleIndexFormatCounter;
  private final Counter corruptIndexCounter;
  private final Counter incompatibleCodecCounter;
  private final Counter incompatibleSpiFormatCounter;

  /** Tracks the count of unreadable indexes that occurred on dropped indexes. */
  private final Counter unreadableDroppedIndexCounter;

  @VisibleForTesting
  public IndexDirectoryHelper(
      Path rootPath, MetricsFactory metricsFactory, AtomicDirectoryRemover indexRemover) {
    this.indexRemover = indexRemover;
    this.rootPath = rootPath;
    this.metricsFactory = metricsFactory;
    this.corruptIndexCounter = getUnreadableIndexCounter("corruptIndex");
    this.incompatibleIndexFormatCounter = getUnreadableIndexCounter("incompatibleIndexFormat");
    this.incompatibleCodecCounter = getUnreadableIndexCounter("incompatibleCodec");
    this.incompatibleSpiFormatCounter = getUnreadableIndexCounter("incompatibleSpiFormat");
    // Track a separate metric if index was dropped, as it need not be alertable.
    this.unreadableDroppedIndexCounter = this.metricsFactory.counter("unreadableDroppedIndexes");
  }

  public static IndexDirectoryHelper create(Path rootPath, MetricsFactory metricsFactory)
      throws IOException {
    var directoryRemover = new AtomicDirectoryRemover(rootPath.resolve("trash"));
    atomicDropDryRun(rootPath, directoryRemover);
    return new IndexDirectoryHelper(rootPath, metricsFactory, directoryRemover);
  }

  /* Tries to run an atomic drop as a means of failing early in case AtomicDirectoryRemover isn't
   * properly configured.
   */
  private static void atomicDropDryRun(Path rootPath, AtomicDirectoryRemover indexRemover)
      throws IOException {
    Path path = rootPath.resolve("mock_index_dir_to_remove");
    FileUtils.mkdirIfNotExist(path);
    // will fail if the trash directory and data directory are not on the same volume:
    indexRemover.deleteDirectory(path);
  }

  private boolean handleIndexError(
      GenerationId generationId,
      String errorType,
      Path directory,
      Throwable cause,
      Counter counter,
      boolean isRetainFailedIndexDataOnDiskEnabled)
      throws IOException {
    counter.increment();

    if (!isRetainFailedIndexDataOnDiskEnabled) {
      LOG.atError()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .addKeyValue("errorType", errorType)
          .addKeyValue("directory", directory)
          .setCause(cause)
          .log("Could not load index, will attempt to delete its directory and recover");
      this.indexRemover.deleteDirectory(directory);
      return true;
    } else {
      LOG.atError()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .addKeyValue("errorType", errorType)
          .setCause(cause)
          .log("Could not load index");
      return false;
    }
  }

  /**
   * Package private metricsFactory getter for initializing ByteReadCollector within
   * IndexDirectoryFactory.
   *
   * @return MetricsFactory field.
   */
  MetricsFactory getMetricsFactory() {
    return this.metricsFactory;
  }

  /**
   * Returns true if index directory is deleted for index corruption or version mismatch. Returns
   * false if the directory is not deleted.
   */
  public boolean attemptToRecoverUnreadableIndex(
      GenerationId generationId,
      Path directory,
      Exception cause,
      Boolean isIndexDropped,
      Boolean isRetainFailedIndexDataOnDiskEnabled)
      throws IOException {
    LOG.atWarn()
        .addKeyValue("indexId", generationId.indexId)
        .addKeyValue("generationId", generationId)
        .log("Attempt to recover unreadable index");
    if (isIndexDropped) {
      LOG.atWarn()
          .addKeyValue("indexId", generationId.indexId)
          .addKeyValue("generationId", generationId)
          .addKeyValue("exceptionMessage", cause.getMessage())
          .log("Exception on non-existent index, deleting its directory");
      this.unreadableDroppedIndexCounter.increment();
      this.indexRemover.deleteDirectory(directory);
      return true;
    }

    if (ExceptionUtils.indexOfThrowable(cause, CorruptIndexException.class) >= 0
        || ExceptionUtils.indexOfThrowable(cause, NoSuchFileException.class) >= 0) {
      return handleIndexError(
          generationId,
          "index corruption or no such file",
          directory,
          cause,
          this.corruptIndexCounter,
          isRetainFailedIndexDataOnDiskEnabled);
    }

    if (ExceptionUtils.indexOfThrowable(cause, IndexFormatTooOldException.class) >= 0
        || ExceptionUtils.indexOfThrowable(cause, IndexFormatTooNewException.class) >= 0) {
      return handleIndexError(
          generationId,
          "incompatible index formats",
          directory,
          cause,
          this.incompatibleIndexFormatCounter,
          isRetainFailedIndexDataOnDiskEnabled);
    }

    if (ExceptionUtils.getThrowableList(cause).stream()
        .filter(e -> IllegalArgumentException.class.equals(e.getClass()))
        .map(Throwable::getMessage)
        .map(Optional::ofNullable)
        .flatMap(Optional::stream)
        .anyMatch(s -> s.contains("Could not load codec"))) {
      return handleIndexError(
          generationId,
          "Lucene codec version mismatch",
          directory,
          cause,
          this.incompatibleCodecCounter,
          isRetainFailedIndexDataOnDiskEnabled);
    }

    // SPI format lookup failures (e.g. an older binary reading a segment written in a newer
    // KnnVectorsFormat/PostingsFormat) surface as an IllegalArgumentException with a distinct
    // message. Match it so the recovery attempt fires and unreadableIndexRecoveries increments,
    // which is what the downgrade test's assert_no_index_rebuild() keys off of.
    //
    // This match depends on the exact wording Lucene's NamedSPILoader produces. It is asserted
    // against a real Lucene-generated message in
    // IndexDirectoryHelperTest
    // #attemptToRecoverUnreadableIndex_spiFormatNotFound_recoversAndIncrementsSpiCounter
    // -- if a Lucene upgrade changes the message, that test fails. Keep them in sync.
    if (ExceptionUtils.getThrowableList(cause).stream()
        .filter(e -> IllegalArgumentException.class.equals(e.getClass()))
        .map(Throwable::getMessage)
        .map(Optional::ofNullable)
        .flatMap(Optional::stream)
        .anyMatch(s -> s.contains("An SPI class of type") && s.contains("does not exist"))) {
      return handleIndexError(
          generationId,
          "SPI format not found",
          directory,
          cause,
          this.incompatibleSpiFormatCounter,
          isRetainFailedIndexDataOnDiskEnabled);
    }

    return false;
  }

  public Path getIndexDirectoryPath(IndexDefinitionGeneration definition) {
    if (definition.generation().indexFormatVersion.versionNumber <= 4) {
      throw new IllegalStateException("Index format version is not supported");
    }
    // Each index's data will live in a directory with the index's generation ID as the name, as
    // these IDs are unique.
    if (definition.generation().equals(Generation.FIRST)) {
      // indexes preceding the introduction of generations, we use the indexId as the path
      String indexId = definition.getIndexDefinition().getIndexId().toHexString();
      LOG.atInfo()
          .addKeyValue("indexId", indexId)
          .addKeyValue("generation", definition.generation())
          .log("using legacy data path for index of first generation");
      return Paths.get(this.rootPath.toString(), indexId);
    }

    // for any generation other than Generation.FIRST, we encode the generation as well as the
    // indexId.
    return Paths.get(this.rootPath.toString(), getNameForGeneration(definition));
  }

  public Path getCustomEngineIndexPath(IndexDefinitionGeneration definition, String engineName) {
    Path indexDirectoryPath = this.getIndexDirectoryPath(definition);
    return Paths.get(indexDirectoryPath.toString(), engineName);
  }

  /**
   * Retrieve path where index metadata (eg: mapping of index files in blobstore) will be stored.
   */
  public Path getIndexMetadataPath(IndexDefinitionGeneration definition) {
    return Paths.get(this.rootPath.toString(), "indexMetadata", getNameForGeneration(definition));
  }

  @VisibleForTesting
  public static String getNameForGeneration(IndexDefinitionGeneration definition) {
    return String.format(
        "%s_f%s_u%s_a%s",
        definition.getIndexDefinition().getIndexId().toHexString(),
        definition.generation().indexFormatVersion.versionNumber,
        definition.generation().userIndexVersion.versionNumber,
        definition.generation().attemptNumber);
  }

  public AtomicDirectoryRemover getIndexRemover() {
    return this.indexRemover;
  }

  private Counter getUnreadableIndexCounter(String metricLabel) {
    return this.metricsFactory.counter(
        "unreadableIndexRecoveries", Tags.of(Tag.of("unreadableIndexCause", metricLabel)));
  }
}
