package com.xgen.mongot.index.lucene.directory;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.util.FileUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A factory class to create {@link Directory} according to the config. */
public class IndexDirectoryFactory extends IndexPathFactory {
  private static final Logger LOG = LoggerFactory.getLogger(IndexDirectoryFactory.class);
  private final LuceneConfig config;
  private final Optional<ByteReadCollector> byteReadCollector;
  private final boolean prewarm;
  private final Optional<CacheWarmerTimer> cacheWarmerTimer;

  public IndexDirectoryFactory(
      Path indexPath,
      Path metadataPath,
      LuceneConfig config,
      int numPartitions,
      Optional<ByteReadCollector> byteReadCollector,
      boolean prewarm,
      Optional<CacheWarmerTimer> cacheWarmerTimer) {
    super(indexPath, metadataPath, numPartitions);
    this.config = config;
    this.byteReadCollector = byteReadCollector;
    this.prewarm = prewarm;
    this.cacheWarmerTimer = cacheWarmerTimer;
  }

  public IndexDirectoryFactory(
      Path indexPath,
      Path metadataPath,
      LuceneConfig config,
      int numPartitions,
      Optional<ByteReadCollector> byteReadCollector) {
    this(
        indexPath, metadataPath, config, numPartitions, byteReadCollector, false, Optional.empty());
  }

  public IndexDirectoryFactory(
      IndexDirectoryHelper helper,
      IndexDefinitionGeneration index,
      LuceneConfig config,
      Optional<ByteReadCollector> byteReadCollector,
      boolean prewarm,
      Optional<CacheWarmerTimer> cacheWarmerTimer) {
    this(
        helper.getIndexDirectoryPath(index),
        helper.getIndexMetadataPath(index),
        config,
        index.getIndexDefinition().getNumPartitions(),
        byteReadCollector,
        prewarm,
        cacheWarmerTimer);
  }

  public IndexDirectoryFactory(
      IndexDirectoryHelper helper,
      IndexDefinitionGeneration index,
      LuceneConfig config,
      Optional<ByteReadCollector> byteReadCollector) {
    this(helper, index, config, byteReadCollector, false, Optional.empty());
  }

  @VisibleForTesting
  public void recordCacheWarmerWindow(long startNanos, long endNanos) {
    this.cacheWarmerTimer.ifPresent(timer -> timer.recordPrewarmWindow(startNanos, endNanos));
  }

  public Directory create(int indexPartitionId) throws IOException {
    var indexPartitionPath = getIndexPartitionDataPath(indexPartitionId);
    FileUtils.mkdirIfNotExist(indexPartitionPath);
    FileSystemDirectory fsd = new FileSystemDirectory(indexPartitionPath, this.byteReadCollector);
    if (this.prewarm) {
      long startNanos = System.nanoTime();
      fsd.prewarmVectorFiles();
      long stopNanos = System.nanoTime();
      this.recordCacheWarmerWindow(startNanos, stopNanos);
      long elapsedMilliseconds = TimeUnit.NANOSECONDS.toMillis(stopNanos - startNanos);
      LOG.atDebug()
          .addKeyValue("directory", indexPartitionPath)
          .addKeyValue("milliseconds", elapsedMilliseconds)
          .log("Cache Warmer: finished warming directory");
    }
    return this.config.nrtCacheEnabled()
        ? new NRTCachingDirectory(
            fsd, this.config.nrtMaxMergeSizeMb(), this.config.nrtTotalCacheSizeMb())
        : fsd;
  }

  public File createMetadata(int indexPartitionId) throws IOException {
    var indexPartitionPath = getIndexPartitionMetadataPath(indexPartitionId);
    FileUtils.mkdirIfNotExist(indexPartitionPath);
    return indexPartitionPath.toFile();
  }

  @VisibleForTesting
  public LuceneConfig getConfig() {
    return this.config;
  }
}
