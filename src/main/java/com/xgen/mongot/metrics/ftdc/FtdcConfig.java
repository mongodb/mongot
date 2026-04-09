package com.xgen.mongot.metrics.ftdc;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.Check;
import java.nio.file.Path;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FtdcConfig {
  private static final Logger LOG = LoggerFactory.getLogger(FtdcConfig.class);

  final int samplesPerInterimUpdate;
  final int samplesPerMetricChunk;
  final Path ftdcDirectory;
  final Bytes archiveFileSize;
  final Bytes directorySize;
  final int maxFileCount;
  private final int maxMeterCount;

  public FtdcConfig(
      Path ftdcDirectory,
      Bytes directorySize,
      Bytes archiveFileSize,
      int samplesPerInterimUpdate,
      int samplesPerMetricChunk,
      int maxFileCount,
      int maxMeterCount) {
    Check.argIsPositive(samplesPerInterimUpdate, "samplesPerInterimUpdate");
    Check.argIsPositive(samplesPerMetricChunk, "samplesPerMetricChunk");
    Check.argIsPositive(maxFileCount, "maxFileCount");
    Check.argIsPositive(maxMeterCount, "maxMeterCount");

    checkArg(
        samplesPerMetricChunk > samplesPerInterimUpdate,
        "interim updates should be more frequent than archive updates");
    checkArg(
        directorySize.compareTo(archiveFileSize) > 0,
        "directorySize should be larger than archiveFileSize");

    this.samplesPerInterimUpdate = samplesPerInterimUpdate;
    this.samplesPerMetricChunk = samplesPerMetricChunk;
    this.ftdcDirectory = ftdcDirectory;
    this.archiveFileSize = archiveFileSize;
    this.directorySize = directorySize;
    this.maxFileCount = maxFileCount;
    this.maxMeterCount = maxMeterCount;
  }

  /** Create a FtdcConfig with default values for all the missing parameters. */
  public static FtdcConfig create(
      Path directory,
      Optional<Bytes> optionalDirectorySize,
      Optional<Bytes> optionalArchiveFileSize,
      Optional<Integer> samplesPerInterimUpdate,
      Optional<Integer> samplesPerMetricChunk,
      Optional<Integer> maxFileCount,
      Optional<Integer> maxMeterCount) {
    FtdcConfig config =
        new FtdcConfig(
            directory,
            optionalDirectorySize.orElse(Bytes.ofMebi(100)),
            optionalArchiveFileSize.orElse(Bytes.ofMebi(10)),
            samplesPerInterimUpdate.orElse(10),
            samplesPerMetricChunk.orElse(50),
            maxFileCount.orElse(300),
            maxMeterCount.orElse(FtdcScheduledReporter.DEFAULT_MAX_METER_COUNT));
    LOG.atInfo()
        .addKeyValue("ftdcDirectory", config.ftdcDirectory)
        .addKeyValue("directorySize", config.directorySize)
        .addKeyValue("archiveFileSize", config.archiveFileSize)
        .addKeyValue("samplesPerInterimUpdate", config.samplesPerInterimUpdate)
        .addKeyValue("samplesPerMetricChunk", config.samplesPerMetricChunk)
        .addKeyValue("maxFileCount", config.maxFileCount)
        .addKeyValue("maxMeterCount", config.maxMeterCount)
        .log("instantiated FTDC config");
    return config;
  }

  public int maxMeterCount() {
    return this.maxMeterCount;
  }
}
