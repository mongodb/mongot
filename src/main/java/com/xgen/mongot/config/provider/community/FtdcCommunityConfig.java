package com.xgen.mongot.config.provider.community;

import com.xgen.mongot.config.provider.cli.CliOptionRegistry;
import com.xgen.mongot.util.DeploymentEnvironment;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;
import picocli.CommandLine;

public record FtdcCommunityConfig(
    Boolean enabled, Integer directorySizeMB, Integer fileSizeMB, Integer collectionPeriodMillis)
    implements DocumentEncodable {

  private static final Boolean DEFAULT_ENABLED = Boolean.TRUE;
  private static final Integer DEFAULT_DIRECTORY_SIZE_MB = 100;
  private static final Integer DEFAULT_FILE_SIZE_MB = 10;
  private static final Integer DEFAULT_COLLECTION_PERIOD_MILLIS = 1000;

  private static class Fields {

    public static final Field.WithDefault<Boolean> ENABLED =
        Field.builder("enabled")
            .booleanField()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .withDefault(DEFAULT_ENABLED);

    public static final Field.WithDefault<Integer> DIRECTORY_SIZE_MB =
        Field.builder("directorySizeMb")
            .intField()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .withDefault(DEFAULT_DIRECTORY_SIZE_MB);

    public static final Field.WithDefault<Integer> FILE_SIZE_MB =
        Field.builder("fileSizeMb")
            .intField()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .withDefault(DEFAULT_FILE_SIZE_MB);

    public static final Field.WithDefault<Integer> COLLECTION_PERIOD_MILLIS =
        Field.builder("collectionPeriodMillis")
            .intField()
            .edition(DeploymentEnvironment.COMMUNITY)
            .optional()
            .withDefault(DEFAULT_COLLECTION_PERIOD_MILLIS);
  }

  public static final class FtdcCommunityConfigCliOptionRegistry extends CliOptionRegistry {
    @CommandLine.Option(names = "--noFtdc", description = "Disables FTDC diagnostics")
    public Optional<Boolean> noFtdc = Optional.empty();

    @CommandLine.Option(
        names = "--ftdcDirectorySizeMb",
        description = "Maximum total size of the FTDC archive directory.")
    public Optional<Integer> directorySizeMb = Optional.empty();

    @CommandLine.Option(
        names = "--ftdcFileSizeMb",
        description = "Maximum size of an individual FTDC archive file")
    public Optional<Integer> fileSizeMb = Optional.empty();

    @CommandLine.Option(
        names = "--ftdcCollectionPeriodMs",
        description =
            "Sampling interval that controls how often mongot collects metrics into FTDC.")
    public Optional<Integer> collectionPeriodMillis = Optional.empty();

    public FtdcCommunityConfigCliOptionRegistry() {
      addBooleanLeafValue(Fields.ENABLED, () -> this.noFtdc.map(enabled -> !enabled));
      addIntegerLeafValue(Fields.DIRECTORY_SIZE_MB, () -> this.directorySizeMb);
      addIntegerLeafValue(Fields.FILE_SIZE_MB, () -> this.fileSizeMb);
      addIntegerLeafValue(Fields.COLLECTION_PERIOD_MILLIS, () -> this.collectionPeriodMillis);
    }
  }

  private FtdcCommunityConfig() {
    this(
        DEFAULT_ENABLED,
        DEFAULT_DIRECTORY_SIZE_MB,
        DEFAULT_FILE_SIZE_MB,
        DEFAULT_COLLECTION_PERIOD_MILLIS);
  }

  public static FtdcCommunityConfig getDefault() {
    return new FtdcCommunityConfig();
  }

  public static FtdcCommunityConfig fromBson(DocumentParser parser) throws BsonParseException {
    FtdcCommunityConfig config =
        new FtdcCommunityConfig(
            parser.getField(Fields.ENABLED).unwrap(),
            parser.getField(Fields.DIRECTORY_SIZE_MB).unwrap(),
            parser.getField(Fields.FILE_SIZE_MB).unwrap(),
            parser.getField(Fields.COLLECTION_PERIOD_MILLIS).unwrap());

    if (config.directorySizeMB() < 10) {
      parser
          .getContext()
          .handleSemanticError("directorySizeMb must be greater than or equal to 10mb");
    }

    if (config.fileSizeMB() < 1) {
      parser.getContext().handleSemanticError("fileSizeMb must be greater than or equal to 1mb");
    }

    if (config.fileSizeMB() >= config.directorySizeMB()) {
      parser.getContext().handleSemanticError("directorSizeMb must be greater than fileSizeMb");
    }

    if (config.collectionPeriodMillis() < 100) {
      parser
          .getContext()
          .handleSemanticError("collectionPeriodMillis must be greater than or equal to 100ms");
    }

    return config;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ENABLED, this.enabled)
        .field(Fields.DIRECTORY_SIZE_MB, this.directorySizeMB)
        .field(Fields.FILE_SIZE_MB, this.fileSizeMB)
        .field(Fields.COLLECTION_PERIOD_MILLIS, this.collectionPeriodMillis)
        .build();
  }
}
